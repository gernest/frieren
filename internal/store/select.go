package store

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/RoaringBitmap/roaring"
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/bsi"
	"github.com/gernest/rbf/dsl/cursor"
	rroaring "github.com/gernest/roaring"
	"github.com/gernest/rows"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
)

func (db *DB) Select(min, max int64, matchers ...*labels.Matcher) (MapSet, error) {
	a := roaring.New()
	err := db.Match(a, matchers...)
	if err != nil {
		return nil, err
	}
	if a.IsEmpty() {
		return nil, err
	}
	txn, err := db.idx.Begin(false)
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()

	tx, err := db.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blobs := tx.Bucket(blobBucket)
	if blobs == nil {
		return nil, err
	}

	var blobBuf [4]byte

	it := db.shards.Iterator()
	match := roaring.New()
	hs := &prompb.Histogram{}

	ms := make(MapSet)

	for it.HasNext() {
		shard := uint64(it.Next())

		txc, err := txn.Cursor(field("timestamp", shard))
		if err != nil {
			txc.Close()
			return nil, err
		}
		// check if we have valid time range
		r, err := bsi.Compare(txc, shard, bsi.RANGE, min, max, nil)
		if err != nil {
			txc.Close()
			return nil, err
		}
		if r.IsEmpty() {
			txc.Close()
			continue
		}

		// we have matches for this shard find  all series within this time range.
		sxc, err := txn.Cursor(field("series", shard))
		if err != nil {
			txc.Close()
			return nil, err
		}
		match.Clear()
		err = Distinct(sxc, match, r)
		if err != nil {
			sxc.Close()
			txc.Close()
			return nil, err
		}
		match.And(a)

		if match.IsEmpty() {
			sxc.Close()
			txc.Close()
			continue
		}

		// now match contains all the series we want. We can decode the
		// timestamp/values

		// We treat histograms and float values differently
		exists, err := exists(txn, shard, r)
		if err != nil {
			sxc.Close()
			txc.Close()
			return nil, err
		}

		hxc, err := txn.Cursor(field("histogram", shard))
		if err != nil {
			sxc.Close()
			txc.Close()
			return nil, err
		}

		sit := match.Iterator()

		vxc, err := txn.Cursor(field("values", shard))
		if err != nil {
			sxc.Close()
			txc.Close()
			hxc.Close()
			return nil, err
		}
		lxc, err := txn.Cursor(field("labels", shard))
		if err != nil {
			sxc.Close()
			txc.Close()
			hxc.Close()
			vxc.Close()
			return nil, err
		}
		for sit.HasNext() {
			series := uint64(sit.Next())

			f, err := cursor.Row(sxc, shard, series)
			if err != nil {
				txc.Close()
				sxc.Close()
				hxc.Close()
				vxc.Close()
				lxc.Close()
				return nil, err
			}

			// f matches through the whole shard, we need to reduce to what is in the
			// current time range.
			f = f.Intersect(r)

			columns := f.Columns()

			sx, ok := ms[series]
			if !ok {
				// read labels: only one column is enough
				sx = &S{}
				err = cursor.Rows(lxc, shard,
					func(row uint64) error {
						name, value, _ := bytes.Cut(get(blobs, &blobBuf, uint32(row)), sep)
						sx.Labels = append(sx.Labels, labels.Label{
							Name:  string(name),
							Value: string(value),
						})
						return nil
					},
					rroaring.NewBitmapColumnFilter(columns[0]),
				)
				if err != nil {
					txc.Close()
					sxc.Close()
					hxc.Close()
					vxc.Close()
					lxc.Close()
					return nil, err
				}
				ms[series] = sx
			}
			samples := make([]chunks.Sample, len(columns))
			// Is this a histogram ?
			if exists.Includes(columns[0]) {
				// Histogram series
				for i := range columns {
					err := cursor.Rows(hxc, shard, func(row uint64) error {
						hs.Reset()
						err = hs.Unmarshal(get(blobs, &blobBuf, uint32(row)))
						if err != nil {
							return err
						}
						// we only generate int histogram
						samples[i] = NewH(hs)
						return nil
					},
						rroaring.NewBitmapColumnFilter(columns[i]),
					)
					if err != nil {
						txc.Close()
						sxc.Close()
						hxc.Close()
						vxc.Close()
						lxc.Close()
						return nil, err
					}
				}
			} else {
				// float series
				data := NewData(columns)

				err = BSI(data, columns, vxc, f, shard, func(position int, value int64) error {
					samples[position] = &V{
						f: math.Float64frombits(uint64(value)),
					}
					return nil
				})
				if err != nil {
					txc.Close()
					sxc.Close()
					hxc.Close()
					vxc.Close()
					lxc.Close()
					return nil, err
				}
				err = BSI(data, columns, txc, f, shard, func(position int, value int64) error {
					samples[position].(*V).ts = value
					return nil
				})
				if err != nil {
					txc.Close()
					sxc.Close()
					hxc.Close()
					vxc.Close()
					lxc.Close()
					return nil, err
				}
			}
			sx.Samples = append(sx.Samples, samples...)
		}
		txc.Close()
		sxc.Close()
		hxc.Close()
		vxc.Close()
		lxc.Close()
	}
	return ms, nil
}

func field(name string, shard uint64) string {
	return fmt.Sprintf("%s:%d", name, shard)
}

func exists(tx *rbf.Tx, shard uint64, f *rows.Row) (*rows.Row, error) {
	c, err := tx.Cursor(field("kind", shard))
	if err != nil {
		if errors.Is(err, rbf.ErrBitmapNotFound) {
			// It is possible for samples to not have histogram series.
			return rows.NewRow(), nil
		}
		return nil, err
	}
	defer c.Close()
	r, err := cursor.Row(c, shard, 1)
	if err != nil {
		return nil, err
	}
	return r.Intersect(f), nil
}

type SeriesSet struct {
	series []storage.Series
	pos    int
}

func NewSeriesSet(m MapSet) *SeriesSet {
	keys := make([]uint64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	s := make([]storage.Series, 0, len(m))
	for i := range keys {
		s = append(s, storage.NewListSeries(m[keys[i]].Labels, m[keys[i]].Samples))
	}
	return &SeriesSet{series: s}
}

var _ storage.SeriesSet = (*SeriesSet)(nil)

func (s *SeriesSet) Next() bool {
	return s.pos < len(s.series)
}

func (s *SeriesSet) At() storage.Series {
	defer func() { s.pos++ }()
	return s.series[s.pos]
}

func (s *SeriesSet) Err() error {
	return nil
}

func (s *SeriesSet) Warnings() annotations.Annotations {
	return nil
}

type MapSet map[uint64]*S

type S struct {
	Labels  labels.Labels
	Samples []chunks.Sample
}

type V struct {
	ts int64
	f  float64
}

var _ chunks.Sample = (*V)(nil)

func (h *V) T() int64                      { return h.ts }
func (h *V) F() float64                    { return h.f }
func (h *V) H() *histogram.Histogram       { return nil }
func (h *V) FH() *histogram.FloatHistogram { return nil }
func (h *V) Type() chunkenc.ValueType      { return chunkenc.ValFloat }

type H struct {
	ts int64
	h  *histogram.Histogram
}

var _ chunks.Sample = (*H)(nil)

func (h *H) T() int64                      { return h.ts }
func (h *H) F() float64                    { return 0 }
func (h *H) H() *histogram.Histogram       { return h.h }
func (h *H) FH() *histogram.FloatHistogram { return nil }
func (h *H) Type() chunkenc.ValueType      { return chunkenc.ValHistogram }

func NewH(o *prompb.Histogram) *H {
	return &H{
		ts: o.Timestamp,
		h:  o.ToIntHistogram(),
	}
}

type FH struct {
	ts int64
	h  *histogram.FloatHistogram
}

var _ chunks.Sample = (*FH)(nil)

func (h *FH) T() int64                      { return h.ts }
func (h *FH) F() float64                    { return 0 }
func (h *FH) H() *histogram.Histogram       { return nil }
func (h *FH) FH() *histogram.FloatHistogram { return h.h }
func (h *FH) Type() chunkenc.ValueType      { return chunkenc.ValHistogram }

func NewFH(o *prompb.Histogram) *FH {
	return &FH{
		ts: o.Timestamp,
		h:  o.ToFloatHistogram(),
	}
}
