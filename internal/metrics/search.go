package metrics

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/blevesearch/vellum"
	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/fields"
	"github.com/gernest/frieren/internal/predicate"
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl"
	"github.com/gernest/rbf/dsl/bsi"
	"github.com/gernest/rbf/dsl/cursor"
	"github.com/gernest/rbf/dsl/mutex"
	rq "github.com/gernest/rbf/dsl/query"
	"github.com/gernest/rbf/dsl/tx"
	"github.com/gernest/roaring"
	"github.com/gernest/rows"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
)

type Queryable struct {
	store *Store
}

var _ storage.Queryable = (*Queryable)(nil)

func (q *Queryable) Querier(mints, maxts int64) (storage.Querier, error) {
	r, err := q.store.Reader()
	if err != nil {
		return nil, err
	}
	defer r.Release()

	return &Querier{
		store:  q.store,
		shards: r.Range(time.UnixMilli(mints).UTC(), time.UnixMilli(maxts).UTC()),
	}, nil

}

type Querier struct {
	store  *Store
	shards []dsl.Shard
}

var _ storage.Querier = (*Querier)(nil)

func (q *Querier) Close() error {
	return nil
}

func (s *Querier) LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	r, err := s.store.Reader()
	if err != nil {
		return nil, nil, err
	}
	defer r.Release()

	m := map[string]struct{}{}
	prefix := []byte(name + "=")
	err = r.Tr().Search("labels", &vellum.AlwaysMatch{}, prefix, nil, func(key []byte, value uint64) error {
		if !bytes.HasPrefix(key, prefix) {
			return io.EOF
		}
		m[string(bytes.TrimPrefix(key, prefix))] = struct{}{}
		return nil
	})
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, nil, err
		}
	}
	o := make([]string, 0, len(m))
	for k := range m {
		o = append(o, k)
	}
	slices.Sort(o)
	return o, nil, nil
}

func (s *Querier) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	r, err := s.store.Reader()
	if err != nil {
		return nil, nil, err
	}
	defer r.Release()

	m := map[string]struct{}{}
	split := []byte("=")
	err = r.Tr().Search("labels", &vellum.AlwaysMatch{}, nil, nil, func(key []byte, value uint64) error {
		name, _, _ := bytes.Cut(key, split)
		m[string(name)] = struct{}{}
		return nil
	})
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, nil, err
		}
	}
	o := make([]string, 0, len(m))
	for k := range m {
		o = append(o, k)
	}
	slices.Sort(o)
	return o, nil, nil
}

func (s *Querier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if len(matchers) == 0 || len(s.shards) == 0 {
		return storage.EmptySeriesSet()
	}

	m := make(MapSet)

	// We use timestamp filter as the base filter
	base := bsi.Filter("timestamp", bsi.RANGE, hints.Start, hints.End)

	// all matchers use AND
	filter := make(rq.And, 0, len(matchers))
	var b bytes.Buffer
	for _, m := range matchers {
		var op mutex.OP
		b.Reset()
		b.WriteString(m.Name)
		b.WriteByte('=')
		switch m.Type {
		case labels.MatchEqual:
			op = mutex.EQ
			b.WriteString(m.Value)
		case labels.MatchNotEqual:
			op = mutex.NEQ
			b.WriteString(m.Value)
		case labels.MatchRegexp:
			op = mutex.RE
			b.WriteString(clean(m.Value))
		case labels.MatchNotRegexp:
			op = mutex.NRE
			b.WriteString(clean(m.Value))
		}
		filter = append(filter, &mutex.MatchString{
			Field: "labels",
			Op:    op,
			Value: b.String(),
		})
	}
	r, err := s.store.Reader()
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	defer r.Release()
	bitmap := roaring64.New()
	isSeriesQuery := hints.Func == "series"
	for i := range s.shards {
		r.View(s.shards[i], func(txn *tx.Tx) error {
			r, err := base.Apply(txn, nil)
			if err != nil {
				return err
			}
			if r.IsEmpty() {
				return nil
			}
			f, err := filter.Apply(txn, r)
			if err != nil {
				return err
			}
			if f.IsEmpty() {
				return nil
			}

			// Get all series
			bitmap.Clear()
			err = mutex.Distinct(txn, "series", bitmap, f)
			if err != nil {
				return err
			}
			it := bitmap.Iterator()

			// a single cursor for series so we can reuse it for all ops on this shard.
			seriesCursor, err := txn.Tx.Cursor(txn.Key("series"))
			if err != nil {
				return err
			}
			defer seriesCursor.Close()
			labels, err := txn.Tx.Cursor(txn.Key("labels"))
			if err != nil {
				return err
			}
			defer labels.Close()

			if isSeriesQuery {
				// Fast path for series queries. We only need to read series labels no need
				// to process samples.
				for it.HasNext() {
					series := it.Next()
					if _, exists := m[series]; exists {
						continue
					}

					// Find all rows for this series + filter
					row, err := cursor.Row(seriesCursor, txn.Shard, series)
					if err != nil {
						return err
					}
					row = row.Intersect(f)
					err = row.RangeColumns(func(u uint64) error {
						lbl, err := readLabels(labels, txn, u)
						if err != nil {
							return err
						}
						m[series] = &S{Labels: lbl}
						// We are only reading a single column
						return io.EOF
					})
					if err != nil {
						if !errors.Is(err, io.EOF) {
							return err
						}
					}
				}
				return nil
			}

			// maps  column to sample
			kind := map[uint64]v1.Sample_Kind{}
			err = txn.Cursor("kind", func(c *rbf.Cursor, tx *tx.Tx) error {
				return mutex.Extract(c, tx.Shard, f, func(column, value uint64) error {
					kind[column] = v1.Sample_Kind(value)
					return nil
				})
			})
			if err != nil {
				return err
			}
			ts := map[uint64]int64{}
			err = txn.Cursor("timestamp", func(c *rbf.Cursor, tx *tx.Tx) error {
				return bsi.Extract(c, tx.Shard, f, func(column uint64, value int64) error {
					ts[column] = value
					return nil
				})
			})
			if err != nil {
				return err
			}
			value, err := txn.Tx.Cursor(txn.Key("value"))
			if err != nil {
				return err
			}
			defer value.Close()

			hist, err := txn.Tx.Cursor(txn.Key("histogram"))
			if err != nil {
				return err
			}
			defer hist.Close()

			for it.HasNext() {
				series := it.Next()

				// Find all rows for this series + filter
				row, err := cursor.Row(seriesCursor, txn.Shard, series)
				if err != nil {
					return err
				}
				row = row.Intersect(f)

				columns := row.Columns()
				mapping := make(map[uint64]int, len(columns))
				for i := range columns {
					mapping[columns[i]] = i
				}
				chunks := make([]chunks.Sample, len(columns))

				switch kind[columns[0]] {
				case v1.Sample_Float:
					bsi.Extract(value, txn.Shard, row, func(column uint64, value int64) error {
						chunks[mapping[column]] = &V{
							f:  math.Float64frombits(uint64(value)),
							ts: ts[column],
						}
						return nil
					})
				case v1.Sample_Histogram:
					hs := &prompb.Histogram{}
					err = mutex.Extract(hist, txn.Shard, row, func(column, value uint64) error {
						hs.Reset()
						err := hs.Unmarshal(txn.Tr.Blob("histogram", value))
						if err != nil {
							return err
						}
						if _, isFloat := hs.Count.(*prompb.Histogram_CountFloat); isFloat {
							chunks[i] = NewFH(hs)
						} else {
							chunks[i] = NewH(hs)
						}
						return nil
					})
					if err != nil {
						return err
					}
				}

				sx, seen := m[series]
				if !seen {
					// read labels
					lbl, err := readLabels(labels, txn, columns[0])
					if err != nil {
						return err
					}
					m[series] = &S{
						Labels:  lbl,
						Samples: chunks,
					}
					continue
				}
				sx.Samples = append(sx.Samples, chunks...)
			}
			return nil
		})
	}

	return NewSeriesSet(m)
}

var eq = []byte("=")

func readLabels(c *rbf.Cursor, txn *tx.Tx, column uint64) (o labels.Labels, err error) {
	err = cursor.Rows(c, 0, func(row uint64) error {
		name, value, _ := bytes.Cut(txn.Tr.Key("labels", row), eq)
		o = append(o, labels.Label{
			Name:  string(name),
			Value: string(value),
		})
		return nil
	}, roaring.NewBitmapColumnFilter(column))
	return
}
func clean(s string) string {
	s = strings.TrimPrefix(s, "^")
	s = strings.TrimSuffix(s, "$")
	return s
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

// Series is like build but avoids reading samples. It is optimized for /series endpoint.
func (s MapSet) Series(ctx *predicate.Context, filter *rows.Row) error {
	// fragments
	sf := fields.From(ctx, constants.MetricsSeries)
	lf := fields.From(ctx, constants.MetricsLabels)

	// find all series
	series, err := sf.TransposeBSI(ctx.Index(), filter)
	if err != nil {
		return err
	}
	if series.IsEmpty() {
		return nil
	}

	// iterate on each series
	it := series.Iterator()

	for it.HasNext() {
		// This id is unique only in current view. We create a xxhash of the checksum
		// to have a global unique series.
		seriesHashID := it.Next()

		// Globally unique ID for the current series.
		seriesID := binary.BigEndian.Uint64(ctx.Tr(constants.MetricsSeries, seriesHashID))
		if _, seenSeries := s[seriesID]; seenSeries {
			continue
		}
		// Find all rows for each series matching the filter
		sr, err := sf.EqBSI(ctx.Index(), seriesHashID, filter)
		if err != nil {
			return fmt.Errorf("reading columns for series %w", err)
		}
		err = sr.RangeColumns(func(u uint64) error {
			labels, err := lf.Labels(ctx, u)
			if err != nil {
				return err
			}
			s[seriesID] = &S{Labels: labels}
			// Make sure we only iterate once
			return io.EOF
		})
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return fmt.Errorf("reading series labels %w", err)
			}
		}
	}
	return nil
}

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
		h:  remote.HistogramProtoToHistogram(*o),
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
		h:  remote.FloatHistogramProtoToFloatHistogram(*o),
	}
}
