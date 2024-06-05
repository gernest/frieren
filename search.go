package ernestdb

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/blevesearch/vellum"
	re "github.com/blevesearch/vellum/regexp"
	"github.com/cespare/xxhash/v2"
	"github.com/gernest/ernestdb/keys"
	"github.com/gernest/ernestdb/shardwidth"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

func Queryable(db Store, mints, maxts int64) {

	// adjust to epoch
	mints = max(mints, epochMs)
	if maxts < mints {
		maxts = mints
	}
	if maxts == epochMs {
		return
	}
	minShard := (mints - epochMs) / shardwidth.ShardWidth
	maxShard := (maxts - epochMs) / shardwidth.ShardWidth

	if minShard == maxShard {

	}
}

type singleQuerier struct {
	storage.LabelQuerier
	shard uint64
	db    Store
}

var _ storage.Querier = (*singleQuerier)(nil)

func (s *singleQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return nil
}

type MapSet map[uint64]*S

var (
	sep = []byte("=")
)

func (s MapSet) Build(db Store, start, end int64, shard uint64, series *roaring64.Bitmap) error {
	tsSlice := (&keys.Timestamp{ShardID: shard}).Slice()
	valueSlice := (&keys.Value{ShardID: shard}).Slice()
	hsSlice := (&keys.Histogram{ShardID: shard}).Slice()
	seriesSlice := (&keys.Series{ShardID: shard}).Slice()
	blobSlice := (&keys.Blob{}).Slice()

	histogram, err := readBitmap(db, (&keys.Kind{}).Key())
	if err != nil {
		return err
	}
	kb := make([]byte, 0, 1<<10)
	it := series.Iterator()

	add := func(seriesID uint64, samples []chunks.Sample) error {
		sx, ok := s[seriesID]
		if ok {
			sx.Samples = append(sx.Samples, samples...)
			return nil
		}
		seriesSlice[len(seriesSlice)-1] = seriesID
		b, err := readBitmap(db, keys.Encode(kb, seriesSlice))
		if err != nil {
			return fmt.Errorf("reading series bitmap %w", err)
		}
		sx = &S{
			Labels:  make(labels.Labels, 0, b.GetCardinality()),
			Samples: samples,
		}
		itr := b.Iterator()
		for itr.HasNext() {
			blobSlice[len(blobSlice)-1] = it.Next()
			err = db.Get(keys.Encode(kb, blobSlice), func(val []byte) error {
				key, value, _ := bytes.Cut(val, sep)
				sx.Labels = append(sx.Labels, labels.Label{
					Name:  string(key),
					Value: string(value),
				})
				return nil
			})
			if err != nil {
				return fmt.Errorf("reading series labels %w", err)
			}
		}
		s[seriesID] = sx
		return nil
	}
	for it.HasNext() {
		seriesID := it.Next()

		tsSlice[len(tsSlice)-1] = seriesID
		ts, err := readBSI(db, keys.Encode(kb, tsSlice))
		if err != nil {
			return err
		}

		// find ids within range
		ids := ts.CompareValue(0, roaring64.RANGE, start, end, nil)
		if ids.IsEmpty() {
			continue
		}
		if histogram.Contains(seriesID) {
			// read this series as histogram
			hsSlice[len(hsSlice)-1] = seriesID
			hs, err := readBSI(db, keys.Encode(kb, hsSlice))
			if err != nil {
				return err
			}

			itr := ids.Iterator()
			var h prompb.Histogram
			decode := func(n uint64) error {
				value, found := hs.GetValue(n)
				if !found {
					return fmt.Errorf("missing histogram value for series=%d id=%d ", seriesID, n)
				}
				blobSlice[len(blobSlice)-1] = uint64(value)
				err = db.Get(keys.Encode(kb, blobSlice), h.Unmarshal)
				if err != nil {
					return fmt.Errorf("reading histogram %w", err)
				}
				return nil
			}
			// we need to detect kind of histogram before proceeding
			err = decode(itr.Next())
			if err != nil {
				return err
			}
			samples := make([]chunks.Sample, 0, ids.GetCardinality())
			switch h.Count.(type) {
			case *prompb.Histogram_CountFloat:
				samples = append(samples, NewFH(&h))
				for itr.HasNext() {
					err = decode(itr.Next())
					if err != nil {
						return err
					}
					samples = append(samples, NewFH(&h))
				}
			default:
				samples = append(samples, NewH(&h))
				for itr.HasNext() {
					err = decode(itr.Next())
					if err != nil {
						return err
					}
					samples = append(samples, NewH(&h))
				}
			}
			err = add(seriesID, samples)
			if err != nil {
				return err
			}
			continue
		}
		valueSlice[len(valueSlice)-1] = seriesID
		values, err := readBSI(db, keys.Encode(kb, valueSlice))
		if err != nil {
			return fmt.Errorf("reading series value %w", err)
		}
		samples := make([]chunks.Sample, 0, ids.GetCardinality())
		timestamps := ts.IntersectAndTranspose(0, ids)
		tsItr := timestamps.Iterator()
		itr := ids.Iterator()
		a, b := ids.GetCardinality(), timestamps.GetCardinality()
		if a != b {
			// Make sure
			exit("mismatch ids and timestamp values", "ids", a, "ts", b)
		}
		for itr.HasNext() {
			column := itr.Next()
			value, found := values.GetValue(column)
			if !found {
				return fmt.Errorf("missing value for column=%d", column)
			}
			samples = append(samples, &V{
				ts: int64(tsItr.Next()),
				f:  math.Float64frombits(uint64(value)),
			})
		}
		err = add(seriesID, samples)
		if err != nil {
			return err
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

// Series returns a bitmap of all series ID that match matchers for the given shard.
func Series(db Store, shard uint64, matchers ...*labels.Matcher) (*roaring64.Bitmap, error) {
	if len(matchers) == 0 {
		return readBitmap(db, (&keys.Exists{ShardID: shard}).Key())
	}

	var (
		hasRe bool
	)

	allLabels, err := readBitmap(db, (&keys.FSTBitmap{ShardID: shard}).Key())
	if err != nil {
		return nil, err
	}
	lbl := roaring64.New()
	var buf bytes.Buffer
	var h xxhash.Digest

	for _, m := range matchers {
		switch m.Type {
		case labels.MatchEqual, labels.MatchNotEqual:
			buf.Reset()
			buf.WriteString(m.Name)
			buf.WriteByte('=')
			buf.WriteString(m.Value)
			h.Reset()
			h.Write(buf.Bytes())
			label := h.Sum64()
			if !allLabels.Contains(label) {
				// Return early, this matcher will never satisfy.
				return roaring64.New(), nil
			}
			if m.Type == labels.MatchEqual {
				lbl.Add(label)
			} else {
				all := allLabels.Clone()
				all.Remove(label)
				lbl.Or(all)
			}
		default:
			hasRe = true
		}
	}
	if hasRe {
		err := readFST(db, shard, func(fst *vellum.FST) error {
			o := roaring64.New()
			for _, m := range matchers {
				switch m.Type {
				case labels.MatchRegexp, labels.MatchNotRegexp:
					rx, err := compile(&buf, m.Name, m.Value)
					if err != nil {
						return fmt.Errorf("compiling matcher %q %w", m.String(), err)
					}
					o.Clear()
					itr, err := fst.Search(rx, nil, nil)
					for err == nil {
						_, value := itr.Current()
						o.Add(value)
						err = itr.Next()
					}
					if m.Type == labels.MatchRegexp {
						lbl.Or(o)
					} else {
						all := allLabels.Clone()
						all.AndNot(o)
						lbl.Or(all)
					}
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	// lbl contains all labels satisfying the matchers conditions. The series we
	// want is the intersection of all labels
	series := roaring64.New()
	slice := (&keys.Labels{}).Slice()
	xb := make([]byte, 0, len(slice)*8)
	it := lbl.Iterator()
	start := true
	for it.HasNext() {
		slice[len(slice)-1] = it.Next()
		b, err := readBitmap(db, keys.Encode(xb, slice))
		if err != nil {
			return nil, err
		}
		if start {
			series.Or(b)
			start = false
		} else {
			series.And(b)
		}
		if series.IsEmpty() {
			break
		}
	}
	return series, nil
}

func compile(b *bytes.Buffer, key, value string) (*re.Regexp, error) {
	value = strings.TrimPrefix(value, "^")
	value = strings.TrimSuffix(value, "$")
	b.Reset()
	b.WriteString(key)
	b.WriteByte('=')
	b.WriteString(value)
	return re.New(b.String())
}

func readFST(db Store, shard uint64, f func(fst *vellum.FST) error) error {
	return db.Get((&keys.FST{ShardID: shard}).Key(), func(val []byte) error {
		fst, err := vellum.Load(val)
		if err != nil {
			return err
		}
		return f(fst)
	})
}

func readBSI(db Store, key []byte) (*roaring64.BSI, error) {
	o := roaring64.NewDefaultBSI()
	err := db.Get(key, func(val []byte) error {
		_, err := o.ReadFrom(bytes.NewReader(val))
		return err
	})
	if err != nil {
		return nil, err
	}
	return o, nil
}

func readBitmap(db Store, key []byte) (*roaring64.Bitmap, error) {
	o := roaring64.New()
	err := db.Get(key, o.UnmarshalBinary)
	if err != nil {
		return nil, err
	}
	return o, nil
}
