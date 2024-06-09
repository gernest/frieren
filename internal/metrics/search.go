package metrics

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/fields"
	"github.com/gernest/frieren/internal/fst"
	"github.com/gernest/frieren/internal/tags"
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/quantum"
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
	db  *badger.DB
	idx *rbf.DB
}

var _ storage.Queryable = (*Queryable)(nil)

func (q *Queryable) Querier(mints, maxts int64) (storage.Querier, error) {
	var views []string
	if date(mints).Equal(date(maxts)) {
		// Same day generate a single view
		views = []string{quantum.ViewByTimeUnit("", time.UnixMilli(mints), 'D')}
	} else {
		// We want view that might contain maxts to be included too, we need to add
		// extra date
		views = quantum.ViewsByTimeRange("",
			time.UnixMilli(mints), time.UnixMilli(maxts).AddDate(0, 0, 1),
			quantum.TimeQuantum("D"))
	}
	tx, err := q.idx.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	ids := make([]string, 0, len(views))
	shards := make([][]uint64, 0, len(views))
	for i := range views {
		// read the shards observed per view
		view := fields.Fragment{ID: fields.MetricsShards, View: views[i]}
		r, err := tx.RoaringBitmap(view.String())
		if err != nil {
			return nil, fmt.Errorf("reading shards bitmap %w", err)
		}
		if r.Count() == 0 {
			continue
		}
		ids = append(ids, views[i])
		shards = append(shards, r.Slice())
	}
	return &Querier{
		shards: shards,
		views:  ids,
		db:     q.db,
		idx:    q.idx,
	}, nil

}

func date(ts int64) time.Time {
	y, m, d := time.UnixMilli(ts).Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

type Querier struct {
	storage.LabelQuerier
	shards [][]uint64
	views  []string
	db     *badger.DB
	idx    *rbf.DB
}

var _ storage.Querier = (*Querier)(nil)

func (s *Querier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if len(matchers) == 0 || len(s.views) == 0 {
		return storage.EmptySeriesSet()
	}
	tx, err := s.idx.Begin(false)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	defer tx.Rollback()

	txn := s.db.NewTransaction(false)
	defer txn.Discard()

	yes, no := roaring64.New(), roaring64.New()
	m := make(MapSet)

	for i := range s.views {
		view := s.views[i]
		for _, shard := range s.shards[i] {
			fra := fields.Fragment{ID: fields.MetricsFST, Shard: shard, View: view}
			yes.Clear()
			no.Clear()
			err := fst.Match(txn, []byte(fra.String()), yes, no, matchers...)
			if err != nil {
				return storage.ErrSeriesSet(err)
			}
			if yes.IsEmpty() && no.IsEmpty() {
				continue
			}
			r, err := tags.Filter(tx, fields.Fragment{ID: fields.MetricsLabels, Shard: shard, View: view}, yes, no)
			if err != nil {
				return storage.ErrSeriesSet(err)
			}
			err = m.Build(txn, tx, blob.Translate(txn), hints.Start, hints.End, view, shard, r)
			if err != nil {
				return storage.ErrSeriesSet(err)
			}
		}

	}
	return nil
}

type SeriesSet struct {
	series []storage.Series
	pos    int
}

var _ storage.SeriesSet = (*SeriesSet)(nil)

func (s *SeriesSet) Next() bool {
	s.pos++
	return s.pos < len(s.series)
}

func (s *SeriesSet) At() storage.Series {
	return s.series[s.pos]
}

func (s *SeriesSet) Err() error {
	return nil
}

func (s *SeriesSet) Warnings() annotations.Annotations {
	return nil
}

type MapSet map[uint64]*S

var (
	sep = []byte("=")
)

func (s MapSet) Build(txn *badger.Txn, tx *rbf.Tx, tr blob.Tr, start, end int64, view string, shard uint64, filter *rows.Row) error {
	add := func(lf *fields.Fragment, seriesID, validID uint64, samples []chunks.Sample) error {
		sx, ok := s[seriesID]
		if ok {
			sx.Samples = append(sx.Samples, samples...)
			return nil
		}
		lbl, err := lf.ReadSetValue(tx, validID)
		if err != nil {
			return fmt.Errorf("reading labels %w", err)
		}
		sx = &S{
			Labels:  make(labels.Labels, 0, len(lbl)),
			Samples: samples,
		}
		for i := range lbl {
			err = tr(lbl[i], func(val []byte) error {
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
	// fragments
	sf := fields.Fragment{ID: fields.MetricsSeries, Shard: shard, View: view}
	hf := fields.Fragment{ID: fields.MetricsHistogram, Shard: shard, View: view}
	vf := fields.Fragment{ID: fields.MetricsValue, Shard: shard, View: view}
	tf := fields.Fragment{ID: fields.MetricsTimestamp, Shard: shard, View: view}
	lf := fields.Fragment{ID: fields.MetricsLabels, Shard: shard, View: view}

	// find matching timestamps
	r, err := tf.Between(tx, uint64(start), uint64(end))
	if err != nil {
		return fmt.Errorf("reading timestamp %w", err)
	}
	if filter != nil {
		r = r.Intersect(filter)
	}
	if r.IsEmpty() {
		return nil
	}

	// find all series
	series, err := sf.TransposeBSI(tx, r)
	if err != nil {
		return err
	}
	if series.IsEmpty() {
		return nil
	}

	// iterate on each series
	it := series.Iterator()

	// Filter to check if series is of histogram type
	hsSet, err := hf.True(tx)
	if err != nil {
		return fmt.Errorf("reading histogram %w", err)
	}
	mapping := map[uint64]int{}
	for it.HasNext() {
		seriesID := it.Next()
		sr, err := sf.EqBSI(tx, seriesID)
		if err != nil {
			return fmt.Errorf("reading columns for series %w", err)
		}
		sr = sr.Intersect(r)
		if sr.IsEmpty() {
			continue
		}
		columns := sr.Columns()
		clear(mapping)
		for i := range columns {
			mapping[columns[i]] = i
		}

		chunks := make([]chunks.Sample, len(columns))

		if !hsSet.Includes(columns[0]) {
			// This is a float series
			for i := range chunks {
				chunks[i] = &V{}
			}
			err := vf.ExtractBSI(tx, sr, mapping, func(i int, v uint64) error {
				chunks[i].(*V).f = math.Float64frombits(v)
				return nil
			})
			if err != nil {
				return fmt.Errorf("extracting values %w", err)
			}
			err = tf.ExtractBSI(tx, sr, mapping, func(i int, v uint64) error {
				chunks[i].(*V).ts = int64(v)
				return nil
			})
			if err != nil {
				return fmt.Errorf("extracting timestamp %w", err)
			}
		} else {
			isFloat := false
			first := true
			hs := &prompb.Histogram{}
			err := vf.ExtractBSI(tx, sr, mapping, func(i int, v uint64) error {
				hs.Reset()
				err := tr(v, hs.Unmarshal)
				if err != nil {
					return fmt.Errorf("reading histogram blob %w", err)
				}
				if first {
					_, isFloat = hs.Count.(*prompb.Histogram_CountFloat)
					first = false
				}
				if isFloat {
					chunks[i] = NewFH(hs)
					return nil
				}
				chunks[i] = NewH(hs)
				return nil
			})
			if err != nil {
				return fmt.Errorf("extracting values %w", err)
			}
		}
		err = add(&lf, shard, columns[0], chunks)
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
