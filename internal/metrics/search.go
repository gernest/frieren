package metrics

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"time"

	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/fields"
	"github.com/gernest/frieren/internal/predicate"
	"github.com/gernest/frieren/internal/query"
	"github.com/gernest/frieren/internal/store"
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
	store *store.Store
}

func NewQueryable(db *store.Store) *Queryable {
	return &Queryable{store: db}
}

var _ storage.Queryable = (*Queryable)(nil)

func (q *Queryable) Querier(mints, maxts int64) (storage.Querier, error) {
	tx, err := q.store.Index.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	txn := q.store.DB.NewTransaction(false)
	defer txn.Discard()
	view, err := query.New(txn, tx, constants.METRICS, time.UnixMilli(mints), time.UnixMilli(maxts))
	if err != nil {
		return nil, err
	}
	return &Querier{
		view:  view,
		store: q.store,
	}, nil

}

type Querier struct {
	view  *query.View
	store *store.Store
}

var _ storage.Querier = (*Querier)(nil)

func (q *Querier) Close() error {
	return nil
}

func (s *Querier) LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	if s.view.IsEmpty() {
		return []string{}, nil, nil
	}
	names := map[string]struct{}{}

	txn := s.store.DB.NewTransaction(false)
	defer txn.Discard()

	err := s.view.Traverse(func(info *v1.Shard, view string) error {
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	o := make([]string, 0, len(names))
	for k := range names {
		o = append(o, k)
	}
	sort.Strings(o)
	return o, nil, nil
}

func (s *Querier) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	if s.view.IsEmpty() {
		return []string{}, nil, nil
	}
	names := map[string]struct{}{}

	txn := s.store.DB.NewTransaction(false)
	defer txn.Discard()

	err := s.view.Traverse(func(info *v1.Shard, view string) error {
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	o := make([]string, 0, len(names))
	for k := range names {
		o = append(o, k)
	}
	sort.Strings(o)
	return o, nil, nil
}

func (s *Querier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if len(matchers) == 0 || s.view.IsEmpty() {
		return storage.EmptySeriesSet()
	}
	tx, err := s.store.Index.Begin(false)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	defer tx.Rollback()

	txn := s.store.DB.NewTransaction(false)
	defer txn.Discard()

	m := make(MapSet)

	matchSet := predicate.MatchersPlain(constants.MetricsLabels, matchers...)

	matchSet = append(matchSet, &predicate.Between{
		Field: constants.MetricsTimestamp,
		Start: uint64(hints.Start),
		End:   uint64(hints.End),
	})

	match := predicate.And(predicate.Optimize(matchSet, true))

	err = s.view.Traverse(func(shard *v1.Shard, view string) error {
		filterCtx := predicate.NewContext(
			shard.Id, view, s.store, tx, txn,
		)
		r, err := match.Apply(filterCtx)
		if err != nil {
			return err
		}
		if r.IsEmpty() {
			return nil
		}
		return m.Build(filterCtx, r)
	})
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	return NewSeriesSet(m)
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
	return &SeriesSet{
		series: s, pos: -1,
	}
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

func (s MapSet) Build(ctx *predicate.Context, filter *rows.Row) error {
	add := func(lf *fields.Fragment, seriesID, validID uint64, samples []chunks.Sample) error {
		sx, ok := s[seriesID]
		if ok {
			sx.Samples = append(sx.Samples, samples...)
			return nil
		}
		lbl, err := lf.Labels(ctx.Tx, ctx.Tr, validID)
		if err != nil {
			return fmt.Errorf("reading labels %w", err)
		}
		s[seriesID] = &S{
			Labels:  lbl,
			Samples: samples,
		}
		return nil
	}
	// fragments
	sf := fields.New(constants.MetricsSeries, ctx.Shard, ctx.View)
	hf := fields.New(constants.MetricsHistogram, ctx.Shard, ctx.View)
	vf := fields.New(constants.MetricsValue, ctx.Shard, ctx.View)
	tf := fields.New(constants.MetricsTimestamp, ctx.Shard, ctx.View)
	lf := fields.New(constants.MetricsLabels, ctx.Shard, ctx.View)

	// find all series
	series, err := sf.TransposeBSI(ctx.Tx, filter)
	if err != nil {
		return err
	}
	if series.IsEmpty() {
		return nil
	}

	// iterate on each series
	it := series.Iterator()

	// Filter to check if series is of histogram type
	histograms, err := hf.True(ctx.Tx)
	if err != nil {
		return fmt.Errorf("reading histogram %w", err)
	}
	histograms = histograms.Intersect(filter)
	hasHistogram := !histograms.IsEmpty()
	mapping := map[uint64]int{}
	for it.HasNext() {

		// Process series individually
		seriesID := it.Next()

		// Find all rows for each series matching the filter
		sr, err := sf.EqBSI(ctx.Tx, seriesID, filter)
		if err != nil {
			return fmt.Errorf("reading columns for series %w", err)
		}
		columns := sr.Columns()
		clear(mapping)
		for i := range columns {
			mapping[columns[i]] = i
		}

		chunks := make([]chunks.Sample, len(columns))

		if hasHistogram && histograms.Includes(columns[0]) {
			isFloat := false
			first := true
			hs := &prompb.Histogram{}
			err := vf.ExtractBSI(ctx.Tx, sr, mapping, func(i int, v uint64) error {
				hs.Reset()
				err = ctx.TrCall(constants.MetricsHistogram, v, hs.Unmarshal)
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
		} else {
			// This is a float series
			for i := range chunks {
				chunks[i] = &V{}
			}
			err := vf.ExtractBSI(ctx.Tx, sr, mapping, func(i int, v uint64) error {
				chunks[i].(*V).f = math.Float64frombits(v)
				return nil
			})
			if err != nil {
				return fmt.Errorf("extracting values %w", err)
			}
			err = tf.ExtractBSI(ctx.Tx, sr, mapping, func(i int, v uint64) error {
				chunks[i].(*V).ts = int64(v)
				return nil
			})
			if err != nil {
				return fmt.Errorf("extracting timestamp %w", err)
			}
		}
		err = add(lf, ctx.Shard, columns[0], chunks)
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
