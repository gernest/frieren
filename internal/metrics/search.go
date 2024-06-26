package metrics

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"time"

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
	return &Querier{
		store: q.store,
		start: time.UnixMilli(mints),
		end:   time.UnixMilli(maxts),
	}, nil

}

type Querier struct {
	store      *store.Store
	start, end time.Time
}

var _ storage.Querier = (*Querier)(nil)

func (q *Querier) Close() error {
	return nil
}

func (s *Querier) LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	o, err := query.Labels(s.store, constants.METRICS, constants.MetricsLabels, s.start, s.end, name, matchers...)
	if err != nil {
		return nil, nil, err
	}
	return o, nil, nil
}

func (s *Querier) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	o, err := query.Labels(s.store, constants.METRICS, constants.MetricsLabels, s.start, s.end, "", matchers...)
	if err != nil {
		return nil, nil, err
	}
	return o, nil, nil
}

func (s *Querier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if len(matchers) == 0 {
		return storage.EmptySeriesSet()
	}

	m := make(MapSet)

	matchSet := predicate.MatchersPlain(constants.MetricsLabels, matchers...)

	matchSet = append(matchSet, &predicate.Between{
		Field: constants.MetricsTimestamp,
		Start: uint64(hints.Start),
		End:   uint64(hints.End),
	})

	match := predicate.And(predicate.Optimize(matchSet, true))
	isSeriesQuery := hints.Func == "series"
	err := query.Query(s.store, constants.METRICS, s.start, s.end, func(view *store.View) error {
		r, err := match.Apply(view)
		if err != nil {
			return err
		}
		if r.IsEmpty() {
			return nil
		}
		if isSeriesQuery {
			return m.Series(view, r)
		}
		return m.Build(view, r)
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

func (s MapSet) Build(ctx *predicate.Context, filter *rows.Row) error {
	add := func(lf *fields.Fragment, seriesID, validID uint64, samples []chunks.Sample) error {
		sx, ok := s[seriesID]
		if ok {
			sx.Samples = append(sx.Samples, samples...)
			return nil
		}
		lbl, err := lf.Labels(ctx, validID)
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
	sf := fields.From(ctx, constants.MetricsSeries)
	hf := fields.From(ctx, constants.MetricsHistogram)
	vf := fields.From(ctx, constants.MetricsValue)
	tf := fields.From(ctx, constants.MetricsTimestamp)
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

	// Filter to check if series is of histogram type
	histograms, err := hf.True(ctx.Index())
	if err != nil {
		return fmt.Errorf("reading histogram %w", err)
	}
	histograms = histograms.Intersect(filter)
	hasHistogram := !histograms.IsEmpty()

	// Filter to check if series is of histogram type
	floats, err := hf.False(ctx.Index())
	if err != nil {
		return fmt.Errorf("reading histogram %w", err)
	}
	floats = floats.Intersect(filter)
	hasFloats := !floats.IsEmpty()

	mapping := map[uint64]int{}
	for it.HasNext() {
		// This id is unique only in current view. We create a xxhash of the checksum
		// to have a global unique series.
		seriesHashID := it.Next()

		// Globally unique ID for the current series.
		seriesID := binary.BigEndian.Uint64(ctx.Tr(constants.MetricsSeries, seriesHashID))

		// Find all rows for each series matching the filter
		sr, err := sf.EqBSI(ctx.Index(), seriesHashID, filter)
		if err != nil {
			return fmt.Errorf("reading columns for series %w", err)
		}
		columns := sr.Columns()
		clear(mapping)
		for i := range columns {
			mapping[columns[i]] = i
		}

		chunks := make([]chunks.Sample, len(columns))

		if hasHistogram {
			hf := histograms.Intersect(sr)
			if hf.Any() {
				hs := &prompb.Histogram{}
				err := vf.ExtractBSI(ctx.Index(), hf, mapping, func(i int, v uint64) error {
					hs.Reset()
					err = hs.Unmarshal(ctx.Tr(constants.MetricsHistogram, v))
					if err != nil {
						return fmt.Errorf("reading histogram blob %w", err)
					}
					if _, isFloat := hs.Count.(*prompb.Histogram_CountFloat); isFloat {
						chunks[i] = NewFH(hs)
					} else {
						chunks[i] = NewH(hs)
					}
					return nil
				})
				if err != nil {
					return fmt.Errorf("extracting values %w", err)
				}
			}
		}
		if hasFloats {
			ff := floats.Intersect(sr)
			if ff.Any() {
				err := vf.ExtractBSI(ctx.Index(), ff, mapping, func(i int, v uint64) error {
					chunks[i] = &V{
						f: math.Float64frombits(v),
					}
					return nil
				})
				if err != nil {
					return fmt.Errorf("extracting values %w", err)
				}
				err = tf.ExtractBSI(ctx.Index(), ff, mapping, func(i int, v uint64) error {
					chunks[i].(*V).ts = int64(v)
					return nil
				})
				if err != nil {
					return fmt.Errorf("extracting timestamp %w", err)
				}
			}
		}
		err = add(lf, seriesID, columns[0], chunks)
		if err != nil {
			return err
		}
	}
	return nil
}

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
