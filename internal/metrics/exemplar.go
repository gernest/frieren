package metrics

import (
	"context"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/fields"
	"github.com/gernest/frieren/internal/predicate"
	"github.com/gernest/frieren/internal/query"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/rows"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
)

func EncodeExemplars(value []prompb.Exemplar) ([]byte, error) {
	ts := prompb.TimeSeries{Exemplars: value}
	return ts.Marshal()
}

type ExemplarQueryable struct {
	store *store.Store
}

func NewExemplarQueryable(db *store.Store) *ExemplarQueryable {
	return &ExemplarQueryable{store: db}
}

var _ storage.ExemplarQueryable = (*ExemplarQueryable)(nil)

func (e *ExemplarQueryable) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return e, nil
}

var _ storage.ExemplarQuerier = (*ExemplarQueryable)(nil)

func (e *ExemplarQueryable) Select(start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	m := make(ExemplarSet)
	match := predicate.MultiMatchers(constants.MetricsLabels, matchers...)
	err := query.Query(e.store, constants.METRICS, time.UnixMilli(start), time.UnixMilli(end), func(view *store.View) error {
		r, err := match.Apply(view)
		if err != nil {
			return err
		}
		if r.IsEmpty() {
			return nil
		}
		return m.Build(view, r)
	})
	if err != nil {
		return nil, err
	}
	o := make([]exemplar.QueryResult, 0, len(m))
	for _, e := range m {
		o = append(o, *e)
	}
	return o, nil
}

type ExemplarSet map[uint64]*exemplar.QueryResult

func (s ExemplarSet) Build(view *store.View, filter *rows.Row) error {
	lb := labels.NewScratchBuilder(1 << 10)

	add := func(lf *fields.Fragment, seriesID, validID uint64, exemplars *roaring64.Bitmap) error {
		sx, ok := s[seriesID]
		if !ok {
			lbl, err := lf.Labels(view, validID)
			if err != nil {
				return fmt.Errorf("reading labels %w", err)
			}
			sx = &exemplar.QueryResult{}
			sx.SeriesLabels = lbl
			return nil
		}
		it := exemplars.Iterator()
		ts := &prompb.TimeSeries{}
		for it.HasNext() {
			ts.Reset()
			ts.Unmarshal(view.Tr(constants.MetricsExemplars, it.Next()))
			sx.Exemplars = slices.Grow(sx.Exemplars, len(ts.Exemplars))
			for i := range ts.Exemplars {
				ex := &ts.Exemplars[i]
				o := exemplar.Exemplar{
					Value: ex.Value,
					Ts:    ex.Timestamp,
				}
				if len(ex.Labels) > 0 {
					lb.Reset()
					for j := range ex.Labels {
						lb.Add(ex.Labels[j].Name, ex.Labels[j].Value)
					}
					lb.Sort()
					o.Labels = lb.Labels()
				}
				sx.Exemplars = append(sx.Exemplars, o)
			}
		}
		return nil
	}
	// fragments
	sf := fields.From(view, constants.MetricsSeries)
	ef := fields.From(view, constants.MetricsExemplars)
	lf := fields.From(view, constants.MetricsLabels)

	// find all series
	series, err := sf.TransposeBSI(view.Index(), filter)
	if err != nil {
		return err
	}

	// iterate on each series
	it := series.Iterator()
	hash := new(store.Hash)
	for it.HasNext() {
		seriesID := it.Next()
		seriesHashID := hash.Sum(view.Tr(constants.MetricsSeries, seriesID))
		sr, err := sf.EqBSI(view.Index(), seriesID, filter)
		if err != nil {
			return fmt.Errorf("reading columns for series %w", err)
		}

		var active uint64
		sr.RangeColumns(func(u uint64) error {
			active = u
			return io.EOF
		})
		o, err := ef.TransposeBSI(view.Index(), sr)
		if err != nil {
			return err
		}
		err = add(lf, seriesHashID, active, o)
		if err != nil {
			return err
		}
	}
	return nil
}
