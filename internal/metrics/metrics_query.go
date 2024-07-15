package metrics

import (
	"context"

	"github.com/gernest/frieren/internal/store"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

type Queryable struct {
	store *Store
}

var _ storage.Queryable = (*Queryable)(nil)

func (q *Queryable) Querier(mints, maxts int64) (storage.Querier, error) {
	return &Querier{
		store: q.store,
	}, nil

}

type Querier struct {
	store *Store
}

var _ storage.Querier = (*Querier)(nil)

func (q *Querier) Close() error {
	return nil
}

func (s *Querier) LabelValues(ctx context.Context, name string, hits *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	values, err := s.store.db.Values(name)
	if err != nil {
		return nil, nil, err
	}
	return values, nil, err
}

func (s *Querier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	names, err := s.store.db.Names()
	if err != nil {
		return nil, nil, err
	}
	return names, nil, err
}

func (s *Querier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if len(matchers) == 0 {
		return storage.EmptySeriesSet()
	}
	rs, err := s.store.db.Select(hints.Start, hints.End, matchers...)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	return store.NewSeriesSet(rs)
}
