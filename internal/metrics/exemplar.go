package metrics

import (
	"context"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

var _ storage.ExemplarQueryable = (*Store)(nil)

func (s *Store) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return s, nil
}

var _ storage.ExemplarQuerier = (*Store)(nil)

func (s *Store) Select(start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	return s.db.Exemplars(start, end, matchers...)
}
