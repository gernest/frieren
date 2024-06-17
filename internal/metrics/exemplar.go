package metrics

import (
	"context"
	"fmt"
	"io"
	"slices"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/dgraph-io/badger/v4"
	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/fields"
	"github.com/gernest/frieren/internal/fst"
	"github.com/gernest/frieren/internal/query"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/frieren/internal/tags"
	"github.com/gernest/rbf"
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
	tx, err := e.store.Index.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	txn := e.store.DB.NewTransaction(false)
	defer txn.Discard()

	view, err := query.New(txn, tx, constants.METRICS, start, end)
	if err != nil {
		return nil, err
	}
	if view.IsEmpty() {
		return []exemplar.QueryResult{}, nil
	}
	m := make(ExemplarSet)
	err = view.Traverse(func(shard *v1.Shard, view string) error {
		tr := blob.Translate(txn, e.store, view)
		filters, err := fst.MatchSet(txn, tx, shard.Id, view, constants.MetricsFST, matchers...)
		if err != nil {
			return err
		}
		if len(filters) == 0 {
			return nil
		}
		r, err := tags.Filter(tx, fields.New(constants.MetricsLabels, shard.Id, view), filters)
		if err != nil {
			return err
		}
		return m.Build(txn, tx, tr, start, end, view, shard.Id, r)
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

func (s ExemplarSet) Build(txn *badger.Txn, tx *rbf.Tx, tr blob.Tr, start, end int64, view string, shard uint64, filter *rows.Row) error {
	lb := labels.NewScratchBuilder(1 << 10)

	add := func(lf *fields.Fragment, seriesID, validID uint64, exemplars *roaring64.Bitmap) error {
		sx, ok := s[seriesID]
		if !ok {
			lbl, err := lf.Labels(tx, tr, validID)
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
			ts.Unmarshal(tr(constants.MetricsExemplars, it.Next()))
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
	sf := fields.New(constants.MetricsSeries, shard, view)
	ef := fields.New(constants.MetricsExemplars, shard, view)
	tf := fields.New(constants.MetricsTimestamp, shard, view)
	lf := fields.New(constants.MetricsLabels, shard, view)

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
		var active uint64
		sr.RangeColumns(func(u uint64) error {
			active = u
			return io.EOF
		})
		o, err := ef.TransposeBSI(tx, sr)
		if err != nil {
			return err
		}
		err = add(lf, shard, active, o)
		if err != nil {
			return err
		}
	}
	return nil
}
