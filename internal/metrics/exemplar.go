package metrics

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/bsi"
	"github.com/gernest/rbf/dsl/cursor"
	"github.com/gernest/rbf/dsl/mutex"
	rq "github.com/gernest/rbf/dsl/query"
	"github.com/gernest/rbf/dsl/tx"
	"github.com/gernest/roaring"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
)

var _ storage.ExemplarQueryable = (*Store)(nil)

func (s *Store) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return s, nil
}

var _ storage.ExemplarQuerier = (*Store)(nil)

func (s *Store) Select(start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {

	// We use timestamp filter as the base filter
	base := bsi.Filter("timestamp", bsi.RANGE, start, end)

	var b bytes.Buffer
	filter := make(rq.Or, 0, len(matchers))

	for _, match := range matchers {
		a := make(rq.And, 0, len(match))
		for _, m := range match {
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
			a = append(a, &mutex.MatchString{
				Field: "labels",
				Op:    op,
				Value: b.String(),
			})
		}
		filter = append(filter, a)
	}

	r, err := s.Reader()
	if err != nil {
		return nil, err
	}
	bitmap := roaring64.New()
	discard := roaring64.New()
	result := make([]exemplar.QueryResult, 0, 1<<10)
	for _, shard := range r.Range(time.UnixMilli(start), time.UnixMilli(end)) {
		err = r.View(shard, func(txn *tx.Tx) error {
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

			ex, err := txn.Tx.Cursor(txn.Key("exemplar"))
			if err != nil {
				return err
			}
			defer ex.Close()

			for it.HasNext() {
				series := it.Next()
				if discard.Contains(series) {
					continue
				}

				// Find all rows for this series + filter
				row, err := cursor.Row(seriesCursor, txn.Shard, series)
				if err != nil {
					return err
				}
				row = row.Intersect(f)
				err = row.RangeColumns(func(u uint64) error {
					defer discard.Add(series)

					e, err := readExemplars(ex, txn, u)
					if err != nil {
						return err
					}
					if len(e) == 0 {
						return nil
					}
					lbl, err := readLabels(labels, txn, u)
					if err != nil {
						return err
					}
					result = append(result, exemplar.QueryResult{
						SeriesLabels: lbl,
						Exemplars:    e,
					})
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

		})

		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func readExemplars(c *rbf.Cursor, txn *tx.Tx, column uint64) (o []exemplar.Exemplar, err error) {
	var e prompb.Exemplar
	err = cursor.Rows(c, 0, func(row uint64) error {
		b := txn.Tr.Blob("exemplar", row)
		if len(b) > 0 {
			e.Reset()
			err := e.Unmarshal(b)
			if err != nil {
				return err
			}
			o = append(o, decodeExemplar(&e))
		}
		return nil
	}, roaring.NewBitmapColumnFilter(column))
	return
}

func decodeExemplar(ex *prompb.Exemplar) exemplar.Exemplar {
	o := exemplar.Exemplar{
		Value: ex.Value,
		Ts:    ex.Timestamp,
	}
	if len(ex.Labels) > 0 {
		o.Labels = make(labels.Labels, len(ex.Labels))
		for i := range ex.Labels {
			o.Labels[i] = labels.Label{
				Name:  ex.Labels[i].Name,
				Value: ex.Labels[i].Value,
			}
		}
	}
	return o
}
