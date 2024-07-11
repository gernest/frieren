package metrics

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/frieren/internal/lbx"
	"github.com/gernest/rbf/dsl/bsi"
	"github.com/gernest/rbf/dsl/mutex"
	rq "github.com/gernest/rbf/dsl/query"
	"github.com/gernest/rbf/dsl/tx"
	"github.com/gernest/rows"
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

	// we only read a series once across all shards.
	discard := roaring64.New()

	result := make([]exemplar.QueryResult, 0, 1<<10)
	read := make([]labels.Labels, 0, 8)
	readRows := make([]uint64, 0, 8)

	err = r.View(func(txn *tx.Tx) error {
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

		series, err := txn.Tx.Cursor("series")
		if err != nil {
			return err
		}
		defer series.Close()
		labels, err := txn.Tx.Cursor("labels")
		if err != nil {
			return err
		}
		defer labels.Close()

		ex, err := txn.Tx.Cursor("exemplar")
		if err != nil {
			return err
		}
		defer ex.Close()
		read = read[:0]
		readRows = readRows[:0]
		err = lbx.Distinct(series, f, txn.Shard, func(value uint64, columns *rows.Row) error {
			if discard.Contains(value) {
				return nil
			}
			return columns.RangeColumns(func(u uint64) error {
				defer discard.Add(value)
				lbl, err := lbx.Labels(labels, "labels", txn, columns.Columns()[0])
				if err != nil {
					return err
				}
				read = append(read, lbl)
				readRows = append(readRows, u)
				return io.EOF
			})
		})
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
		}
		if len(read) > 0 {
			data := lbx.NewData(readRows)
			sm := &prompb.TimeSeries{}
			mapping := map[uint64]int{}
			for i := range readRows {
				mapping[readRows[i]] = i
			}
			err = lbx.BSI(data, readRows, ex, rows.NewRow(readRows...), txn.Shard, func(position int, value int64) error {
				data := txn.Tr.Blob("exemplar", uint64(value))
				if len(data) == 0 {
					return nil
				}
				sm.Reset()
				err := sm.Unmarshal(data)
				if err != nil {
					return fmt.Errorf("decoding exemplar %w", err)
				}
				o := exemplar.QueryResult{
					SeriesLabels: read[position].Copy(),
					Exemplars:    make([]exemplar.Exemplar, len(sm.Exemplars)),
				}
				for i := range sm.Exemplars {
					o.Exemplars[i] = decodeExemplar(&sm.Exemplars[i])
				}
				result = append(result, o)
				return nil
			})
			if err != nil {
				return fmt.Errorf("reading exemplar %w", err)
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
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
