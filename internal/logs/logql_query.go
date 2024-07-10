package logs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/blevesearch/vellum"
	"github.com/gernest/frieren/internal/lbx"
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/bsi"
	"github.com/gernest/rbf/dsl/cursor"
	"github.com/gernest/rbf/dsl/mutex"
	"github.com/gernest/rbf/dsl/query"
	"github.com/gernest/rbf/dsl/tx"
	"github.com/gernest/roaring"
	"github.com/gernest/rows"
	"github.com/grafana/loki/pkg/push"
	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/prometheus/prometheus/model/labels"
)

func (s *Store) Label(ctx context.Context, req *logproto.LabelRequest) (*logproto.LabelResponse, error) {
	r, err := s.Reader()
	if err != nil {
		return nil, err
	}
	m := map[string]struct{}{}
	if req.Values {
		prefix := []byte(req.Name + "=")
		err = r.Tr().Search("labels", &vellum.AlwaysMatch{}, prefix, nil, func(key []byte, value uint64) error {
			if !bytes.HasPrefix(key, prefix) {
				return io.EOF
			}
			_, v, _ := bytes.Cut(key, sep)
			m[string(v)] = struct{}{}
			return nil
		})
		if err != nil {
			if errors.Is(err, io.EOF) {
				return &logproto.LabelResponse{}, nil
			}
			return nil, err
		}
	} else {
		err = r.Tr().Search("labels", &vellum.AlwaysMatch{}, nil, nil, func(key []byte, value uint64) error {
			name, _, _ := bytes.Cut(key, sep)
			m[string(name)] = struct{}{}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	o := &logproto.LabelResponse{Values: make([]string, len(m))}
	for k := range m {
		o.Values = append(o.Values, k)
	}
	slices.Sort(o.Values)
	return o, nil
}

func (*Store) SelectSamples(_ context.Context, req logql.SelectSampleParams) (iter.SampleIterator, error) {
	return iter.NoopSampleIterator, nil
}

func (s *Store) SelectLogs(ctx context.Context, req logql.SelectLogParams) (result iter.EntryIterator, err error) {
	expr, err := req.LogSelector()
	if err != nil {
		return nil, err
	}
	matchers := expr.Matchers()
	if len(matchers) == 0 {
		return iter.NoopEntryIterator, nil
	}

	// We use timestamp filter as the base filter
	base := bsi.Filter("timestamp", bsi.RANGE, req.Start.UnixNano(), req.End.UnixNano())

	// all matchers use AND
	filter := make(query.And, 0, len(matchers))
	var b bytes.Buffer
	for _, m := range matchers {
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
		filter = append(filter, &mutex.MatchString{
			Field: "labels",
			Op:    op,
			Value: b.String(),
		})
	}
	r, err := s.Reader()
	if err != nil {
		return nil, err
	}
	defer r.Release()
	streamSet := map[uint64]*logproto.Stream{}

	for _, shard := range r.Range(req.Start, req.End) {
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
			stream, err := txn.Tx.Cursor(txn.Key("stream"))
			if err != nil {
				return err
			}
			defer stream.Close()
			labels, err := txn.Tx.Cursor(txn.Key("labels"))
			if err != nil {
				return err
			}
			defer labels.Close()
			ts, err := txn.Tx.Cursor(txn.Key("timestamp"))
			if err != nil {
				return err
			}
			defer ts.Close()

			line, err := txn.Tx.Cursor(txn.Key("line"))
			if err != nil {
				return err
			}
			defer line.Close()

			meta, err := txn.Tx.Cursor(txn.Key("metadata"))
			if err != nil {
				return err
			}
			defer meta.Close()

			return lbx.Distinct(stream, f, txn.Shard, func(rowID uint64, columns *rows.Row) error {
				cols := r.Columns()
				st, ok := streamSet[rowID]
				if !ok {
					st = &logproto.Stream{}
					lbl, err := lbx.Labels(labels, "labels", txn, cols[0])
					if err != nil {
						return fmt.Errorf("reading labels %w", err)
					}
					st.Labels = lbl.String()
					st.Hash = rowID
				}

				// Prepare entries
				size := len(st.Entries)
				count := len(cols)
				st.Entries = slices.Grow(st.Entries, int(count))[:size+int(count)]

				data := lbx.NewData(cols)

				// read timestamp
				err = lbx.BSI(data, cols, ts, columns, txn.Shard, func(position int, value int64) error {
					st.Entries[size+position].Timestamp = time.Unix(0, value)
					return nil
				})
				if err != nil {
					return fmt.Errorf("reading timestamp %w", err)
				}
				// read line
				err = lbx.BSI(data, cols, ts, columns, txn.Shard, func(position int, value int64) error {
					st.Entries[size+position].Line = string(txn.Tr.Blob("line", uint64(value)))
					return nil
				})
				if err != nil {
					return fmt.Errorf("reading line %w", err)
				}

				// read metadata
				for i := range cols {
					md, err := metadata(meta, "metadata", txn, cols[i])
					if err != nil {
						return fmt.Errorf("reading metadata%w", err)
					}
					if len(md) > 0 {
						st.Entries[size+i].StructuredMetadata = md
					}
				}
				if err != nil {
					return fmt.Errorf("reading metadata %w", err)
				}
				return nil
			})

		})
		if err != nil {
			return nil, err
		}
	}
	o := make([]push.Stream, 0, len(streamSet))
	for _, v := range streamSet {
		o = append(o, *v)
	}
	return iter.NewStreamsIterator(o, req.Direction), nil
}

func clean(s string) string {
	s = strings.TrimPrefix(s, "^")
	s = strings.TrimSuffix(s, "$")
	return s
}

var sep = []byte("=")

func metadata(c *rbf.Cursor, field string, tx *tx.Tx, rowID uint64) (o push.LabelsAdapter, err error) {
	err = cursor.Rows(c, 0, func(row uint64) error {
		data := tx.Tr.Key(field, row)
		if len(data) == 0 {
			return nil
		}
		name, value, _ := bytes.Cut(data, sep)
		o = append(o, push.LabelAdapter{
			Name:  string(name),
			Value: string(value),
		})
		return nil
	}, roaring.NewBitmapColumnFilter(rowID))
	if err != nil {
		return nil, err
	}
	return
}
