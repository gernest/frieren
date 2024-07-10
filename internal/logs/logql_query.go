package logs

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/gernest/frieren/internal/lbx"
	"github.com/gernest/rbf/dsl/bsi"
	"github.com/gernest/rbf/dsl/mutex"
	"github.com/gernest/rbf/dsl/query"
	"github.com/gernest/rbf/dsl/tx"
	"github.com/gernest/rows"
	"github.com/grafana/loki/pkg/push"
	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/protobuf/proto"
)

func (s *Store) Label(ctx context.Context, req *logproto.LabelRequest) (*logproto.LabelResponse, error) {
	return &logproto.LabelResponse{}, nil
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
				sm := &v1.Entry_StructureMetadata{}
				err = lbx.BSI(data, cols, meta, columns, txn.Shard, func(position int, value int64) error {
					err := proto.Unmarshal(txn.Tr.Blob("metadata", uint64(value)), sm)
					if err != nil {
						return fmt.Errorf("decoding metadata %w", err)
					}
					o := &st.Entries[size+position]
					o.StructuredMetadata = make(push.LabelsAdapter, 0, len(sm.Labels))
					for _, l := range sm.Labels {
						name, value, _ := strings.Cut(l, "=")
						o.StructuredMetadata = append(o.StructuredMetadata, push.LabelAdapter{
							Name: name, Value: value,
						})
					}
					return nil
				})
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
