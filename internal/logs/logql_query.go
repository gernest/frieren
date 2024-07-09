package logs

import (
	"bytes"
	"context"
	"strings"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/rbf/dsl/bsi"
	"github.com/gernest/rbf/dsl/mutex"
	"github.com/gernest/rbf/dsl/query"
	"github.com/gernest/rbf/dsl/tx"
	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/prometheus/prometheus/model/labels"
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
	// streamSet := map[uint64]*logproto.Stream{}
	bitmap := roaring64.New()

	for _, shard := range r.Range(req.Start, req.End) {
		r.View(shard, func(txn *tx.Tx) error {
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
			bitmap.Clear()
			err = mutex.Distinct(txn, "stream", bitmap, f)
			if err != nil {
				return err
			}
			if bitmap.IsEmpty() {
				return nil
			}
			it := bitmap.Iterator()
			for it.HasNext() {
			}
			stream, err := txn.Tx.Cursor(txn.Key("series"))
			if err != nil {
				return err
			}
			defer stream.Close()
			labels, err := txn.Tx.Cursor(txn.Key("labels"))
			if err != nil {
				return err
			}
			defer labels.Close()

			return nil
		})
	}
	return iter.NoopEntryIterator, nil
}

func clean(s string) string {
	s = strings.TrimPrefix(s, "^")
	s = strings.TrimSuffix(s, "$")
	return s
}

// var sep = []byte("=")

// func (qr *Querier) SelectLogs(ctx context.Context, req logql.SelectLogParams) (result iter.EntryIterator, err error) {
// 	expr, err := req.LogSelector()
// 	if err != nil {
// 		return nil, err
// 	}
// 	matchers := expr.Matchers()
// 	if len(matchers) == 0 {
// 		return iter.NoopEntryIterator, nil
// 	}
// 	plain := predicate.MatchersPlain(constants.LogsLabels, matchers...)
// 	plain = append(plain, &predicate.Between{
// 		Field: constants.LogsTimestamp,
// 		Start: uint64(req.Start.UnixNano()),
// 		End:   uint64(req.End.UnixNano()),
// 	})
// 	plain = predicate.Optimize(plain, true)
// 	match := predicate.And(plain)

// 	streamSet := map[uint64]*logproto.Stream{}
// 	err = query.Query(qr.db, constants.LOGS, req.Start, req.End, func(view *store.View) error {
// 		r, err := match.Apply(view)
// 		if err != nil {
// 			return err
// 		}
// 		if r.IsEmpty() {
// 			return nil
// 		}
// 		//Find all unique streams
// 		stream := fields.From(view, constants.LogsStreamID)
// 		streams, err := stream.TransposeBSI(view.Index(), r)
// 		if err != nil {
// 			return err
// 		}
// 		it := streams.Iterator()
// 		mapping := map[uint64]int{}
// 		ts := fields.From(view, constants.LogsTimestamp)
// 		line := fields.From(view, constants.LogsLine)
// 		labels := fields.From(view, constants.LogsLabels)
// 		meta := fields.From(view, constants.LogsMetadata)
// 		b := roaring64.New()
// 		for it.HasNext() {
// 			streamHashID := it.Next()
// 			// streamHashID is local to this view, we use xxhash to generate a global
// 			// unique stream hash.
// 			//
// 			// We use Tr to make sure the hash blob is cached.
// 			streamID := binary.BigEndian.Uint64(view.Tr(constants.LogsStreamID, streamHashID))

// 			// find all rows for the current stream ID matching the filter.
// 			streamRows, err := stream.EqBSI(view.Index(), streamHashID, r)
// 			if err != nil {
// 				return err
// 			}
// 			columns := streamRows.Columns()
// 			clear(mapping)
// 			for i := range columns {
// 				mapping[columns[i]] = i
// 			}
// 			result := make([]logproto.Entry, len(columns))

// 			err = ts.ExtractBSI(view.Index(), r, mapping, func(i int, v uint64) error {
// 				result[i].Timestamp = time.Unix(0, int64(v))
// 				return nil
// 			})
// 			if err != nil {
// 				return err
// 			}
// 			err = line.ExtractBSI(view.Index(), r, mapping, func(i int, v uint64) error {
// 				result[i].Line = string(view.Tr(constants.LogsLine, v))
// 				return nil
// 			})
// 			if err != nil {
// 				return err
// 			}
// 			for i, column := range columns {
// 				o, err := attr(view, b, meta, column)
// 				if err != nil {
// 					return err
// 				}
// 				result[i].StructuredMetadata = o
// 			}
// 			sx, ok := streamSet[streamID]
// 			if !ok {
// 				o, err := attrString(view, b, labels, columns[0])
// 				if err != nil {
// 					return err
// 				}
// 				sx = &push.Stream{
// 					Labels:  o.String(),
// 					Entries: result,
// 					Hash:    streamID,
// 				}
// 				streamSet[streamID] = sx
// 				continue
// 			}
// 			sx.Entries = append(sx.Entries, result...)
// 		}
// 		return nil
// 	})

// 	if err != nil {
// 		return nil, err
// 	}
// 	o := make([]push.Stream, 0, len(streamSet))
// 	for _, v := range streamSet {
// 		o = append(o, *v)
// 	}
// 	return iter.NewStreamsIterator(o, req.Direction), nil
// }

// func attr(ctx *predicate.Context, b *roaring64.Bitmap, f *fields.Fragment, column uint64) ([]push.LabelAdapter, error) {
// 	b.Clear()
// 	err := f.RowsBitmap(ctx.Index(), 0, b, roaring.NewBitmapColumnFilter(column))
// 	if err != nil {
// 		return nil, err
// 	}
// 	if b.IsEmpty() {
// 		return []push.LabelAdapter{}, nil
// 	}
// 	o := make([]push.LabelAdapter, 0, b.GetCardinality())
// 	x := b.Iterator()
// 	for x.HasNext() {
// 		value := ctx.Tr(constants.LogsLabels, x.Next())
// 		key, value, _ := bytes.Cut(value, sep)
// 		o = append(o, push.LabelAdapter{
// 			Name: string(key), Value: string(value),
// 		})
// 	}
// 	return o, nil
// }

// func attrString(ctx *predicate.Context, b *roaring64.Bitmap, f *fields.Fragment, column uint64) (labels.Labels, error) {
// 	b.Clear()
// 	err := f.RowsBitmap(ctx.Index(), 0, b, roaring.NewBitmapColumnFilter(column))
// 	if err != nil {
// 		return nil, err
// 	}
// 	if b.IsEmpty() {
// 		return []labels.Label{}, nil
// 	}

// 	o := make(labels.Labels, 0, b.GetCardinality())
// 	x := b.Iterator()
// 	for x.HasNext() {
// 		v := ctx.Tr(constants.LogsLabels, x.Next())
// 		key, value, _ := bytes.Cut(v, sep)
// 		o = append(o, labels.Label{
// 			Name: string(key), Value: string(value),
// 		})
// 	}
// 	return o, nil
// }
