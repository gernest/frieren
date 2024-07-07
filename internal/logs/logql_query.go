package logs

import (
	"bytes"
	"context"
	"encoding/binary"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/fields"
	"github.com/gernest/frieren/internal/predicate"
	"github.com/gernest/frieren/internal/query"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/roaring"
	"github.com/grafana/loki/pkg/push"
	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/prometheus/prometheus/model/labels"
)

type Querier struct {
	db *store.Store
}

func NewQuerier(db *store.Store) *Querier {
	return &Querier{db: db}
}

var _ logql.Querier = (*Querier)(nil)

func (qr *Querier) Label(ctx context.Context, req *logproto.LabelRequest) (*logproto.LabelResponse, error) {
	now := time.Now()
	start, end := now.Add(-time.Hour), now
	if req.Start != nil {
		start = *req.Start
	}
	if req.End != nil {
		end = *req.End
	}
	if start.After(end) {
		return &logproto.LabelResponse{}, nil
	}
	var matchers []*labels.Matcher
	if req.Query != "" {
		var err error
		matchers, err = syntax.ParseMatchers(req.Query, true)
		if err != nil {
			return nil, err
		}
	}
	result, err := query.Labels(qr.db, constants.LOGS, constants.LogsLabels, start, end, req.Name, matchers...)
	if err != nil {
		return nil, err
	}
	return &logproto.LabelResponse{Values: result}, nil
}

func (Querier) SelectSamples(_ context.Context, req logql.SelectSampleParams) (iter.SampleIterator, error) {
	return iter.NoopSampleIterator, nil
}

var sep = []byte("=")

func (qr *Querier) SelectLogs(ctx context.Context, req logql.SelectLogParams) (result iter.EntryIterator, err error) {
	expr, err := req.LogSelector()
	if err != nil {
		return nil, err
	}
	matchers := expr.Matchers()
	if len(matchers) == 0 {
		return iter.NoopEntryIterator, nil
	}
	plain := predicate.MatchersPlain(constants.LogsLabels, matchers...)
	plain = append(plain, &predicate.Between{
		Field: constants.LogsTimestamp,
		Start: uint64(req.Start.UnixNano()),
		End:   uint64(req.End.UnixNano()),
	})
	plain = predicate.Optimize(plain, true)
	match := predicate.And(plain)

	streamSet := map[uint64]*logproto.Stream{}
	err = query.Query(qr.db, constants.LOGS, req.Start, req.End, func(view *store.View) error {
		r, err := match.Apply(view)
		if err != nil {
			return err
		}
		if r.IsEmpty() {
			return nil
		}
		//Find all unique streams
		stream := fields.From(view, constants.LogsStreamID)
		streams, err := stream.TransposeBSI(view.Index(), r)
		if err != nil {
			return err
		}
		it := streams.Iterator()
		mapping := map[uint64]int{}
		ts := fields.From(view, constants.LogsTimestamp)
		line := fields.From(view, constants.LogsLine)
		labels := fields.From(view, constants.LogsLabels)
		meta := fields.From(view, constants.LogsMetadata)
		b := roaring64.New()
		for it.HasNext() {
			streamHashID := it.Next()
			// streamHashID is local to this view, we use xxhash to generate a global
			// unique stream hash.
			//
			// We use Tr to make sure the hash blob is cached.
			streamID := binary.BigEndian.Uint64(view.Tr(constants.LogsStreamID, streamHashID))

			// find all rows for the current stream ID matching the filter.
			streamRows, err := stream.EqBSI(view.Index(), streamHashID, r)
			if err != nil {
				return err
			}
			columns := streamRows.Columns()
			clear(mapping)
			for i := range columns {
				mapping[columns[i]] = i
			}
			result := make([]logproto.Entry, len(columns))

			err = ts.ExtractBSI(view.Index(), r, mapping, func(i int, v uint64) error {
				result[i].Timestamp = time.Unix(0, int64(v))
				return nil
			})
			if err != nil {
				return err
			}
			err = line.ExtractBSI(view.Index(), r, mapping, func(i int, v uint64) error {
				result[i].Line = string(view.Tr(constants.LogsLine, v))
				return nil
			})
			if err != nil {
				return err
			}
			for i, column := range columns {
				o, err := attr(view, b, meta, column)
				if err != nil {
					return err
				}
				result[i].StructuredMetadata = o
			}
			sx, ok := streamSet[streamID]
			if !ok {
				o, err := attrString(view, b, labels, columns[0])
				if err != nil {
					return err
				}
				sx = &push.Stream{
					Labels:  o.String(),
					Entries: result,
					Hash:    streamID,
				}
				streamSet[streamID] = sx
				continue
			}
			sx.Entries = append(sx.Entries, result...)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	o := make([]push.Stream, 0, len(streamSet))
	for _, v := range streamSet {
		o = append(o, *v)
	}
	return iter.NewStreamsIterator(o, req.Direction), nil
}

func attr(ctx *predicate.Context, b *roaring64.Bitmap, f *fields.Fragment, column uint64) ([]push.LabelAdapter, error) {
	b.Clear()
	err := f.RowsBitmap(ctx.Index(), 0, b, roaring.NewBitmapColumnFilter(column))
	if err != nil {
		return nil, err
	}
	if b.IsEmpty() {
		return []push.LabelAdapter{}, nil
	}
	o := make([]push.LabelAdapter, 0, b.GetCardinality())
	x := b.Iterator()
	for x.HasNext() {
		value := ctx.Tr(constants.LogsLabels, x.Next())
		key, value, _ := bytes.Cut(value, sep)
		o = append(o, push.LabelAdapter{
			Name: string(key), Value: string(value),
		})
	}
	return o, nil
}

func attrString(ctx *predicate.Context, b *roaring64.Bitmap, f *fields.Fragment, column uint64) (labels.Labels, error) {
	b.Clear()
	err := f.RowsBitmap(ctx.Index(), 0, b, roaring.NewBitmapColumnFilter(column))
	if err != nil {
		return nil, err
	}
	if b.IsEmpty() {
		return []labels.Label{}, nil
	}

	o := make(labels.Labels, 0, b.GetCardinality())
	x := b.Iterator()
	for x.HasNext() {
		v := ctx.Tr(constants.LogsLabels, x.Next())
		key, value, _ := bytes.Cut(v, sep)
		o = append(o, labels.Label{
			Name: string(key), Value: string(value),
		})
	}
	return o, nil
}
