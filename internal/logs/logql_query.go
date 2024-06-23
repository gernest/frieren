package logs

import (
	"bytes"
	"context"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/gernest/frieren/internal/blob"
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
	"github.com/prometheus/prometheus/model/labels"
)

type Querier struct {
	db *store.Store
}

var _ logql.Querier = (*Querier)(nil)

func (Querier) SelectSamples(context.Context, logql.SelectSampleParams) (iter.SampleIterator, error) {
	return iter.NoopIterator, nil
}

var sep = []byte("=")

func (qr *Querier) SelectLogs(ctx context.Context, req logql.SelectLogParams) (iter.EntryIterator, error) {
	expr, err := req.LogSelector()
	if err != nil {
		return nil, err
	}
	matchers := expr.Matchers()
	if len(matchers) == 0 {
		return iter.NoopIterator, nil
	}
	txn := qr.db.DB.NewTransaction(false)
	defer txn.Discard()
	tx, err := qr.db.Index.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	views, err := query.New(txn, tx, constants.LOGS, req.Start, req.End)
	if err != nil {
		return nil, err
	}
	if views.IsEmpty() {
		return iter.NoopIterator, nil
	}
	match := predicate.Matchers(constants.LogsLabels, matchers...)
	hash := new(blob.Hash)

	streamSet := map[uint64]*logproto.Stream{}

	err = views.Traverse(func(shard *v1.Shard, view string) error {
		filterCtx := predicate.NewContext(shard, view, qr.db, tx, txn)
		r, err := match.Apply(filterCtx)
		if err != nil {
			return err
		}
		if r.IsEmpty() {
			return nil
		}

		//Find all unique streams
		stream := fields.New(constants.LogsStreamID, shard.Id, view)
		streams, err := stream.TransposeBSI(tx, r)
		if err != nil {
			return err
		}
		it := streams.Iterator()

		mapping := map[uint64]int{}
		ts := filterCtx.Field(constants.LogsTimestamp)
		line := filterCtx.Field(constants.LogsLine)
		labels := filterCtx.Field(constants.LogsLabels)
		meta := filterCtx.Field(constants.LogsMetadata)
		b := roaring64.New()
		for it.HasNext() {
			streamHashID := it.Next()
			// streamHashID is local to this view, we use xxhash to generate a global
			// unique stream hash.
			//
			// We use Tr to make sure the hash blob is cached.
			streamID := hash.Sum(filterCtx.Tr(constants.LogsStreamID, streamHashID))

			// find all rows for the current stream ID matching the filter.
			streamRows, err := stream.EqBSI(tx, streamHashID, r)
			if err != nil {
				return err
			}
			columns := streamRows.Columns()
			clear(mapping)
			for i := range columns {
				mapping[columns[i]] = i
			}
			result := make([]logproto.Entry, len(columns))

			err = ts.ExtractBSI(tx, r, mapping, func(i int, v uint64) error {
				result[i].Timestamp = time.Unix(0, int64(v))
				return nil
			})
			if err != nil {
				return err
			}
			err = line.ExtractBSI(tx, r, mapping, func(i int, v uint64) error {
				result[i].Line = string(filterCtx.Tr(constants.LogsLine, v))
				return nil
			})
			if err != nil {
				return err
			}
			for i, column := range columns {
				o, err := attr(filterCtx, b, meta, column)
				if err != nil {
					return err
				}
				result[i].StructuredMetadata = o
			}
			sx, ok := streamSet[streamID]
			if !ok {
				o, err := attrString(filterCtx, b, labels, columns[0])
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
	err := f.RowsBitmap(ctx.Tx, 0, b, roaring.NewBitmapColumnFilter(column))
	if err != nil {
		return nil, err
	}
	if b.IsEmpty() {
		return []push.LabelAdapter{}, nil
	}
	o := make([]push.LabelAdapter, 0, b.GetCardinality())
	x := b.Iterator()
	for x.HasNext() {
		err = ctx.TrCall(constants.LogsLabels, x.Next(), func(v []byte) error {
			key, value, _ := bytes.Cut(v, sep)
			o = append(o, push.LabelAdapter{
				Name: string(key), Value: string(value),
			})
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return o, nil
}

func attrString(ctx *predicate.Context, b *roaring64.Bitmap, f *fields.Fragment, column uint64) (labels.Labels, error) {
	b.Clear()
	err := f.RowsBitmap(ctx.Tx, 0, b, roaring.NewBitmapColumnFilter(column))
	if err != nil {
		return nil, err
	}
	if b.IsEmpty() {
		return []labels.Label{}, nil
	}

	o := make(labels.Labels, 0, b.GetCardinality())
	x := b.Iterator()
	for x.HasNext() {
		err = ctx.TrCall(constants.LogsLabels, x.Next(), func(v []byte) error {
			key, value, _ := bytes.Cut(v, sep)
			o = append(o, labels.Label{
				Name: string(key), Value: string(value),
			})
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return o, nil
}
