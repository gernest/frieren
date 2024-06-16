package traces

import (
	"context"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/batch"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/shardwidth"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/frieren/internal/traces/traceproto"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func AppendBatch(ctx context.Context, store *store.Store, td ptrace.Traces, ts time.Time) error {
	return batch.Append(ctx, constants.TRACES, store, ts, func(bx *batch.Batch) error {
		return store.DB.Update(func(txn *badger.Txn) error {
			traceproto.From(td, blob.Upsert(txn, store),
				append(bx, store.Seq))
			return nil
		})
	})
}

func append(b *batch.Batch, seq *store.Seq) func(span *traceproto.Span) {
	currentShard := ^uint64(0)

	return func(span *traceproto.Span) {
		id := seq.NextID(constants.TracesRow)
		b.Rows++
		shard := id / shardwidth.ShardWidth
		if shard != currentShard {
			currentShard = shard
			b.Shard(shard)
		}

		// Combine both resource and span attributes for fst.
		b.AddMany(constants.TracesFST, shard, span.Resource)
		b.AddMany(constants.TracesFST, shard, span.Scope)
		b.AddMany(constants.TracesFST, shard, span.Span)

		b.BSISet(constants.TracesResource, shard, id, span.Resource)
		b.BSISet(constants.TracesScope, shard, id, span.Scope)
		b.BSISet(constants.TracesSpan, shard, id, span.Span)
		b.BSI(constants.TracesTracesID, shard, id, span.TraceID)
		b.BSI(constants.TracesSpanID, shard, id, span.SpanID)
		b.BSI(constants.TracesParent, shard, id, span.Parent)
		if span.Parent != 0 {
			// Store parent_span -> SET(children_span)
			b.BSISetOne(constants.TracesFamily, span.Parent, span.SpanID)
		}
		b.BSI(constants.TracesName, shard, id, span.Name)
		b.Mutex(constants.TracesKind, shard, id, span.Kind)
		b.BSI(constants.TracesStart, shard, id, span.Start)
		b.BSI(constants.TracesEnd, shard, id, span.End)
		b.BSI(constants.TracesDuration, shard, id, span.Duration)
		if span.Events != 0 {
			b.BSI(constants.TracesEvents, shard, id, span.Events)
		}
		if span.Links != 0 {
			b.BSI(constants.TracesLinks, shard, id, span.Links)
		}
		b.Mutex(constants.TracesStatusCode, shard, id, span.StatusCode)
		b.Mutex(constants.TracesStatusMessage, shard, id, span.StatusMessage)
	}
}
