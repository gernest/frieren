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
	"github.com/gernest/rbf/quantum"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func AppendBatch(ctx context.Context, store *store.Store, td ptrace.Traces, ts time.Time) error {
	view := quantum.ViewByTimeUnit("", ts, 'D')
	return batch.Append(ctx, constants.TRACES, store, view, func(bx *batch.Batch) error {
		return store.DB.Update(func(txn *badger.Txn) error {
			seq := store.Seq.Sequence(view)
			defer seq.Release()
			a := appendTrace(bx, seq)

			all := traceproto.From(td, blob.Upsert(txn, store, seq, view))
			for _, t := range all {
				a(t)
			}
			return nil
		})
	})
}

func appendTrace(b *batch.Batch, seq *store.Sequence) func(trace *traceproto.Trace) {
	currentShard := ^uint64(0)
	return func(trace *traceproto.Trace) {
		duration := trace.End - trace.Start
		for _, span := range trace.Spans {
			id := seq.NextID(constants.TracesRow)
			b.Rows++
			shard := id / shardwidth.ShardWidth
			if shard != currentShard {
				currentShard = shard
				b.Shard(shard)
			}
			b.Or(constants.TracesFST, shard, span.FST)
			b.BSI(constants.TracesResource, shard, id, span.Resource)
			b.BSI(constants.TracesScope, shard, id, span.Scope)
			b.BSI(constants.TracesSpan, shard, id, span.Span)
			b.SetBitmap(constants.TracesLabels, shard, id, span.Tags)
			b.BSI(constants.TracesStart, shard, id, trace.Start)
			b.BSI(constants.TracesEnd, shard, id, trace.End)
			b.BSI(constants.TracesSpanStart, shard, id, span.Start)
			b.BSI(constants.TracesSpanEnd, shard, id, span.End)
			b.BSI(constants.TracesSpanDuration, shard, id, span.End-span.Start)
			b.BSI(constants.TracesDuration, shard, id, duration)
		}
	}
}
