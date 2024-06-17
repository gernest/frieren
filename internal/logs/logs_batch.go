package logs

import (
	"context"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/batch"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/logs/logproto"
	"github.com/gernest/frieren/internal/shardwidth"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/rbf/quantum"
	"go.opentelemetry.io/collector/pdata/plog"
)

func AppendBatch(ctx context.Context, store *store.Store, ld plog.Logs, ts time.Time) error {
	view := quantum.ViewByTimeUnit("", ts, 'D')
	return batch.Append(ctx, constants.LOGS, store, view, func(bx *batch.Batch) error {
		return store.DB.Update(func(txn *badger.Txn) error {
			seq := store.Seq.Sequence(view)
			defer seq.Release()
			all := logproto.FromLogs(ld, blob.Upsert(txn, store, seq, view))
			for _, v := range all {
				append(bx, seq, v)
			}
			return nil
		})
	})
}

func append(b *batch.Batch, seq *store.Sequence, m *logproto.Stream) {
	currentShard := ^uint64(0)
	for _, e := range m.Entries {
		id := seq.NextID(constants.LogsRow)
		b.Rows++
		shard := id / shardwidth.ShardWidth
		if shard != currentShard {
			currentShard = shard
			b.Shard(shard)
			b.AddMany(constants.LogsFST, shard, m.Labels)
		}
		b.BSI(constants.LogsStreamID, shard, id, m.ID)
		b.BSI(constants.LogsTimestamp, shard, id, uint64(e.Timestamp))
		b.BSI(constants.LogsLine, shard, id, e.Line)
		b.Set(constants.LogsLabels, shard, id, m.Labels)
		if len(e.Metadata) > 0 {
			b.Set(constants.LogsMetadata, shard, id, e.Metadata)
		}
	}
}
