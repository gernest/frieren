package logs

import (
	"github.com/gernest/frieren/internal/batch"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/logs/logproto"
	"github.com/gernest/frieren/internal/shardwidth"
	"github.com/gernest/frieren/internal/store"
)

type Batch struct {
	*batch.Batch
	rows int64
}

func (b *Batch) Append(seq *store.Seq, m *logproto.Stream) {
	currentShard := ^uint64(0)
	for _, e := range m.Entries {
		id := seq.NextID(constants.LogsRow)
		b.rows++
		shard := id / shardwidth.ShardWidth
		if shard != currentShard {
			currentShard = shard
			b.Shard(shard)
			b.AddMany(constants.MetricsFST, shard, m.Labels)
		}
		b.BSI(constants.LogsStreamID, shard, id, m.ID)
		b.BSI(constants.LogsTimestamp, shard, id, uint64(e.Timestamp))
		b.BSI(constants.LogsLine, shard, id, e.Line)
		b.BSISet(constants.LogsLabels, shard, id, m.Labels)
		if len(e.Metadata) > 0 {
			b.BSISet(constants.LogsMetadata, shard, id, e.Metadata)
		}
	}
}
