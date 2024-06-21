package metrics

import (
	"context"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/batch"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/metrics/metricsproto"
	"github.com/gernest/frieren/internal/shardwidth"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/quantum"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type LabelFunc func([]prompb.Label) []uint64

func Append(ctx context.Context, store *store.Store, mets pmetric.Metrics, ts time.Time) (err error) {
	view := quantum.ViewByTimeUnit("", ts, 'D')
	return batch.Append(ctx, constants.METRICS, store, view,
		func(txn *badger.Txn, _ *rbf.Tx, bx *batch.Batch) error {
			seq := store.Seq.Sequence(view)
			ms := metricsproto.From(mets, blob.Upsert(txn, store, seq, view))
			defer ms.Release()

			for _, s := range ms {
				appendBatch(bx, s, seq)
			}
			meta := prometheusremotewrite.OtelMetricsToMetadata(mets, true)
			return StoreMetadata(txn, meta)
		})
}

func appendBatch(b *batch.Batch, series *metricsproto.Series, seq *store.Sequence) {
	currentShard := ^uint64(0)
	for i := range series.Values {
		id := seq.NextID(constants.MetricsRow)
		b.Rows++
		shard := id / shardwidth.ShardWidth
		if shard != currentShard {
			currentShard = shard
			b.Shard(shard)
		}
		b.BSI(constants.MetricsSeries, shard, id, series.ID)
		b.BSI(constants.MetricsTimestamp, shard, id, series.Timestamp[i])
		b.BSI(constants.MetricsValue, shard, id, series.Values[i])
		b.Set(constants.MetricsLabels, shard, id, series.Labels)
		b.Set(constants.MetricsExemplars, shard, id, series.Exemplars)
	}
	currentShard = ^uint64(0)

	for i := range series.Histograms {
		id := seq.NextID(constants.MetricsRow)
		b.Rows++
		shard := id / shardwidth.ShardWidth
		if shard != currentShard {
			currentShard = shard
			b.Shard(shard)
		}
		b.BSI(constants.MetricsSeries, shard, id, series.ID)
		b.BSI(constants.MetricsTimestamp, shard, id, series.HistogramTS[i])
		b.BSI(constants.MetricsValue, shard, id, series.Histograms[i])
		b.Set(constants.MetricsLabels, shard, id, series.Labels)
		b.Bool(constants.MetricsHistogram, shard, id, true)
		b.Set(constants.MetricsExemplars, shard, id, series.Exemplars)
	}
}
