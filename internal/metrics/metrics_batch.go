package metrics

import (
	"context"
	"time"

	"github.com/gernest/frieren/internal/batch"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/metrics/metricsproto"
	"github.com/gernest/frieren/internal/shardwidth"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/rbf/quantum"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type LabelFunc func([]prompb.Label) []uint64

func Append(ctx context.Context, db *store.Store, mets pmetric.Metrics, ts time.Time) (err error) {
	view := quantum.ViewByTimeUnit("", ts, 'D')
	return batch.Append(ctx, constants.METRICS, db, view,
		func(tx *store.View, bx *batch.Batch) error {
			ms := metricsproto.From(mets, tx)
			defer ms.Release()

			for _, s := range ms {
				appendBatch(bx, s, tx.Seq)
			}
			meta := prometheusremotewrite.OtelMetricsToMetadata(mets, true)
			return StoreMetadata(tx.Txn(), meta)
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
