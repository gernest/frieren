package metrics

import (
	"context"
	"crypto/sha512"
	"math"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/batch"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/shardwidth"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/frieren/internal/util"
	"github.com/gernest/rbf/quantum"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type LabelFunc func([]prompb.Label) []uint64

func Append(ctx context.Context, store *store.Store, mets pmetric.Metrics, ts time.Time) (err error) {
	view := quantum.ViewByTimeUnit("", ts, 'D')
	return batch.Append(ctx, constants.METRICS, store, view,
		func(bx *batch.Batch) error {
			return store.DB.Update(func(txn *badger.Txn) error {
				series, err := prometheusremotewrite.FromMetrics(mets, prometheusremotewrite.Settings{
					AddMetricSuffixes: true,
				})
				if err != nil {
					return err
				}
				seq := store.Seq.Sequence(view)
				defer seq.Release()

				blob := blob.Upsert(txn, store, seq, view)
				label := UpsertLabels(blob)

				for _, s := range series {
					appendBatch(bx, s, label, blob, seq)
				}
				meta := prometheusremotewrite.OtelMetricsToMetadata(mets, true)
				return StoreMetadata(txn, meta)
			})
		})
}

func appendBatch(b *batch.Batch, ts *prompb.TimeSeries, labelFunc LabelFunc, blobFunc blob.Func, seq *store.Sequence) {
	labels := labelFunc(ts.Labels)
	checksum := sha512.Sum512(util.Uint64ToBytes(labels))
	series := blobFunc(constants.MetricsSeries, checksum[:])

	currentShard := ^uint64(0)
	var exemplars uint64
	if len(ts.Exemplars) > 0 {
		data, _ := EncodeExemplars(ts.Exemplars)
		exemplars = blobFunc(constants.MetricsExemplars, data)
	}
	for _, s := range ts.Samples {
		id := seq.NextID(constants.MetricsRow)
		b.Rows++
		shard := id / shardwidth.ShardWidth
		if shard != currentShard {
			currentShard = shard
			b.Shard(shard)
			b.AddMany(constants.MetricsFST, shard, labels)
		}
		b.BSI(constants.MetricsSeries, shard, id, series)
		b.BSI(constants.MetricsTimestamp, shard, id, uint64(s.Timestamp))
		value := math.Float64bits(s.Value)
		b.BSI(constants.MetricsValue, shard, id, value)
		b.Set(constants.MetricsLabels, shard, id, labels)
		if len(ts.Exemplars) > 0 {
			b.BSI(constants.MetricsExemplars, shard, id, exemplars)
		}
	}
	currentShard = ^uint64(0)

	for j := range ts.Histograms {
		s := &ts.Histograms[j]
		id := seq.NextID(constants.MetricsRow)
		b.Rows++
		shard := id / shardwidth.ShardWidth
		if shard != currentShard {
			currentShard = shard
			b.Shard(shard)
			b.AddMany(constants.MetricsFST, shard, labels)
		}
		data, _ := s.Marshal()
		value := blobFunc(constants.MetricsHistogram, data)
		b.BSI(constants.MetricsSeries, shard, id, series)
		b.BSI(constants.MetricsTimestamp, shard, id, uint64(s.Timestamp))
		b.BSI(constants.MetricsValue, shard, id, value)
		b.Set(constants.MetricsLabels, shard, id, labels)
		b.Bool(constants.MetricsHistogram, shard, id, true)
		if len(ts.Exemplars) > 0 {
			b.BSI(constants.MetricsExemplars, shard, id, exemplars)
		}
	}
}
