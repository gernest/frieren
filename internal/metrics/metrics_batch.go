package metrics

import (
	"context"
	"crypto/sha512"
	"math"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/batch"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/self"
	"github.com/gernest/frieren/internal/shardwidth"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/frieren/internal/util"
	"github.com/gernest/rbf/quantum"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type Batch struct {
	*batch.Batch
	rowsAdded int64
}

func NewBatch() *Batch {
	return batchPool.Get().(*Batch)
}

func newBatch() *Batch {
	return &Batch{
		Batch: batch.NewBatch(),
	}
}

var batchPool = &sync.Pool{New: func() any { return newBatch() }}

func (b *Batch) Release() {
	b.Reset()
	batchPool.Put(b)
}

func (b *Batch) Reset() {
	b.Batch.Reset()
	b.rowsAdded = 0
}

type LabelFunc func([]prompb.Label) []uint64

func (b *Batch) Append(ts *prompb.TimeSeries, labelFunc LabelFunc, blobFunc blob.Func, seq *store.Seq) {
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
		b.rowsAdded++
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
		b.BSISet(constants.MetricsLabels, shard, id, labels)
		if len(ts.Exemplars) > 0 {
			b.BSI(constants.MetricsExemplars, shard, id, exemplars)
		}
	}
	currentShard = ^uint64(0)

	for j := range ts.Histograms {
		s := &ts.Histograms[j]
		id := seq.NextID(constants.MetricsRow)
		b.rowsAdded++
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
		b.BSISet(constants.MetricsLabels, shard, id, labels)
		b.Bool(constants.MetricsHistogram, shard, id, true)
		if len(ts.Exemplars) > 0 {
			b.BSI(constants.MetricsExemplars, shard, id, exemplars)
		}
	}
}

var (
	batchRowsAdded metric.Int64Counter
	batchDuration  metric.Float64Histogram
	batchFailure   metric.Int64Counter
	batchLabels    metric.Int64Histogram
	batchSeries    metric.Int64Histogram
	batchOnce      sync.Once
)

func AppendBatch(ctx context.Context, store *store.Store, mets pmetric.Metrics, ts time.Time) (err error) {
	ctx, span := self.Start(ctx, "METRICS.batch")
	defer span.End()
	start := time.Now()

	batch := NewBatch()
	defer batch.Release()

	batchOnce.Do(func() {
		batchRowsAdded = self.Counter("metrics.batch.rows",
			metric.WithDescription("Total number of rows added"),
		)
		batchDuration = self.FloatHistogram("metrics.batch.duration",
			metric.WithDescription("Time in seconds of processing the batch"),
			metric.WithUnit("s"),
		)
		batchFailure = self.Counter("metrics.batch.failure",
			metric.WithDescription("Total number errored batch process"),
		)
		batchLabels = self.Histogram("metrics.batch.labels",
			metric.WithDescription("Unique labels processed per batch"),
		)
		batchSeries = self.Histogram("metrics.batch.series",
			metric.WithDescription("Unique series processed per batch"),
		)
	})
	defer func() {
		batchRowsAdded.Add(ctx, batch.rowsAdded)
		duration := time.Since(start)
		batchDuration.Record(ctx, duration.Seconds())
		if err != nil {
			batchFailure.Add(ctx, 1)
		}
		batch.Range(func(field constants.ID, mapping map[uint64]*roaring64.Bitmap) error {
			switch field {
			case constants.MetricsFST:
				for k, v := range mapping {
					batchLabels.Record(ctx, int64(v.GetCardinality()), metric.WithAttributes(
						attribute.Int64("shard", int64(k)),
					))
				}
			case constants.MetricsSeries:
				for k, v := range mapping {
					batchSeries.Record(ctx, int64(v.GetCardinality()), metric.WithAttributes(
						attribute.Int64("shard", int64(k)),
					))
				}
			}
			return nil
		})
	}()
	conv := prometheusremotewrite.NewPrometheusConverter()
	err = conv.FromMetrics(mets, prometheusremotewrite.Settings{
		AddMetricSuffixes: true,
	})
	if err != nil {
		return err
	}
	meta := prometheusremotewrite.OtelMetricsToMetadata(mets, true)

	// wrap everything in a single transaction
	err = store.DB.Update(func(txn *badger.Txn) error {
		blob := blob.Upsert(txn, store.Seq, store.Cache)
		label := UpsertLabels(blob)
		series := conv.TimeSeries()
		for i := range series {
			batch.Append(&series[i], label, blob, store.Seq)
		}
		return StoreMetadata(txn, meta)
	})
	if err != nil {
		return err
	}
	view := quantum.ViewByTimeUnit("", ts, 'D')
	return batch.Apply(store, view)
}
