package metrics

import (
	"context"
	"crypto/sha512"
	"math"
	"math/bits"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/ro"
	"github.com/gernest/frieren/internal/self"
	"github.com/gernest/frieren/internal/shardwidth"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/frieren/internal/util"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type Batch struct {
	values    map[uint64]*roaring64.Bitmap
	histogram map[uint64]*roaring64.Bitmap
	timestamp map[uint64]*roaring64.Bitmap
	series    map[uint64]*roaring64.Bitmap
	labels    map[uint64]*roaring64.Bitmap
	exemplars map[uint64]*roaring64.Bitmap
	fst       map[uint64]*roaring64.Bitmap
	shards    roaring64.Bitmap
	bitDepth  map[uint64]map[uint64]uint64

	rowsAdded int64
}

func NewBatch() *Batch {
	return batchPool.Get().(*Batch)
}

func newBatch() *Batch {
	return &Batch{
		values:    make(map[uint64]*roaring64.Bitmap),
		histogram: make(map[uint64]*roaring64.Bitmap),
		timestamp: make(map[uint64]*roaring64.Bitmap),
		series:    make(map[uint64]*roaring64.Bitmap),
		labels:    make(map[uint64]*roaring64.Bitmap),
		exemplars: make(map[uint64]*roaring64.Bitmap),
		fst:       make(map[uint64]*roaring64.Bitmap),
		bitDepth:  make(map[uint64]map[uint64]uint64),
	}
}

var batchPool = &sync.Pool{New: func() any { return newBatch() }}

func (b *Batch) Release() {
	b.Reset()
	batchPool.Put(b)
}

func (b *Batch) Reset() {
	clear(b.values)
	clear(b.histogram)
	clear(b.timestamp)
	clear(b.series)
	clear(b.labels)
	clear(b.exemplars)
	clear(b.fst)
	clear(b.bitDepth)
	b.shards.Clear()
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
			b.shards.Add(shard)
			bitmap(shard, b.fst).AddMany(labels)
			b.depth(constants.MetricsSeries, shard, bits.Len64(series))
			b.depth(constants.MetricsExemplars, shard, bits.Len64(exemplars))
		}
		ro.BSI(bitmap(shard, b.series), id, series)
		ro.BSI(bitmap(shard, b.timestamp), id, uint64(s.Timestamp))
		value := math.Float64bits(s.Value)
		ro.BSI(bitmap(shard, b.values), id, value)
		ro.BSISet(bitmap(shard, b.labels), id, labels)

		b.depth(constants.MetricsTimestamp, shard, bits.Len64(uint64(s.Timestamp)))
		b.depth(constants.MetricsValue, shard, bits.Len64(value))

		if len(ts.Exemplars) > 0 {
			ro.BSI(bitmap(shard, b.exemplars), id, exemplars)
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
			b.shards.Add(shard)
			bitmap(shard, b.fst).AddMany(labels)
			b.depth(constants.MetricsSeries, shard, bits.Len64(series))
			if len(ts.Exemplars) > 0 {
				b.depth(constants.MetricsExemplars, shard, bits.Len64(exemplars))
			}
		}
		data, _ := s.Marshal()
		value := blobFunc(constants.MetricsHistogram, data)
		ro.BSI(bitmap(shard, b.series), id, series)
		ro.BSI(bitmap(shard, b.timestamp), id, uint64(s.Timestamp))
		ro.BSI(bitmap(shard, b.values), id, value)
		ro.BSISet(bitmap(shard, b.labels), id, labels)
		ro.Bool(bitmap(shard, b.histogram), id, true)

		b.depth(constants.MetricsValue, shard, bits.Len64(value))
		b.depth(constants.MetricsTimestamp, shard, bits.Len64(uint64(s.Timestamp)))

		if len(ts.Exemplars) > 0 {
			ro.BSI(bitmap(shard, b.exemplars), id, exemplars)
		}
	}
}

func (b *Batch) depth(field constants.ID, shard uint64, depth int) {
	m, ok := b.bitDepth[shard]
	if !ok {
		m = make(map[uint64]uint64)
		b.bitDepth[shard] = m
	}
	m[uint64(field)] = max(m[uint64(field)], uint64(depth))
}

func bitmap(u uint64, m map[uint64]*roaring64.Bitmap) *roaring64.Bitmap {
	b, ok := m[u]
	if !ok {
		b = roaring64.New()
		m[u] = b
	}
	return b
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
		for k, v := range batch.fst {
			batchLabels.Record(ctx, int64(v.GetCardinality()), metric.WithAttributes(
				attribute.Int64("shard", int64(k)),
			))
		}
		for k, v := range batch.series {
			batchSeries.Record(ctx, int64(v.GetCardinality()), metric.WithAttributes(
				attribute.Int64("shard", int64(k)),
			))
		}
	}()
	conv := prometheusremotewrite.NewPrometheusConverter()
	err = conv.FromMetrics(mets, prometheusremotewrite.Settings{
		AddMetricSuffixes: true,
	})
	if err != nil {
		return err
	}
	meta := prometheusremotewrite.OtelMetricsToMetadata(mets, true)
	err = store.DB.Update(func(txn *badger.Txn) error {
		blob := blob.Upsert(txn, store.Seq)
		label := UpsertLabels(blob)
		series := conv.TimeSeries()
		for i := range series {
			batch.Append(&series[i], label, blob, store.Seq)
		}
		err := Save(ctx, store, batch, ts)
		if err != nil {
			return err
		}
		return StoreMetadata(txn, meta)
	})
	return
}
