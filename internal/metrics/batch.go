package metrics

import (
	"crypto/sha512"
	"math"
	"math/bits"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/ro"
	"github.com/gernest/frieren/internal/shardwidth"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/frieren/internal/util"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"
	"go.opentelemetry.io/collector/pdata/pmetric"
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
	bitDepth  map[constants.ID]int
}

func NewBatch() *Batch {
	return &Batch{
		values:    make(map[uint64]*roaring64.Bitmap),
		histogram: make(map[uint64]*roaring64.Bitmap),
		timestamp: make(map[uint64]*roaring64.Bitmap),
		series:    make(map[uint64]*roaring64.Bitmap),
		labels:    make(map[uint64]*roaring64.Bitmap),
		exemplars: make(map[uint64]*roaring64.Bitmap),
		fst:       make(map[uint64]*roaring64.Bitmap),
		bitDepth:  make(map[constants.ID]int),
	}
}

func (b *Batch) Reset() *Batch {
	clear(b.values)
	clear(b.histogram)
	clear(b.timestamp)
	clear(b.series)
	clear(b.labels)
	clear(b.exemplars)
	clear(b.fst)
	b.shards.Clear()
	return b
}

type LabelFunc func([]prompb.Label) []uint64

func (b *Batch) Append(ts *prompb.TimeSeries, labelFunc LabelFunc, blobFunc blob.Func, seq *store.Seq) {
	labels := labelFunc(ts.Labels)
	checksum := sha512.Sum512(util.Uint64ToBytes(labels))
	series := blobFunc(constants.MetricsSeries, checksum[:])

	b.depth(constants.MetricsSeries, bits.Len64(series))

	currentShard := ^uint64(0)
	var exemplars uint64
	if len(ts.Exemplars) > 0 {
		data, _ := EncodeExemplars(ts.Exemplars)
		exemplars = blobFunc(constants.MetricsExemplars, data)
	}
	for _, s := range ts.Samples {
		id := seq.NextID(constants.MetricsRow)
		shard := id / shardwidth.ShardWidth
		if shard != currentShard {
			currentShard = shard
			b.shards.Add(shard)
		}
		ro.BSI(bitmap(shard, b.series), id, series)
		ro.BSI(bitmap(shard, b.timestamp), id, uint64(s.Timestamp))
		value := math.Float64bits(s.Value)
		ro.BSI(bitmap(shard, b.values), id, value)
		ro.BSISet(bitmap(shard, b.labels), id, labels)

		b.depth(constants.MetricsTimestamp, bits.Len64(uint64(s.Timestamp)))
		b.depth(constants.MetricsValue, bits.Len64(value))

		if len(ts.Exemplars) > 0 {
			ro.BSI(bitmap(shard, b.exemplars), id, exemplars)
			b.depth(constants.MetricsExemplars, bits.Len64(exemplars))
		}
	}
	currentShard = ^uint64(0)

	for j := range ts.Histograms {
		s := &ts.Histograms[j]
		id := seq.NextID(constants.MetricsRow)
		shard := id / shardwidth.ShardWidth
		if shard != currentShard {
			currentShard = shard
			b.shards.Add(shard)
		}
		data, _ := s.Marshal()
		value := blobFunc(constants.MetricsHistogram, data)
		ro.BSI(bitmap(shard, b.series), id, series)
		ro.BSI(bitmap(shard, b.timestamp), id, uint64(s.Timestamp))
		ro.BSI(bitmap(shard, b.values), id, value)
		ro.BSISet(bitmap(shard, b.labels), id, labels)
		ro.Bool(bitmap(shard, b.histogram), id, true)

		b.depth(constants.MetricsValue, bits.Len64(value))
		b.depth(constants.MetricsTimestamp, bits.Len64(uint64(s.Timestamp)))

		if len(ts.Exemplars) > 0 {
			ro.BSI(bitmap(shard, b.exemplars), id, exemplars)
			b.depth(constants.MetricsExemplars, bits.Len64(exemplars))
		}
	}
}

func (b *Batch) depth(field constants.ID, depth int) {
	b.bitDepth[field] = max(b.bitDepth[field], depth)
}

func bitmap(u uint64, m map[uint64]*roaring64.Bitmap) *roaring64.Bitmap {
	b, ok := m[u]
	if !ok {
		b = roaring64.New()
		m[u] = b
	}
	return b
}

func AppendBatch(store *store.Store, batch *Batch, mets pmetric.Metrics, ts time.Time) error {
	conv := prometheusremotewrite.NewPrometheusConverter()
	err := conv.FromMetrics(mets, prometheusremotewrite.Settings{
		AddMetricSuffixes: true,
	})
	if err != nil {
		return err
	}
	meta := prometheusremotewrite.OtelMetricsToMetadata(mets, true)
	return store.DB.Update(func(txn *badger.Txn) error {
		blob := blob.Upsert(txn, store.Seq)
		label := UpsertLabels(blob)
		series := conv.TimeSeries()
		for i := range series {
			batch.Append(&series[i], label, blob, store.Seq)
		}
		err := Save(store, batch, ts)
		if err != nil {
			return err
		}
		return StoreMetadata(txn, meta)
	})
}
