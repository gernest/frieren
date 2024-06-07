package metrics

import (
	"math"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/ro"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/frieren/shardwidth"
	"github.com/gernest/frieren/util"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type ID interface {
	NextID() uint64
}

type Batch struct {
	values    map[uint64]*roaring64.Bitmap
	kind      map[uint64]*roaring64.Bitmap
	timestamp map[uint64]*roaring64.Bitmap
	series    map[uint64]*roaring64.Bitmap
	labels    map[uint64]*roaring64.Bitmap
	exemplars map[uint64]*roaring64.Bitmap
	exists    map[uint64]*roaring64.Bitmap

	fst map[uint64]*roaring64.Bitmap

	shards roaring64.Bitmap
}

func NewBatch() *Batch {
	return &Batch{
		values:    make(map[uint64]*roaring64.Bitmap),
		kind:      make(map[uint64]*roaring64.Bitmap),
		timestamp: make(map[uint64]*roaring64.Bitmap),
		series:    make(map[uint64]*roaring64.Bitmap),
		labels:    make(map[uint64]*roaring64.Bitmap),
		exemplars: make(map[uint64]*roaring64.Bitmap),
		exists:    make(map[uint64]*roaring64.Bitmap),
		fst:       make(map[uint64]*roaring64.Bitmap),
	}
}

func (b *Batch) Reset() *Batch {
	clear(b.values)
	clear(b.kind)
	clear(b.timestamp)
	clear(b.series)
	clear(b.labels)
	clear(b.exemplars)
	clear(b.exists)
	clear(b.fst)
	b.shards.Clear()
	return b
}

type LabelFunc func([]prompb.Label) []uint64

func (b *Batch) Append(ts *prompb.TimeSeries, labelFunc LabelFunc, blobFunc blob.Func, id ID) {
	labels := labelFunc(ts.Labels)
	series := xxhash.Sum64(util.Uint64ToBytes(labels))
	currentShard := ^uint64(0)
	var exemplars uint64
	if len(ts.Exemplars) > 0 {
		data, _ := EncodeExemplars(ts.Exemplars)
		exemplars = blobFunc(data)
	}
	for _, s := range ts.Samples {
		id := id.NextID()
		shard := id / shardwidth.ShardWidth
		if shard != currentShard {
			currentShard = shard
			b.shards.Add(shard % shardwidth.ShardWidth)
		}
		ro.SetBSI(bitmap(shard, b.series), id, series)
		ro.SetBSI(bitmap(shard, b.timestamp), id, uint64(s.Timestamp))
		ro.SetBSI(bitmap(shard, b.values), id, math.Float64bits(s.Value))
		ro.SetBSISet(bitmap(shard, b.series), id, labels)
		bitmap(shard, b.exists).Add(id % shardwidth.ShardWidth)
		if exemplars != 0 {
			ro.SetBSI(bitmap(shard, b.exemplars), id, exemplars)
		}
	}
	currentShard = ^uint64(0)

	for j := range ts.Histograms {
		s := &ts.Histograms[j]
		id := id.NextID()
		shard := id / shardwidth.ShardWidth
		if shard != currentShard {
			currentShard = shard
			b.shards.Add(shard % shardwidth.ShardWidth)
		}
		data, _ := s.Marshal()
		value := blobFunc(data)
		ro.SetBSI(bitmap(shard, b.series), id, series)
		ro.SetBSI(bitmap(shard, b.timestamp), id, uint64(s.Timestamp))
		ro.SetBSI(bitmap(shard, b.values), id, value)
		ro.SetBSISet(bitmap(shard, b.series), id, labels)
		ro.SetBBool(bitmap(shard, b.kind), id, false)
		bitmap(shard, b.exists).Add(id % shardwidth.ShardWidth)
		if exemplars != 0 {
			ro.SetBSI(bitmap(shard, b.exemplars), id, exemplars)
		}
	}
}

func bitmap(u uint64, m map[uint64]*roaring64.Bitmap) *roaring64.Bitmap {
	b, ok := m[u]
	if !ok {
		b = roaring64.New()
		m[u] = b
	}
	return b
}

func AppendBatch(store *store.Store, batch *Batch, mets pmetric.Metrics) error {
	ts, err := prometheusremotewrite.FromMetrics(mets, prometheusremotewrite.Settings{
		DisableTargetInfo: true,
	})
	if err != nil {
		return err
	}
	return store.DB.Update(func(txn *badger.Txn) error {
		blob := blob.Upsert(txn)
		label := UpsertLabels(blob)
		for _, series := range ts {
			batch.Append(series, label, blob, store.Seq)
		}
		return nil
	})
}
