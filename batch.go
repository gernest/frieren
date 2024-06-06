package ernestdb

import (
	"math"
	"math/bits"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/cespare/xxhash/v2"
	"github.com/gernest/ernestdb/shardwidth"
	"github.com/gernest/ernestdb/util"
	"github.com/prometheus/prometheus/prompb"
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

	fst map[uint64]*roaring64.Bitmap

	blobFunc  BlobFunc
	labelFunc LabelFunc
	id        ID

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
		fst:       make(map[uint64]*roaring64.Bitmap),
	}
}

func (b *Batch) Reset(blob BlobFunc, label LabelFunc, id ID) *Batch {
	b.blobFunc = blob
	b.labelFunc = label
	clear(b.values)
	clear(b.kind)
	clear(b.timestamp)
	clear(b.series)
	clear(b.labels)
	clear(b.exemplars)
	clear(b.fst)
	b.id = id
	b.shards.Clear()
	return b
}

type LabelFunc func([]prompb.Label) []uint64
type BlobFunc func([]byte) uint64

func (b *Batch) Append(ts *prompb.TimeSeries) {
	labels := b.labelFunc(ts.Labels)
	series := xxhash.Sum64(util.Uint64ToBytes(labels))
	currentShard := ^uint64(0)
	var exemplars uint64
	if len(ts.Exemplars) > 0 {
		data, _ := EncodeExemplars(ts.Exemplars)
		exemplars = b.blobFunc(data)
	}
	for _, s := range ts.Samples {
		id := b.id.NextID()
		shard := id / shardwidth.ShardWidth
		if shard != currentShard {
			currentShard = shard
			b.shards.Add(shard)
		}
		SetBSI(bitmap(shard, b.series), id, series)
		SetBSI(bitmap(shard, b.timestamp), id, uint64(s.Timestamp))
		SetBSI(bitmap(shard, b.values), id, math.Float64bits(s.Value))
		SetBSISet(bitmap(shard, b.series), id, labels)
		SetMutex(bitmap(shard, b.kind), id, uint64(metricsFloat))
		if exemplars != 0 {
			SetBSI(bitmap(shard, b.exemplars), id, exemplars)
		}
	}
	currentShard = ^uint64(0)

	for j := range ts.Histograms {
		s := &ts.Histograms[j]
		id := b.id.NextID()
		shard := id / shardwidth.ShardWidth
		if shard != currentShard {
			currentShard = shard
			b.shards.Add(shard)
		}
		data, _ := s.Marshal()
		value := b.blobFunc(data)
		SetBSI(bitmap(shard, b.series), id, series)
		SetBSI(bitmap(shard, b.timestamp), id, uint64(s.Timestamp))
		SetBSI(bitmap(shard, b.values), id, value)
		SetBSISet(bitmap(shard, b.series), id, labels)
		SetMutex(bitmap(shard, b.kind), id, uint64(metricsFloat))
		if exemplars != 0 {
			SetBSI(bitmap(shard, b.exemplars), id, exemplars)
		}
	}
}

func SetMutex(m *roaring64.Bitmap, id uint64, value uint64) {
	m.Add(value*shardwidth.ShardWidth + (id % shardwidth.ShardWidth))
}

func SetBSI(m *roaring64.Bitmap, id uint64, value uint64) {
	fragmentColumn := id % shardwidth.ShardWidth
	m.Add(fragmentColumn)
	lz := bits.LeadingZeros64(value)
	row := uint64(2)
	for mask := uint64(0x1); mask <= 1<<(64-lz) && mask != 0; mask = mask << 1 {
		if value&mask > 0 {
			m.Add(row*shardwidth.ShardWidth + fragmentColumn)
		}
		row++
	}
}

func SetBSISet(m *roaring64.Bitmap, id uint64, values []uint64) {
	for _, row := range values {
		m.Add(row*shardwidth.ShardWidth + (id % shardwidth.ShardWidth))
	}
}

func SetBBool(m *roaring64.Bitmap, id uint64, value bool) {
	fragmentColumn := id % shardwidth.ShardWidth
	if value {
		m.Add(trueRowOffset + fragmentColumn)
	} else {
		m.Add(falseRowOffset + fragmentColumn)
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
