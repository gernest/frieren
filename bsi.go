package ernestdb

import (
	"math"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/cespare/xxhash/v2"
	"github.com/gernest/ernestdb/shardwidth"
	"github.com/gernest/ernestdb/util"
)

type Batch struct {
	exists    map[uint64]*roaring64.BSI
	values    map[uint64]map[uint64]*roaring64.BSI
	timestamp map[uint64]map[uint64]*roaring64.BSI
	series    map[uint64]map[uint64]*roaring64.Bitmap
	labels    map[uint64]map[uint64]*roaring64.Bitmap
}

func NewBatch() *Batch {
	return &Batch{
		exists:    make(map[uint64]*roaring64.BSI),
		values:    make(map[uint64]map[uint64]*roaring64.BSI),
		timestamp: make(map[uint64]map[uint64]*roaring64.BSI),
		series:    make(map[uint64]map[uint64]*roaring64.Bitmap),
		labels:    make(map[uint64]map[uint64]*roaring64.Bitmap),
	}
}

func (b *Batch) Add(tsNano uint64, value float64, labels []uint64) {
	// adjust timestamp to iD
	id := tsNano - Epoch()
	shard := id / shardwidth.ShardWidth
	series := xxhash.Sum64(util.Uint64ToBytes(labels))

	get(shard, b.exists).SetValue(id, int64(series))
	sx(shard, series, b.values).SetValue(id, int64(math.Float64bits(value)))
	sx(shard, series, b.timestamp).SetValue(id, int64(tsNano))
	lb(shard, series, b.series).AddMany(labels)
	for i := range labels {
		lb(shard, labels[i], b.labels).Add(series)
	}
}

func lb(a, b uint64, m map[uint64]map[uint64]*roaring64.Bitmap) *roaring64.Bitmap {
	x, ok := m[a]
	if !ok {
		x = make(map[uint64]*roaring64.Bitmap)
		m[a] = x
	}
	y, ok := x[b]
	if !ok {
		y = roaring64.New()
		x[b] = y
	}
	return y
}

func sx(a, b uint64, m map[uint64]map[uint64]*roaring64.BSI) *roaring64.BSI {
	x, ok := m[a]
	if !ok {
		x = make(map[uint64]*roaring64.BSI)
		m[a] = x
	}
	y, ok := x[b]
	if !ok {
		y = roaring64.NewBSI(roaring64.Min64BitSigned, roaring64.Max64BitSigned)
		x[b] = y
	}
	return y
}

func get(u uint64, m map[uint64]*roaring64.BSI) *roaring64.BSI {
	b, ok := m[u]
	if !ok {
		b = roaring64.NewBSI(roaring64.Min64BitSigned, roaring64.Max64BitSigned)
		m[u] = b
	}
	return b
}
