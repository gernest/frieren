package ernestdb

import (
	"math"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/cespare/xxhash/v2"
	"github.com/gernest/ernestdb/shardwidth"
	"github.com/gernest/ernestdb/util"
	"github.com/prometheus/prometheus/prompb"
)

type Batch struct {
	exists    map[uint64]*roaring64.BSI
	values    map[uint64]map[uint64]*roaring64.BSI
	histogram map[uint64]map[uint64]*roaring64.BSI
	kind      map[uint64]*roaring64.BSI
	timestamp map[uint64]map[uint64]*roaring64.BSI
	series    map[uint64]map[uint64]*roaring64.Bitmap
	labels    map[uint64]map[uint64]*roaring64.Bitmap
}

func NewBatch() *Batch {
	return &Batch{
		exists:    make(map[uint64]*roaring64.BSI),
		values:    make(map[uint64]map[uint64]*roaring64.BSI),
		histogram: make(map[uint64]map[uint64]*roaring64.BSI),
		kind:      make(map[uint64]*roaring64.BSI),
		timestamp: make(map[uint64]map[uint64]*roaring64.BSI),
		series:    make(map[uint64]map[uint64]*roaring64.Bitmap),
		labels:    make(map[uint64]map[uint64]*roaring64.Bitmap),
	}
}

type LabelFunc func([]prompb.Label) []uint64
type BlobFunc func([]byte) uint64

func (b *Batch) Append(ts *prompb.TimeSeries, f LabelFunc, blob BlobFunc) {
	labels := f(ts.Labels)
	series := xxhash.Sum64(util.Uint64ToBytes(labels))

	currentShard := ^uint64(0)
	for _, s := range ts.Samples {
		id := uint64(s.Timestamp - epochMs)
		shard := id / shardwidth.ShardWidth
		if shard != currentShard {
			lb(shard, series, b.series).AddMany(labels)
			for i := range labels {
				lb(shard, labels[i], b.labels).Add(series)
			}
			currentShard = shard
		}
		get(shard, b.exists).SetValue(id, int64(series))
		sx(shard, series, b.values).SetValue(id, int64(math.Float64bits(s.Value)))
		sx(shard, series, b.timestamp).SetValue(id, s.Timestamp)

	}
	currentShard = ^uint64(0)

	for j := range ts.Histograms {
		s := &ts.Histograms[j]
		id := uint64(s.Timestamp - epochMs)
		shard := id / shardwidth.ShardWidth
		if shard != currentShard {
			lb(shard, series, b.series).AddMany(labels)
			get(shard, b.kind).SetValue(id, 1)
			for i := range labels {
				lb(shard, labels[i], b.labels).Add(series)
			}
			currentShard = shard
		}

		get(shard, b.exists).SetValue(id, int64(series))
		sx(shard, series, b.timestamp).SetValue(id, s.Timestamp)

		data, _ := s.Marshal()
		value := blob(data)
		sx(shard, series, b.values).SetValue(id, int64(value))

		lb(shard, series, b.series).AddMany(labels)
		for i := range labels {
			lb(shard, labels[i], b.labels).Add(series)
		}
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
		y = roaring64.NewDefaultBSI()
		x[b] = y
	}
	return y
}

func get(u uint64, m map[uint64]*roaring64.BSI) *roaring64.BSI {
	b, ok := m[u]
	if !ok {
		b = roaring64.NewDefaultBSI()
		m[u] = b
	}
	return b
}
