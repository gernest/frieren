package ro

import (
	"math/bits"

	"github.com/gernest/frieren/internal/shardwidth"
	"github.com/gernest/frieren/internal/util"
	"github.com/gernest/roaring"
)

const (
	falseRowOffset = 0 * shardwidth.ShardWidth // fragment row 0
	trueRowOffset  = 1 * shardwidth.ShardWidth // fragment row 1
)

func Mutex(m *roaring.Bitmap, id uint64, value uint64) {
	m.Add(value*shardwidth.ShardWidth + (id % shardwidth.ShardWidth))
}

func BSI(m *roaring.Bitmap, id uint64, value uint64) {
	fragmentColumn := id % shardwidth.ShardWidth
	m.Add(fragmentColumn)
	lz := bits.LeadingZeros64(value)
	if lz == 0 {
		util.Exit("illegal negative value")
	}
	row := uint64(2)
	for mask := uint64(0x1); mask <= 1<<(64-lz) && mask != 0; mask = mask << 1 {
		if value&mask > 0 {
			m.Add(row*shardwidth.ShardWidth + fragmentColumn)
		}
		row++
	}
}

func Set(m, exists *roaring.Bitmap, id uint64, values []uint64) {
	frag := (id % shardwidth.ShardWidth)
	exists.Add(frag)
	for _, row := range values {
		m.Add(row*shardwidth.ShardWidth + frag)
	}
}

func SetBitmap(m, exists *roaring.Bitmap, id uint64, values *roaring.Bitmap) {
	frag := (id % shardwidth.ShardWidth)
	exists.Add(frag)
	itr := values.Iterator()
	for next, eof := itr.Next(); !eof; next, eof = itr.Next() {
		m.Add(next*shardwidth.ShardWidth + frag)
	}
}

func Bool(m *roaring.Bitmap, id uint64, value bool) {
	fragmentColumn := id % shardwidth.ShardWidth
	if value {
		m.Add(trueRowOffset + fragmentColumn)
	} else {
		m.Add(falseRowOffset + fragmentColumn)
	}
}
