package ro

import (
	"math"
	"math/bits"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/frieren/shardwidth"
)

const (
	// Row ids used for boolean fields.
	falseRowID = uint64(0)
	trueRowID  = uint64(1)

	// BSI bits used to check existence & sign.
	bsiExistsBit = 0
	bsiSignBit   = 1
	bsiOffsetBit = 2

	falseRowOffset = 0 * shardwidth.ShardWidth // fragment row 0
	trueRowOffset  = 1 * shardwidth.ShardWidth // fragment row 1
)

var bitDepth = uint64(bits.Len64(math.MaxUint64))

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
