package fields

import (
	"math"
	"math/bits"

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
