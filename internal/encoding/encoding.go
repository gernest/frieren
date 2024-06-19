package encoding

import (
	"github.com/gernest/frieren/internal/util"
	"google.golang.org/protobuf/encoding/protowire"
)

// Translation keys account to majority of data stored. We rely on protobuf
// varint encoding to save some bytes.

func Uint64Bytes(v uint64) []byte {
	size := protowire.SizeVarint(v)
	b := make([]byte, size)
	return protowire.AppendVarint(b[:0], v)
}

func Uint64(b []byte) uint64 {
	v, size := protowire.ConsumeVarint(b)
	if size == -1 {
		util.Exit("truncated uint64 value")
	}
	return v

}
