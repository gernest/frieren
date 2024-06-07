package keys

import (
	"encoding/binary"
	"slices"
)

const (
	seq uint64 = iota + (1 << 10)
	blob
	fstBitmap
	fst
	metadata
)

type Seq struct{}

func (e Seq) Slice() []uint64 {
	return []uint64{seq}
}

func (e Seq) Key() []byte {
	return Encode(nil, e.Slice())
}

type Blob struct {
	BlobID uint64
}

func (e *Blob) Slice() []uint64 {
	return []uint64{blob, e.BlobID}
}

func (e *Blob) Key() []byte {
	return Encode(nil, e.Slice())
}

type FSTBitmap struct {
	ShardID uint64
}

func (e *FSTBitmap) Slice() []uint64 {
	return []uint64{fstBitmap, e.ShardID}
}

func (e *FSTBitmap) Key() []byte {
	return Encode(nil, e.Slice())
}

type FST struct {
	ShardID uint64
}

func (e *FST) Key() []byte {
	return Encode(nil, []uint64{fst, e.ShardID})
}

type Metadata struct {
	MetricID uint64
}

func (e *Metadata) Slice() []uint64 {
	return []uint64{metadata, e.MetricID}
}

func (e *Metadata) Key() []byte {
	return Encode(nil, e.Slice())
}

func Encode(b []byte, value []uint64) []byte {
	if b == nil {
		b = make([]byte, 0, len(value)*8)
	} else {
		b = slices.Grow(b[:0], len(value)*8)
	}
	for i := range value {
		b = binary.BigEndian.AppendUint64(b, value[i])
	}
	return b
}
