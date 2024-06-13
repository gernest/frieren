package keys

import (
	"encoding/binary"
	"slices"
)

const (
	seq uint64 = iota + (1 << 10)
	blobSeq
	blobID
	blobKey
	fstBitmap
	fst
	metadata
	depth
)

type Seq struct {
	ID uint64
}

func (e *Seq) Slice() []uint64 {
	return []uint64{seq, e.ID}
}

func (e Seq) Key() []byte {
	return Encode(nil, e.Slice())
}

type BlobID struct {
	FieldID uint64
	Seq     uint64
}

func (e *BlobID) Slice() []uint64 {
	return []uint64{blobID, e.FieldID, e.Seq}
}

func (e *BlobID) Key() []byte {
	return Encode(nil, e.Slice())
}

type BlobKey struct {
	FieldID uint64
	Hash    uint64
}

func (e *BlobKey) Slice() []uint64 {
	return []uint64{blobKey, e.FieldID, e.Hash}
}

func (e *BlobKey) Key() []byte {
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

type BitDepth struct {
	ShardID uint64
}

func (e *BitDepth) Slice() []uint64 {
	return []uint64{depth, e.ShardID}
}

func (e *BitDepth) Key() []byte {
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
