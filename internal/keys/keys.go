package keys

import (
	"bytes"
	"fmt"
)

const (
	seq uint64 = iota
	blobSeq
	blobID
	blobHash
	fstBitmap
	fst
	metadata
	fieldView
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

type BlobHash struct {
	FieldID uint64
	Hash    uint64
}

func (e *BlobHash) Slice() []uint64 {
	return []uint64{blobHash, e.FieldID, e.Hash}
}

func (e *BlobHash) Key() []byte {
	return Encode(nil, e.Slice())
}

type FSTBitmap struct {
	ShardID uint64
	FieldID uint64
}

func (e *FSTBitmap) Slice() []uint64 {
	return []uint64{fstBitmap, e.FieldID, e.ShardID}
}

func (e *FSTBitmap) Key() []byte {
	return Encode(nil, e.Slice())
}

type FST struct {
	ShardID uint64
	FieldID uint64
}

func (e *FST) Key() []byte {
	return Encode(nil, []uint64{fst, e.FieldID, e.ShardID})
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

type FieldView struct{}

func (e *FieldView) Slice() []uint64 {
	return []uint64{fieldView}
}

func (e *FieldView) Key() []byte {
	return Encode(nil, e.Slice())
}

func Encode(b *bytes.Buffer, value []uint64) []byte {
	if b == nil {
		b = new(bytes.Buffer)
	} else {
		b.Reset()
	}
	for i := range value {
		if i != 0 {
			b.WriteByte(':')
		}
		fmt.Fprint(b, value[i])
	}
	return b.Bytes()
}
