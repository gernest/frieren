package keys

import (
	"encoding/binary"
	"slices"
)

type Exists struct {
	ShardID uint64
}

func (e *Exists) Key() []byte {
	return Encode(nil, []uint64{0, e.ShardID})
}

type Timestamp struct {
	ShardID  uint64
	SeriesID uint64
}

func (e *Timestamp) Slice() []uint64 {
	return []uint64{1, e.ShardID, e.SeriesID}
}

func (e *Timestamp) Key() []byte {
	return Encode(nil, []uint64{1, e.ShardID, e.SeriesID})
}

type Value struct {
	ShardID  uint64
	SeriesID uint64
}

func (e *Value) Slice() []uint64 {
	return []uint64{2, e.ShardID, e.SeriesID}
}

func (e *Value) Key() []byte {
	return Encode(nil, e.Slice())
}

type Series struct {
	ShardID  uint64
	SeriesID uint64
}

func (e *Series) Slice() []uint64 {
	return []uint64{2, e.ShardID, e.SeriesID}
}

func (e *Series) Key() []byte {
	return Encode(nil, e.Slice())
}

type Labels struct {
	ShardID uint64
	LabelID uint64
}

func (e *Labels) Slice() []uint64 {
	return []uint64{4, e.ShardID, e.LabelID}
}

func (e *Labels) Key() []byte {
	return Encode(nil, e.Slice())
}

type Blob struct {
	BlobID uint64
}

func (e *Blob) Slice() []uint64 {
	return []uint64{5, e.BlobID}
}

func (e *Blob) Key() []byte {
	return Encode(nil, e.Slice())
}

type Kind struct {
	ShardID uint64
}

func (e *Kind) Key() []byte {
	return Encode(nil, []uint64{6, e.ShardID})
}

type Histogram struct {
	ShardID  uint64
	SeriesID uint64
}

func (e *Histogram) Slice() []uint64 {
	return []uint64{7, e.ShardID, e.SeriesID}
}

func (e *Histogram) Key() []byte {
	return Encode(nil, e.Slice())
}

type FSTBitmap struct {
	ShardID uint64
}

func (e *FSTBitmap) Key() []byte {
	return Encode(nil, []uint64{8, e.ShardID})
}

type FST struct {
	ShardID uint64
}

func (e *FST) Key() []byte {
	return Encode(nil, []uint64{9, e.ShardID})
}

type Shards struct{}

func (Shards) Slice() []uint64 {
	return []uint64{10}
}

func (e Shards) Key() []byte {
	return Encode(nil, e.Slice())
}

type Metadata struct {
	MetricID uint64
}

func (e *Metadata) Slice() []uint64 {
	return []uint64{11, e.MetricID}
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
