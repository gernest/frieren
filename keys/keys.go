package keys

import "github.com/gernest/ernestdb/util"

type Exists struct {
	ShardID uint64
}

func (e *Exists) Key() []byte {
	return util.Uint64ToBytes([]uint64{0, e.ShardID})
}

type Timestamp struct {
	ShardID  uint64
	SeriesID uint64
}

func (e *Timestamp) Key() []byte {
	return util.Uint64ToBytes([]uint64{1, e.ShardID, e.SeriesID})
}

type Value struct {
	ShardID  uint64
	SeriesID uint64
}

func (e *Value) Key() []byte {
	return util.Uint64ToBytes([]uint64{2, e.ShardID, e.SeriesID})
}

type Series struct {
	ShardID  uint64
	SeriesID uint64
}

func (e *Series) Key() []byte {
	return util.Uint64ToBytes([]uint64{3, e.ShardID, e.SeriesID})
}

type Labels struct {
	ShardID uint64
	LabelID uint64
}

func (e *Labels) Key() []byte {
	return util.Uint64ToBytes([]uint64{4, e.ShardID, e.LabelID})
}

type Blob struct {
	BlobID uint64
}

func (e *Blob) Key() []byte {
	return util.Uint64ToBytes([]uint64{5, e.BlobID})
}

type Kind struct {
	ShardID uint64
}

func (e *Kind) Key() []byte {
	return util.Uint64ToBytes([]uint64{6, e.ShardID})
}

type Histogram struct {
	ShardID  uint64
	SeriesID uint64
}

func (e *Histogram) Key() []byte {
	return util.Uint64ToBytes([]uint64{7, e.ShardID, e.SeriesID})
}
