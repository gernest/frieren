package keys

import "github.com/gernest/ernestdb/util"

type Exists struct {
	ShardID uint64
}

func (e *Exists) Key() []byte {
	return util.Uint64ToBytes([]uint64{e.ShardID})
}

type Timestamp struct {
	ShardID  uint64
	SeriesID uint64
}

func (e *Timestamp) Key() []byte {
	return util.Uint64ToBytes([]uint64{e.ShardID, e.SeriesID})
}

type Value struct {
	ShardID  uint64
	SeriesID uint64
}

func (e *Value) Key() []byte {
	return util.Uint64ToBytes([]uint64{e.ShardID, e.SeriesID})
}

type Series struct {
	ShardID  uint64
	SeriesID uint64
}

func (e *Series) Key() []byte {
	return util.Uint64ToBytes([]uint64{e.ShardID, e.SeriesID})
}

type Labels struct {
	ShardID uint64
	LabelID uint64
}

func (e *Labels) Key() []byte {
	return util.Uint64ToBytes([]uint64{e.ShardID, e.LabelID})
}
