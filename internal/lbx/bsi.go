package lbx

import (
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/cursor"
	"github.com/gernest/rows"
)

type Data map[uint64]uint64

func NewData(columns []uint64) Data {
	d := make(Data)
	d.mergeBits(columns, 0)
	return d
}

func (d Data) Clone() Data {
	o := make(Data, len(d))
	for k, v := range d {
		o[k] = v
	}
	return o
}

func (d Data) mergeBits(bits []uint64, mask uint64) {
	for _, v := range bits {
		d[v] |= mask
	}
}

func BSI(base Data, c *rbf.Cursor, exists *rows.Row, shard uint64, f func(column uint64, value int64)) error {
	data := base.Clone()
	bitDepth, err := depth(c)
	if err != nil {
		return err
	}

	for i := uint64(0); i < bitDepth; i++ {
		bits, err := cursor.Row(c, shard, 2+uint64(i))
		if err != nil {
			return err
		}
		bits = bits.Intersect(exists)
		if bits.IsEmpty() {
			continue
		}
		data.mergeBits(bits.Columns(), 1<<i)
	}
	for columnID, val := range data {
		// Convert to two's complement and add base back to value.
		val = uint64((2*(int64(val)>>63) + 1) * int64(val&^(1<<63)))
		f(columnID, int64(val))
	}
	return nil
}

func Distinct(c *rbf.Cursor, exists *rows.Row, shard uint64, f func(value uint64, columns *rows.Row) error) error {
	cols := exists.Columns()
	data := NewData(cols)
	bitDepth, err := depth(c)
	if err != nil {
		return err
	}

	for i := uint64(0); i < bitDepth; i++ {
		bits, err := cursor.Row(c, shard, 2+uint64(i))
		if err != nil {
			return err
		}
		bits = bits.Intersect(exists)
		if bits.IsEmpty() {
			continue
		}
		data.mergeBits(bits.Columns(), 1<<i)
	}
	idx := make(map[uint64][]uint64, len(data))
	for columnID, val := range data {
		// Convert to two's complement and add base back to value.
		val = uint64((2*(int64(val)>>63) + 1) * int64(val&^(1<<63)))
		idx[val] = append(idx[val], columnID)
	}
	for i := range cols {
		err := f(cols[i], rows.NewRow(idx[cols[i]]...))
		if err != nil {
			return err
		}
	}
	return nil
}

func depth(c *rbf.Cursor) (uint64, error) {
	m, err := c.Max()
	return m / rbf.ShardWidth, err
}
