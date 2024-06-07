package ro

import (
	"errors"
	"math/bits"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/frieren/shardwidth"
	"github.com/gernest/rbf"
	"github.com/gernest/roaring"
	"github.com/gernest/rows"
)

var ErrSkip = errors.New("skip")

func ReadSetValue(shard uint64, field, view string, tx *rbf.Tx, column uint64) ([]uint64, error) {
	rw := ViewFor(field, view, shard)
	rs, err := Rows(rw, tx, 0, roaring.NewBitmapColumnFilter(column))
	if err != nil {
		return nil, err
	}
	return rs, nil
}

func MutexValue(shard uint64, field, view string, tx *rbf.Tx, column uint64) (uint64, error) {
	rs, err := Rows(ViewFor(field, view, shard), tx, 0, roaring.NewBitmapColumnFilter(column))
	if err != nil {
		return 0, err
	}
	return rs[0], nil
}

func EqSet(shard uint64, field, view string, tx *rbf.Tx, value uint64) (*rows.Row, error) {
	return Row(shard, ViewFor(field, view, shard), tx, value)
}

func EqBSI(shard uint64, field, view string, tx *rbf.Tx, value uint64) (*rows.Row, error) {
	return rangeEQ(shard, ViewFor(field, view, shard), tx, value)
}

func False(shard uint64, field, view string, tx *rbf.Tx) (*rows.Row, error) {
	return Row(shard, ViewFor(field, view, shard), tx, falseRowOffset)
}

func Exists(shard uint64, field, view string, tx *rbf.Tx) (*rows.Row, error) {
	return Row(shard, ViewFor(field, view, shard), tx, bsiExistsBit)
}

func TransposeBSI(shard uint64, field, view string, tx *rbf.Tx, filters *rows.Row) (*roaring64.Bitmap, error) {
	return transposeBSI(tx, shard, ViewFor(field, view, shard), filters)
}

func transposeBSI(tx *rbf.Tx, shard uint64, view string, columns *rows.Row) (*roaring64.Bitmap, error) {
	exists, err := Row(shard, view, tx, bsiExistsBit)
	if err != nil {
		return nil, err
	}
	exists = exists.Intersect(columns)
	if !exists.Any() {
		// No relevant BSI values are present in this fragment.
		return roaring64.New(), nil
	}
	// Populate a map with the BSI data.
	data := make(map[uint64]uint64)
	mergeBits(exists, 0, data)
	for i := uint64(0); i < bitDepth; i++ {
		bits, err := Row(shard, view, tx, bsiOffsetBit+uint64(i))
		if err != nil {
			return nil, err
		}
		bits = bits.Intersect(exists)
		mergeBits(bits, 1<<i, data)
	}
	o := roaring64.New()
	for _, val := range data {
		// Convert to two's complement and add base back to value.
		val = uint64((2*(int64(val)>>63) + 1) * int64(val&^(1<<63)))
		o.Add(val)
	}
	return o, nil
}

func ExtractBSI(shard uint64, view string, tx *rbf.Tx, exists *rows.Row, mapping map[uint64]int, f func(i int, v uint64) error) error {
	data := make(map[uint64]uint64)
	mergeBits(exists, 0, data)

	for i := uint64(0); i < bitDepth; i++ {
		bits, err := Row(shard, view, tx, bsiOffsetBit+uint64(i))
		if err != nil {
			return err
		}
		bits = bits.Intersect(exists)
		mergeBits(bits, 1<<i, data)
	}
	result := make([][]uint64, len(mapping))
	for columnID, val := range data {
		// Convert to two's complement and add base back to value.
		val = uint64((2*(int64(val)>>63) + 1) * int64(val&^(1<<63)))
		result[mapping[columnID]] = []uint64{val}
		err := f(mapping[columnID], val)
		if err != nil {
			return err
		}
	}
	return nil
}

func Between(shard uint64, field, view string, tx *rbf.Tx, min, max uint64) (*rows.Row, error) {
	return rangeBetween(shard, ViewFor(field, view, shard), tx, min, max)
}

func Row(shard uint64, view string, tx *rbf.Tx, rowID uint64) (*rows.Row, error) {
	data, err := tx.OffsetRange(view,
		shard*shardwidth.ShardWidth,
		rowID*shardwidth.ShardWidth,
		(rowID+1)*shardwidth.ShardWidth,
	)
	if err != nil {
		return nil, err
	}
	row := &rows.Row{
		Segments: []rows.RowSegment{
			rows.NewSegment(data, shard, true),
		},
	}
	row.InvalidateCount()
	return row, nil
}

func Rows(view string, tx *rbf.Tx, start uint64, filters ...roaring.BitmapFilter) ([]uint64, error) {
	var rows []uint64
	cb := func(row uint64) error {
		rows = append(rows, row)
		return nil
	}
	startKey := rowToKey(start)
	filter := roaring.NewBitmapRowFilter(cb, filters...)
	err := tx.ApplyFilter(view, startKey, filter)
	if err != nil {
		return nil, err
	} else {
		return rows, nil
	}
}

func rangeEQ(shard uint64, view string, tx *rbf.Tx, predicate uint64) (*rows.Row, error) {
	// Start with set of columns with values set.
	b, err := Row(shard, view, tx, bsiExistsBit)
	if err != nil {
		return nil, err
	}
	bitDepth := bits.Len64(predicate)
	// Filter any bits that don't match the current bit value.
	for i := int(bitDepth - 1); i >= 0; i-- {
		row, err := Row(shard, view, tx, uint64(bsiOffsetBit+i))
		if err != nil {
			return nil, err
		}
		bit := (predicate >> uint(i)) & 1
		if bit == 1 {
			b = b.Intersect(row)
		} else {
			b = b.Difference(row)
		}
	}
	return b, nil
}

func rangeNEQ(shard uint64, view string, tx *rbf.Tx, predicate uint64) (*rows.Row, error) {
	// Start with set of columns with values set.
	b, err := Row(shard, view, tx, bsiExistsBit)
	if err != nil {
		return nil, err
	}

	// Get the equal bitmap.
	eq, err := rangeEQ(shard, view, tx, predicate)
	if err != nil {
		return nil, err
	}

	// Not-null minus the equal bitmap.
	b = b.Difference(eq)

	return b, nil
}

func rangeLT(shard uint64, view string, tx *rbf.Tx, predicate uint64, allowEquality bool) (*rows.Row, error) {
	if predicate == 1 && !allowEquality {
		predicate, allowEquality = 0, true
	}

	// Start with set of columns with values set.
	b, err := Row(shard, view, tx, bsiExistsBit)
	if err != nil {
		return nil, err
	}
	switch {
	case predicate == 0 && !allowEquality:
		// Match all negative integers.
		return rows.NewRow(), nil
	case predicate == 0 && allowEquality:
		// Match all integers that are either negative or 0.
		return rangeEQ(shard, view, tx, 0)
	default:
		return rangeLTUnsigned(shard, view, tx, b, bitDepth, predicate, allowEquality)
	}
}

func rangeLTUnsigned(shard uint64, view string, tx *rbf.Tx, filter *rows.Row, bitDepth, predicate uint64, allowEquality bool) (*rows.Row, error) {
	switch {
	case uint64(bits.Len64(predicate)) > bitDepth:
		fallthrough
	case predicate == (1<<bitDepth)-1 && allowEquality:
		// This query matches all possible values.
		return filter, nil
	case predicate == (1<<bitDepth)-1 && !allowEquality:
		// This query matches everything that is not (1<<bitDepth)-1.
		matches := rows.NewRow()
		for i := uint64(0); i < bitDepth; i++ {
			row, err := Row(shard, view, tx, uint64(bsiOffsetBit+i))
			if err != nil {
				return nil, err
			}
			matches = matches.Union(filter.Difference(row))
		}
		return matches, nil
	case allowEquality:
		predicate++
	}

	// Compare intermediate bits.
	matched := rows.NewRow()
	remaining := filter
	for i := int(bitDepth - 1); i >= 0 && predicate > 0 && remaining.Any(); i-- {
		row, err := Row(shard, view, tx, uint64(bsiOffsetBit+i))
		if err != nil {
			return nil, err
		}
		zeroes := remaining.Difference(row)
		switch (predicate >> uint(i)) & 1 {
		case 1:
			// Match everything with a zero bit here.
			matched = matched.Union(zeroes)
			predicate &^= 1 << uint(i)
		case 0:
			// Discard everything with a one bit here.
			remaining = zeroes
		}
	}

	return matched, nil
}

func rangeGT(shard uint64, view string, tx *rbf.Tx, predicate uint64, allowEquality bool) (*rows.Row, error) {
	b, err := Row(shard, view, tx, bsiExistsBit)
	if err != nil {
		return nil, err
	}
	switch {
	case predicate == 0 && !allowEquality:
		// Match all positive numbers except zero.
		nonzero, err := rangeNEQ(shard, view, tx, 0)
		if err != nil {
			return nil, err
		}
		b = nonzero
		fallthrough
	case predicate == 0 && allowEquality:
		// Match all positive numbers.
		return b, nil
	default:
		// Match all positive numbers greater than the predicate.
		return rangeGTUnsigned(shard, view, tx, b, bitDepth, uint64(predicate), allowEquality)
	}
}

func rangeGTUnsigned(shard uint64, view string, tx *rbf.Tx, filter *rows.Row, bitDepth, predicate uint64, allowEquality bool) (*rows.Row, error) {
prep:
	switch {
	case predicate == 0 && allowEquality:
		// This query matches all possible values.
		return filter, nil
	case predicate == 0 && !allowEquality:
		// This query matches everything that is not 0.
		matches := rows.NewRow()
		for i := uint64(0); i < bitDepth; i++ {
			row, err := Row(shard, view, tx, uint64(bsiOffsetBit+i))
			if err != nil {
				return nil, err
			}
			matches = matches.Union(filter.Intersect(row))
		}
		return matches, nil
	case !allowEquality && uint64(bits.Len64(predicate)) > bitDepth:
		// The predicate is bigger than the BSI width, so nothing can be bigger.
		return rows.NewRow(), nil
	case allowEquality:
		predicate--
		allowEquality = false
		goto prep
	}

	// Compare intermediate bits.
	matched := rows.NewRow()
	remaining := filter
	predicate |= (^uint64(0)) << bitDepth
	for i := int(bitDepth - 1); i >= 0 && predicate < ^uint64(0) && remaining.Any(); i-- {
		row, err := Row(shard, view, tx, uint64(bsiOffsetBit+i))
		if err != nil {
			return nil, err
		}
		ones := remaining.Intersect(row)
		switch (predicate >> uint(i)) & 1 {
		case 1:
			// Discard everything with a zero bit here.
			remaining = ones
		case 0:
			// Match everything with a one bit here.
			matched = matched.Union(ones)
			predicate |= 1 << uint(i)
		}
	}

	return matched, nil
}

func rangeBetween(shard uint64, view string, tx *rbf.Tx, predicateMin, predicateMax uint64) (*rows.Row, error) {
	b, err := Row(shard, view, tx, bsiExistsBit)
	if err != nil {
		return nil, err
	}

	switch {
	case predicateMin == predicateMax:
		return rangeEQ(shard, view, tx, predicateMin)
	default:
		// Handle positive-only values.
		r, err := Row(shard, view, tx, bsiSignBit)
		if err != nil {
			return nil, err
		}
		return rangeBetweenUnsigned(shard, view, tx, b.Difference(r), predicateMin, predicateMax)
	}
}

func rangeBetweenUnsigned(shard uint64, view string, tx *rbf.Tx, filter *rows.Row, predicateMin, predicateMax uint64) (*rows.Row, error) {
	switch {
	case predicateMax > (1<<bitDepth)-1:
		// The upper bound cannot be violated.
		return rangeGTUnsigned(shard, view, tx, filter, bitDepth, predicateMin, true)
	case predicateMin == 0:
		// The lower bound cannot be violated.
		return rangeLTUnsigned(shard, view, tx, filter, bitDepth, predicateMax, true)
	}

	// Compare any upper bits which are equal.
	diffLen := bits.Len64(predicateMax ^ predicateMin)
	remaining := filter
	for i := int(bitDepth - 1); i >= diffLen; i-- {
		row, err := Row(shard, view, tx, uint64(bsiOffsetBit+i))
		if err != nil {
			return nil, err
		}
		switch (predicateMin >> uint(i)) & 1 {
		case 1:
			remaining = remaining.Intersect(row)
		case 0:
			remaining = remaining.Difference(row)
		}
	}

	// Clear the bits we just compared.
	equalMask := (^uint64(0)) << diffLen
	predicateMin &^= equalMask
	predicateMax &^= equalMask

	var err error
	remaining, err = rangeGTUnsigned(shard, view, tx, remaining, uint64(diffLen), predicateMin, true)
	if err != nil {
		return nil, err
	}
	remaining, err = rangeLTUnsigned(shard, view, tx, remaining, uint64(diffLen), predicateMax, true)
	if err != nil {
		return nil, err
	}
	return remaining, nil
}

// width of roaring containers is 2^16
const containerWidth = 1 << 16

func rowToKey(rowID uint64) (key uint64) {
	return rowID * (shardwidth.ShardWidth / containerWidth)
}

func mergeBits(bits *rows.Row, mask uint64, out map[uint64]uint64) {
	for _, v := range bits.Columns() {
		out[v] |= mask
	}
}
