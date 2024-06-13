package fields

import (
	"bytes"
	"fmt"
	"math/bits"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/shardwidth"
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/short_txkey"
	"github.com/gernest/roaring"
	"github.com/gernest/rows"
	"github.com/prometheus/prometheus/model/labels"
)

type Fragment struct {
	ID    constants.ID
	Shard uint64
	View  string
	full  string
}

func New(id constants.ID, shard uint64, view string) *Fragment {
	return &Fragment{ID: id, Shard: shard, View: view}
}

func (v *Fragment) WithShard(shard uint64) *Fragment {
	return &Fragment{
		ID:    v.ID,
		Shard: shard,
		View:  v.View,
	}
}

func (v *Fragment) String() string {
	if v.full == "" {
		key := short_txkey.Prefix("", fmt.Sprint(v.ID), v.View, v.Shard)
		v.full = string(key)
	}
	return v.full
}

var eql = []byte("=")

func (f *Fragment) Shards(tx *rbf.Tx) ([]uint64, error) {
	r, err := tx.RoaringBitmap(f.String())
	if err != nil {
		return nil, err
	}
	return r.Slice(), nil
}

func (f *Fragment) Labels(tx *rbf.Tx, tr blob.Tr, column uint64) (labels.Labels, error) {
	rows, err := f.ReadSetValue(tx, column)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return labels.EmptyLabels(), nil
	}
	o := make(labels.Labels, 0, len(rows))
	for i := range rows {
		err = tr(constants.MetricsLabels, rows[i], func(val []byte) error {
			key, value, _ := bytes.Cut(val, eql)
			o = append(o, labels.Label{
				Name:  string(key),
				Value: string(value),
			})
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("translating  series labels %w", err)
		}
	}
	return o, nil
}

func (f *Fragment) ReadSetValue(tx *rbf.Tx, column uint64) ([]uint64, error) {
	rs, err := f.Rows(tx, 0, roaring.NewBitmapColumnFilter(column))
	if err != nil {
		return nil, err
	}
	return rs, nil
}

func (f *Fragment) MutexValue(tx *rbf.Tx, column uint64) (uint64, error) {
	rs, err := f.Rows(tx, 0, roaring.NewBitmapColumnFilter(column))
	if err != nil {
		return 0, err
	}
	return rs[0], nil
}

func (f *Fragment) EqSet(tx *rbf.Tx, value uint64) (*rows.Row, error) {
	return f.Row(tx, value)
}

func (f *Fragment) EqBSI(tx *rbf.Tx, value uint64) (*rows.Row, error) {
	return f.rangeEQ(tx, value)
}

func (f *Fragment) False(tx *rbf.Tx) (*rows.Row, error) {
	return f.Row(tx, falseRowOffset)
}

func (f *Fragment) True(tx *rbf.Tx) (*rows.Row, error) {
	return f.Row(tx, trueRowOffset)
}

func (f *Fragment) Exists(tx *rbf.Tx) (*rows.Row, error) {
	return f.Row(tx, bsiExistsBit)
}

func (f *Fragment) TransposeBSI(tx *rbf.Tx, filters *rows.Row) (*roaring64.Bitmap, error) {
	return f.transposeBSI(tx, filters)
}

func (f *Fragment) transposeBSI(tx *rbf.Tx, columns *rows.Row) (*roaring64.Bitmap, error) {
	exists, err := f.Row(tx, bsiExistsBit)
	if err != nil {
		return nil, err
	}
	if columns != nil {
		exists = exists.Intersect(columns)
	}
	if !exists.Any() {
		// No relevant BSI values are present in this fragment.
		return roaring64.New(), nil
	}
	// Populate a map with the BSI data.
	data := make(map[uint64]uint64)
	mergeBits(exists, 0, data)
	for i := uint64(0); i < bitDepth; i++ {
		bits, err := f.Row(tx, bsiOffsetBit+uint64(i))
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

func (fra *Fragment) ExtractBSI(tx *rbf.Tx, exists *rows.Row, mapping map[uint64]int, f func(i int, v uint64) error) error {
	return fra.extractBSI(tx, exists, mapping, f)
}

func (fra *Fragment) extractBSI(tx *rbf.Tx, exists *rows.Row, mapping map[uint64]int, f func(i int, v uint64) error) error {
	data := make(map[uint64]uint64)
	mergeBits(exists, 0, data)

	for i := uint64(0); i < bitDepth; i++ {
		bits, err := fra.Row(tx, bsiOffsetBit+uint64(i))
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

func (f *Fragment) Between(tx *rbf.Tx, min, max uint64) (*rows.Row, error) {
	return f.rangeBetween(tx, min, max)
}

func (f *Fragment) Row(tx *rbf.Tx, rowID uint64) (*rows.Row, error) {
	shard := f.Shard
	view := f.String()
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

func (f *Fragment) Rows(tx *rbf.Tx, start uint64, filters ...roaring.BitmapFilter) ([]uint64, error) {
	var rows []uint64
	cb := func(row uint64) error {
		rows = append(rows, row)
		return nil
	}
	startKey := rowToKey(start)
	filter := roaring.NewBitmapRowFilter(cb, filters...)
	err := tx.ApplyFilter(f.String(), startKey, filter)
	if err != nil {
		return nil, err
	} else {
		return rows, nil
	}
}

func (f *Fragment) rangeEQ(tx *rbf.Tx, predicate uint64) (*rows.Row, error) {
	// Start with set of columns with values set.
	b, err := f.Row(tx, bsiExistsBit)
	if err != nil {
		return nil, err
	}
	bitDepth := bits.Len64(predicate)
	// Filter any bits that don't match the current bit value.
	for i := int(bitDepth - 1); i >= 0; i-- {
		row, err := f.Row(tx, uint64(bsiOffsetBit+i))
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

func (f *Fragment) rangeNEQ(tx *rbf.Tx, predicate uint64) (*rows.Row, error) {
	// Start with set of columns with values set.
	b, err := f.Row(tx, bsiExistsBit)
	if err != nil {
		return nil, err
	}

	// Get the equal bitmap.
	eq, err := f.rangeEQ(tx, predicate)
	if err != nil {
		return nil, err
	}

	// Not-null minus the equal bitmap.
	b = b.Difference(eq)

	return b, nil
}

func (f *Fragment) rangeLT(tx *rbf.Tx, predicate uint64, allowEquality bool) (*rows.Row, error) {
	if predicate == 1 && !allowEquality {
		predicate, allowEquality = 0, true
	}

	// Start with set of columns with values set.
	b, err := f.Row(tx, bsiExistsBit)
	if err != nil {
		return nil, err
	}
	switch {
	case predicate == 0 && !allowEquality:
		// Match all negative integers.
		return rows.NewRow(), nil
	case predicate == 0 && allowEquality:
		// Match all integers that are either negative or 0.
		return f.rangeEQ(tx, 0)
	default:
		return f.rangeLTUnsigned(tx, b, bitDepth, predicate, allowEquality)
	}
}

func (f *Fragment) rangeLTUnsigned(tx *rbf.Tx, filter *rows.Row, bitDepth, predicate uint64, allowEquality bool) (*rows.Row, error) {
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
			row, err := f.Row(tx, uint64(bsiOffsetBit+i))
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
		row, err := f.Row(tx, uint64(bsiOffsetBit+i))
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

func (f *Fragment) rangeGT(tx *rbf.Tx, predicate uint64, allowEquality bool) (*rows.Row, error) {
	b, err := f.Row(tx, bsiExistsBit)
	if err != nil {
		return nil, err
	}
	switch {
	case predicate == 0 && !allowEquality:
		// Match all positive numbers except zero.
		nonzero, err := f.rangeNEQ(tx, 0)
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
		return f.rangeGTUnsigned(tx, b, bitDepth, uint64(predicate), allowEquality)
	}
}

func (f *Fragment) rangeGTUnsigned(tx *rbf.Tx, filter *rows.Row, bitDepth, predicate uint64, allowEquality bool) (*rows.Row, error) {
prep:
	switch {
	case predicate == 0 && allowEquality:
		// This query matches all possible values.
		return filter, nil
	case predicate == 0 && !allowEquality:
		// This query matches everything that is not 0.
		matches := rows.NewRow()
		for i := uint64(0); i < bitDepth; i++ {
			row, err := f.Row(tx, uint64(bsiOffsetBit+i))
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
		row, err := f.Row(tx, uint64(bsiOffsetBit+i))
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

func (f *Fragment) rangeBetween(tx *rbf.Tx, predicateMin, predicateMax uint64) (*rows.Row, error) {
	b, err := f.Row(tx, bsiExistsBit)
	if err != nil {
		return nil, err
	}

	switch {
	case predicateMin == predicateMax:
		return f.rangeEQ(tx, predicateMin)
	default:
		// Handle positive-only values.
		r, err := f.Row(tx, bsiSignBit)
		if err != nil {
			return nil, err
		}
		return f.rangeBetweenUnsigned(tx, b.Difference(r), predicateMin, predicateMax)
	}
}

func (f *Fragment) rangeBetweenUnsigned(tx *rbf.Tx, filter *rows.Row, predicateMin, predicateMax uint64) (*rows.Row, error) {
	switch {
	case predicateMax > (1<<bitDepth)-1:
		// The upper bound cannot be violated.
		return f.rangeGTUnsigned(tx, filter, bitDepth, predicateMin, true)
	case predicateMin == 0:
		// The lower bound cannot be violated.
		return f.rangeLTUnsigned(tx, filter, bitDepth, predicateMax, true)
	}

	// Compare any upper bits which are equal.
	diffLen := bits.Len64(predicateMax ^ predicateMin)
	remaining := filter
	for i := int(bitDepth - 1); i >= diffLen; i-- {
		row, err := f.Row(tx, uint64(bsiOffsetBit+i))
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
	remaining, err = f.rangeGTUnsigned(tx, remaining, uint64(diffLen), predicateMin, true)
	if err != nil {
		return nil, err
	}
	remaining, err = f.rangeLTUnsigned(tx, remaining, uint64(diffLen), predicateMax, true)
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
