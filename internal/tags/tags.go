package tags

import (
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/frieren/internal/fields"
	"github.com/gernest/rbf"
	"github.com/gernest/rows"
)

func Filter(tx *rbf.Tx, fra fields.Fragment, a, b *roaring64.Bitmap) (*rows.Row, error) {
	r := rows.NewRow()
	if !a.IsEmpty() {
		start := true
		it := a.Iterator()
		for it.HasNext() {
			label := it.Next()
			rw, err := fra.EqSet(tx, label)
			if err != nil {
				return nil, fmt.Errorf("reading labels %w", err)
			}
			if start {
				r = rw
				start = false
			} else {
				r = r.Intersect(rw)
			}
			if r.IsEmpty() {
				return r, nil
			}
		}
	}
	if !b.IsEmpty() {
		it := b.Iterator()
		exists, err := fra.Row(tx, 0)
		if err != nil {
			return nil, fmt.Errorf("reading labels exists bitmap %w", err)
		}
		r = exists
		for it.HasNext() {
			label := it.Next()
			rw, err := fra.EqSet(tx, label)
			if err != nil {
				return nil, fmt.Errorf("reading labels %w", err)
			}
			r = r.Difference(rw)
			if r.IsEmpty() {
				return r, nil
			}
		}
	}
	return r, nil
}
