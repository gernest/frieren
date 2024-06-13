package tags

import (
	"fmt"

	"github.com/gernest/frieren/internal/fields"
	"github.com/gernest/rbf"
	"github.com/gernest/rows"
)

func Filter(tx *rbf.Tx, fra *fields.Fragment, labels []uint64) (*rows.Row, error) {
	r := rows.NewRow()
	for i := range labels {
		rw, err := fra.EqSet(tx, labels[i])
		if err != nil {
			return nil, fmt.Errorf("reading labels %w", err)
		}
		if i == 0 {
			r = rw
			continue
		}
		r = r.Intersect(rw)
	}
	return r, nil
}
