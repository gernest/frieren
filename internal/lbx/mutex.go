package lbx

import (
	"bytes"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/cursor"
	"github.com/gernest/rbf/dsl/mutex"
	"github.com/gernest/rbf/dsl/tx"
	"github.com/gernest/roaring"
	"github.com/gernest/rows"
	"github.com/prometheus/prometheus/model/labels"
)

var sep = []byte("=")

func Labels(c *rbf.Cursor, field string, tx *tx.Tx, rowID uint64) (labels.Labels, error) {
	var rows []uint64
	err := cursor.Rows(c, 0, func(row uint64) error {
		rows = append(rows, row)
		return nil
	}, roaring.NewBitmapColumnFilter(rowID))
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return labels.Labels{}, nil
	}
	rs := make(labels.Labels, 0, len(rows))
	tx.Keys(field, rows, func(value []byte) {
		name, value, _ := bytes.Cut(value, sep)
		rs = append(rs, labels.Label{
			Name:  string(name),
			Value: string(value),
		})
	})
	return rs, nil
}

func Unique(c *rbf.Cursor, exists *rows.Row, shard uint64, f func(value uint64, columns *rows.Row) error) error {
	o := roaring64.New()
	err := mutex.Distinct(c, o, exists)
	if err != nil {
		return err
	}
	it := o.Iterator()
	for it.HasNext() {
		rowID := it.Next()
		r, err := cursor.Row(c, shard, rowID)
		if err != nil {
			return err
		}
		err = f(rowID, r)
		if err != nil {
			return err
		}
	}
	return nil
}
