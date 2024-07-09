package lbx

import (
	"bytes"

	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/cursor"
	"github.com/gernest/rbf/dsl/tx"
	"github.com/prometheus/prometheus/model/labels"
)

var sep = []byte("=")

func Labels(c *rbf.Cursor, field string, tx *tx.Tx, rowID uint64) (labels.Labels, error) {
	r, err := cursor.Row(c, tx.Shard, rowID)
	if err != nil {
		return nil, err
	}
	if r.IsEmpty() {
		return labels.Labels{}, nil
	}
	b := labels.NewScratchBuilder(int(r.Count()))
	r.RangeColumns(func(u uint64) error {
		name, value, _ := bytes.Cut(tx.Tr.Key(field, u), sep)
		b.Add(string(name), string(value))
		return nil
	})
	return b.Labels(), nil
}

func Blobs(c *rbf.Cursor, field string, tx *tx.Tx, rowID uint64) (labels.Labels, error) {
	r, err := cursor.Row(c, tx.Shard, rowID)
	if err != nil {
		return nil, err
	}
	if r.IsEmpty() {
		return labels.Labels{}, nil
	}
	b := labels.NewScratchBuilder(int(r.Count()))
	r.RangeColumns(func(u uint64) error {
		name, value, _ := bytes.Cut(tx.Tr.Blob(field, u), sep)
		b.Add(string(name), string(value))
		return nil
	})
	return b.Labels(), nil
}
