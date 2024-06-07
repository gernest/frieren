package metrics

import (
	"testing"

	"github.com/gernest/rbf"
	"github.com/gernest/roaring/shardwidth"
)

func TestYay(t *testing.T) {
	db := rbf.NewDB(t.TempDir(), nil)

	db.Open()
	defer db.Close()

	sw := uint64(shardwidth.ShardWidth)
	sw2 := sw * 2
	values := []uint64{1, 2, 3, 3, sw + 1, sw2}
	o := make([]uint64, 0, len(values))
	for i := range values {
		o = append(o, (values[i])%shardwidth.ShardWidth)
	}
	view := "=test"
	tx, _ := db.Begin(true)
	tx.Add(view, o...)
	tx.Commit()

	tx, _ = db.Begin(false)
	defer tx.Rollback()

	e, _ := row(1, view, tx, 0)
	t.Error(e.Columns())
}
