package lbx

import (
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/cursor"
	"github.com/gernest/rbf/dsl/tx"
	"github.com/gernest/rows"
)

func Histograms(c *rbf.Cursor, tx *tx.Tx) (*rows.Row, error) {
	return cursor.Row(c, tx.Shard, 1)
}
