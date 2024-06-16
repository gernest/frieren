package traces

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/rbf"
)

type queryContext struct {
	tx  *rbf.Tx
	txn *badger.Txn
	tr  blob.Tr
}

func newQueryContext(db *store.Store) (*queryContext, error) {
	tx, err := db.Index.Begin(false)
	if err != nil {
		return nil, err
	}
	txn := db.DB.NewTransaction(false)
	return &queryContext{
		tx:  tx,
		txn: txn,
		tr:  blob.Translate(txn, db),
	}, nil
}

func (ctx *queryContext) Close() {
	ctx.tx.Rollback()
	ctx.txn.Discard()
}
