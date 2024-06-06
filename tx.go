package ernestdb

import (
	"github.com/dgraph-io/badger/v4"
)

func Has(txn *badger.Txn, key []byte) bool {
	_, err := txn.Get(key)
	return err == nil
}
