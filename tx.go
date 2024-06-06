package ernestdb

import (
	"github.com/dgraph-io/badger/v4"
)

func Has(txn *badger.Txn, key []byte) bool {
	_, err := txn.Get(key)
	return err == nil
}

func Get(txn *badger.Txn, key []byte, value func(val []byte) error) error {
	it, err := txn.Get(key)
	if err != nil {
		return err
	}
	return it.Value(value)
}

func Prefix(txn *badger.Txn, prefix []byte, f func(key []byte, value Value) error) error {
	o := badger.DefaultIteratorOptions
	o.Prefix = prefix
	it := txn.NewIterator(o)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		err := f(item.Key(), item)
		if err != nil {
			return err
		}
	}
	return nil
}
