package blob

import (
	"fmt"

	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/keys"
	"github.com/gernest/frieren/internal/store"
)

type Func func([]byte) uint64

type Tr func(id uint64, f func([]byte) error) error

func Upsert(txn *badger.Txn) Func {
	h := xxhash.New()
	var key keys.Blob
	return func(b []byte) uint64 {
		h.Reset()
		h.Write(b)
		key.BlobID = h.Sum64()
		k := key.Key()
		if store.Has(txn, k) {
			return key.BlobID
		}
		err := txn.Set(key.Key(), b)
		if err != nil {
			panic(fmt.Sprintf("failed saving blob to storage %v", err))
		}
		return key.BlobID
	}
}

func Translate(txn *badger.Txn) Tr {
	slice := (&keys.Blob{}).Slice()
	buf := make([]byte, 0, len(slice)*8)
	return func(u uint64, f func([]byte) error) error {
		slice[len(slice)-1] = u
		it, err := txn.Get(keys.Encode(buf, slice))
		if err != nil {
			return err
		}
		return it.Value(f)
	}
}
