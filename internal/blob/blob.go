package blob

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/keys"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/frieren/internal/util"
)

type Func func(id constants.ID, value []byte) uint64

type Tr func(id constants.ID, key uint64) []byte

func Upsert(txn *badger.Txn, store *store.Store) Func {
	h := xxhash.New()
	buf := new(bytes.Buffer)

	return func(field constants.ID, b []byte) uint64 {
		h.Reset()
		h.Write(b)
		hash := h.Sum64()
		if v, ok := store.HashCache.Get(hash); ok {
			return v.(uint64)
		}
		bhk := keys.BlobHash(buf, field, hash)
		it, err := txn.Get(bhk)
		if err != nil {
			if !errors.Is(err, badger.ErrKeyNotFound) {
				util.Exit("unexpected badger error", "err", err)
			}
			id := store.Seq.NextID(field)
			store.HashCache.Set(hash, id, 1)
			err = txn.Set(bytes.Clone(bhk),
				binary.BigEndian.AppendUint64(make([]byte, 8), id),
			)
			if err != nil {
				util.Exit("writing blob hash key", "err", err)
			}
			err = txn.Set(bytes.Clone(keys.BlobID(buf, field, id)), b)
			if err != nil {
				util.Exit("writing blob id", "err", err)
			}
			store.ValueCache.Set(id, b, int64(len(b)))
			return id
		}
		var id uint64
		err = it.Value(func(val []byte) error {
			id = binary.BigEndian.Uint64(val)
			return nil
		})
		if err != nil {
			util.Exit("reading blob id", "err", err)
		}
		store.HashCache.Set(hash, id, 1)
		return id
	}
}

func Translate(txn *badger.Txn, store *store.Store) Tr {
	b := new(bytes.Buffer)
	return func(field constants.ID, u uint64) []byte {
		if v, ok := store.ValueCache.Get(u); ok {
			return v.([]byte)
		}
		it, err := txn.Get(keys.BlobID(b, field, u))
		if err != nil {
			util.Exit("BUG: reading translated blob", "key", b.String(), "err", err)
		}
		data, err := it.ValueCopy(nil)
		if err != nil {
			return nil
		}
		store.ValueCache.Set(u, data, int64(len(data)))
		return data
	}
}
