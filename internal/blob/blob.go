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

type Find func(id constants.ID, value []byte) (uint64, bool)

type Tr func(id constants.ID, key uint64) []byte

var emptyKey = []byte{
	0x00, 0x00, 0x00,
	0x4d, 0x54, 0x4d, 0x54, // MTMT
	0x00,
	0xc2, 0xa0, // NO-BREAK SPACE
	0x00,
}

func Finder(txn *badger.Txn, store *store.Store) Find {
	h := xxhash.New()
	buf := new(bytes.Buffer)
	return func(field constants.ID, b []byte) (uint64, bool) {
		if len(b) == 0 {
			b = emptyKey
		}
		h.Reset()
		h.Write(b)
		hash := h.Sum64()
		if v, ok := store.HashCache.Get(hash); ok {
			return v.(uint64), true
		}
		bhk := keys.BlobHash(buf, field, hash)
		it, err := txn.Get(bhk)
		if err != nil {
			if !errors.Is(err, badger.ErrKeyNotFound) {
				util.Exit("finding blob id", "err", err)
			}
			return 0, false
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
		return id, true
	}
}

func Upsert(txn *badger.Txn, store *store.Store) Func {
	h := xxhash.New()
	buf := new(bytes.Buffer)

	return func(field constants.ID, b []byte) uint64 {
		if len(b) == 0 {
			b = emptyKey
		}
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
			idKey := keys.BlobID(buf, field, id)
			err = txn.Set(bytes.Clone(idKey), b)
			if err != nil {
				util.Exit("writing blob id", "err", err)
			}
			h.Reset()
			h.Write(idKey)
			store.ValueCache.Set(h.Sum64(), b, int64(len(b)))
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
	h := xxhash.Digest{}
	return func(field constants.ID, u uint64) []byte {
		key := keys.BlobID(b, field, u)
		h.Reset()
		h.Write(key)
		hash := h.Sum64()
		if v, ok := store.ValueCache.Get(hash); ok {
			return v.([]byte)
		}
		it, err := txn.Get(key)
		if err != nil {
			util.Exit("BUG: reading translated blob", "key", b.String(), "err", err)
		}
		data, err := it.ValueCopy(nil)
		if err != nil {
			return nil
		}
		store.ValueCache.Set(hash, data, int64(len(data)))
		return data
	}
}
