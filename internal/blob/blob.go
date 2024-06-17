package blob

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

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

func Finder(txn *badger.Txn, store *store.Store, view string) Find {
	h := xxhash.New()
	buf := new(bytes.Buffer)
	return func(field constants.ID, b []byte) (uint64, bool) {
		if len(b) == 0 {
			b = emptyKey
		}
		h.Reset()
		h.Write(b)
		hash := h.Sum64()
		viewBlobHash := keys.BlobHash(buf, field, hash, view)
		h.Reset()
		h.Write(viewBlobHash)
		sum := h.Sum64()
		if v, ok := store.HashCache.Get(sum); ok {
			return v.(uint64), true
		}
		it, err := txn.Get(viewBlobHash)
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
		store.HashCache.Set(sum, id, 1)
		return id, true
	}
}

func Upsert(txn *badger.Txn, store *store.Store, seq *store.Sequence, view string) Func {
	h := Hash{}
	buf := new(bytes.Buffer)
	return func(field constants.ID, b []byte) uint64 {
		if len(b) == 0 {
			b = emptyKey
		}

		// We use the same translation logic for short strings and large blobs.
		// Instead of using actual blob as part of key we use hash of it
		hash := h.Sum(b)

		blobHashKey := keys.BlobHash(buf, field, hash, view)
		sum := h.Sum(blobHashKey)
		fmt.Println(sum, string(b))
		if v, ok := store.HashCache.Get(sum); ok {
			fmt.Println("=>", sum, string(b), v)
			return v.(uint64)
		}
		it, err := txn.Get(blobHashKey)
		if err != nil {
			if !errors.Is(err, badger.ErrKeyNotFound) {
				util.Exit("unexpected badger error", "err", err)
			}
			// Observability generates a large amount of data. Storing each copy per view
			// is wasteful.
			//
			// We save blobs in content addressable manner. Where content is identified
			// by hash of its content.

			id := seq.NextID(field)

			blobHashKey = bytes.Clone(blobHashKey)

			baseBlobHashKey := bytes.Clone(keys.BlobHash(buf, field, hash, ""))

			err = saveIfNotExists(txn, baseBlobHashKey, b)
			if err != nil {
				util.Exit("writing blob data", "err", err)
			}
			store.HashCache.Set(sum, id, 1)
			err = txn.Set(bytes.Clone(blobHashKey),
				binary.BigEndian.AppendUint64(make([]byte, 8), id),
			)
			if err != nil {
				util.Exit("writing blob hash key", "err", err)
			}
			idKey := keys.BlobID(buf, field, id, view)
			err = txn.Set(bytes.Clone(idKey),
				binary.BigEndian.AppendUint64(make([]byte, 8), hash),
			)
			if err != nil {
				util.Exit("writing blob id", "err", err)
			}

			// speedup fst building by caching id => blob
			idSum := h.Sum(idKey)
			store.ValueCache.Set(idSum, b, int64(len(b)))

			// Speedup find by caching blob_hash => blob
			baseSum := h.Sum(baseBlobHashKey)
			store.ValueCache.Set(baseSum, b, int64(len(b)))
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
		store.HashCache.Set(sum, id, 1)
		return id
	}
}

func saveIfNotExists(txn *badger.Txn, key, value []byte) error {
	_, err := txn.Get(key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return txn.Set(key, value)
		}
		return err
	}
	return nil
}

func Translate(txn *badger.Txn, store *store.Store, view string) Tr {
	b := new(bytes.Buffer)
	h := Hash{}
	return func(field constants.ID, u uint64) []byte {
		viewBlobKey := keys.BlobID(b, field, u, view)
		viewBlobHash := h.Sum(viewBlobKey)
		if v, ok := store.ValueCache.Get(viewBlobHash); ok {
			return v.([]byte)
		}
		it, err := txn.Get(viewBlobKey)
		if err != nil {
			util.Exit("BUG: reading translated blob key id", "key", b.String(), "err", err)
		}
		var caHash uint64
		it.Value(func(val []byte) error {
			caHash = binary.BigEndian.Uint64(val)
			return nil
		})
		caBlobKey := keys.BlobHash(b, field, caHash, "")
		caSum := h.Sum(caBlobKey)
		if v, ok := store.ValueCache.Get(caSum); ok {
			return v.([]byte)
		}
		it, err = txn.Get(caBlobKey)
		if err != nil {
			util.Exit("BUG: reading translated blob data", "key", b.String(), "err", err)
		}
		data, _ := it.ValueCopy(nil)
		store.ValueCache.Set(viewBlobHash, data, int64(len(data)))
		store.ValueCache.Set(caSum, data, int64(len(data)))
		return data
	}
}

type Hash struct {
	h xxhash.Digest
}

func (h *Hash) Sum(b []byte) uint64 {
	h.h.Reset()
	h.h.Write(b)
	return h.h.Sum64()
}
