package blob

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/keys"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/frieren/internal/util"
)

type Func func([]byte) uint64

type Tr func(id uint64, f func([]byte) error) error

func Upsert(txn *badger.Txn, seq *store.Seq) Func {
	h := xxhash.New()
	blobID := (&keys.BlobID{}).Slice()
	blobHash := (&keys.BlobID{}).Slice()
	buf := make([]byte, len(blobID)*8)

	return func(b []byte) uint64 {
		h.Reset()
		h.Write(b)
		hash := h.Sum64()
		blobHash[len(blobHash)-1] = hash
		bhk := keys.Encode(buf, blobHash)
		it, err := txn.Get(bhk)
		if err != nil {
			if !errors.Is(err, badger.ErrKeyNotFound) {
				util.Exit("unexpected badger error", "err", err)
			}
			id := seq.NextID()
			blobID[len(blobID)-1] = id
			err = txn.Set(bytes.Clone(bhk),
				binary.BigEndian.AppendUint64(make([]byte, 0), id),
			)
			if err != nil {
				util.Exit("writing blob hash key", "err", err)
			}
			err = txn.Set(keys.Encode(nil, blobID), b)
			if err != nil {
				util.Exit("writing blob id", "err", err)
			}
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
		return id
	}
}

func Translate(txn *badger.Txn) Tr {
	slice := (&keys.BlobID{}).Slice()
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
