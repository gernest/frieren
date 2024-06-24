package store

import (
	"bytes"
	"crypto/sha512"
	"errors"
	"path/filepath"

	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/ristretto"
	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/encoding"
	"github.com/gernest/frieren/internal/keys"
	"github.com/gernest/frieren/internal/util"
	"github.com/gernest/rbf"
)

type Key [sha512.Size256]byte

type Value interface {
	Value(f func(val []byte) error) error
}

type Store struct {
	path       string
	db         *badger.DB
	blob       *badger.DB
	idx        *rbf.DB
	seq        *Seq
	hashCache  *ristretto.Cache
	valueCache *ristretto.Cache
}

func (s *Store) Path() string {
	return s.path
}

const hashItems = (16 << 20) / 16

func New(path string) (*Store, error) {
	dbPath := filepath.Join(path, "db")
	blobsPath := filepath.Join(path, "blobs")

	db, err := badger.Open(badger.DefaultOptions(dbPath).
		WithLogger(nil))
	if err != nil {
		return nil, err
	}
	blob, err := badger.Open(badger.DefaultOptions(blobsPath).
		WithLogger(nil))
	if err != nil {
		return nil, err
	}
	idxPath := filepath.Join(path, "index")
	idx := rbf.NewDB(idxPath, nil)
	err = idx.Open()
	if err != nil {
		db.Close()
		blob.Close()
		return nil, err
	}
	hashCache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: hashItems * 10,
		MaxCost:     hashItems,
		BufferItems: 64,
	})
	if err != nil {
		idx.Close()
		db.Close()
		blob.Close()
		return nil, err
	}
	valueCache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,
		MaxCost:     1 << 30,
		BufferItems: 64,
	})
	if err != nil {
		idx.Close()
		db.Close()
		hashCache.Close()
		blob.Close()
		return nil, err
	}

	return &Store{path: path, db: db, blob: blob, idx: idx,
		seq: NewSequence(db), hashCache: hashCache,
		valueCache: valueCache}, nil
}

func (s *Store) View(f func(tx *Tx) error) error {
	return s.tx(false, f)
}

func (s *Store) Update(f func(tx *Tx) error) error {
	return s.tx(true, f)
}

func (s *Store) tx(update bool, f func(tx *Tx) error) error {
	tx, err := s.newCtx(update)
	if err != nil {
		return err
	}
	err = f(tx)
	if err != nil {
		tx.Discard()
		return err
	}
	return tx.Commit()
}

func (s *Store) newCtx(update bool) (*Tx, error) {
	tx, err := s.idx.Begin(update)
	if err != nil {
		return nil, err
	}
	return &Tx{
		update: update,
		db:     s.db.NewTransaction(update),
		blob:   s.blob.NewTransaction(update),
		idx:    tx,
		store:  s,
	}, nil
}

func (s *Store) Close() error {
	s.hashCache.Close()
	s.valueCache.Close()
	return errors.Join(
		s.seq.Release(),
		s.idx.Close(), s.db.Close(),
	)
}

type Tx struct {
	update bool
	db     *badger.Txn
	blob   *badger.Txn
	idx    *rbf.Tx
	store  *Store
}

func (ctx *Tx) Tx() *rbf.Tx {
	return ctx.idx
}

func (ctx *Tx) Txn() *badger.Txn {
	return ctx.db
}

func (ctx *Tx) Discard() {
	ctx.db.Discard()
	ctx.blob.Discard()
	ctx.idx.Rollback()
}

func (ctx *Tx) Commit() error {
	if !ctx.update {
		ctx.Discard()
		return nil
	}
	return errors.Join(
		ctx.db.Commit(),
		ctx.blob.Commit(),
		ctx.idx.Commit(),
	)
}

type View struct {
	Tx    *Tx
	View  string
	hash  Hash
	Seq   *Sequence
	Shard *v1.Shard
}

func (v *View) Txn() *badger.Txn {
	return v.Tx.db
}

func (v *View) Index() *rbf.Tx {
	return v.Tx.idx
}

func (ctx *Tx) View(shard *v1.Shard, view string) *View {
	return &View{
		Tx:    ctx,
		View:  view,
		Shard: shard,
		Seq:   ctx.store.seq.Sequence(view),
	}
}

func (t *View) Tr(field constants.ID, u uint64) []byte {
	viewBlobKey := keys.BlobID(field, u, t.View)
	it, err := t.Tx.db.Get(viewBlobKey)
	if err != nil {
		util.Exit("BUG: reading translated blob key id", "key", string(viewBlobKey), "err", err)
	}
	checksum, err := it.ValueCopy(nil)
	if err != nil {
		util.Exit("BUG: reading translated blob checksum", "key", string(viewBlobKey), "err", err)
	}
	checksumHash := t.hash.Sum(checksum)
	if v, ok := t.Tx.store.valueCache.Get(checksumHash); ok {
		return v.([]byte)
	}
	data := t.Tx.blobGet(Key(checksum))
	t.Tx.store.valueCache.Set(checksumHash, data, int64(len(data)))
	return data
}

func (f *View) Find(field constants.ID, b []byte) (uint64, bool) {
	if len(b) == 0 {
		b = emptyKey
	}
	baseKey := sum(b)
	hash := f.hash.Sum(baseKey[:])

	viewBlobHash := keys.BlobHash(field, hash, f.View)
	sum := f.hash.Sum(viewBlobHash)
	if v, ok := f.Tx.store.hashCache.Get(sum); ok {
		return v.(uint64), true
	}
	it, err := f.Tx.db.Get(viewBlobHash)
	if err != nil {
		if !errors.Is(err, badger.ErrKeyNotFound) {
			util.Exit("finding blob id", "err", err)
		}
		return 0, false
	}
	var id uint64
	err = it.Value(func(val []byte) error {
		id = encoding.Uint64(val)
		return nil
	})
	if err != nil {
		util.Exit("reading blob id", "err", err)
	}
	f.Tx.store.hashCache.Set(sum, id, 1)
	return id, true
}

type Hash struct {
	xxhash.Digest
}

func (h *Hash) Sum(b []byte) uint64 {
	h.Reset()
	h.Write(b)
	return h.Sum64()
}

var emptyKey = []byte{
	0x00, 0x00, 0x00,
	0x4d, 0x54, 0x4d, 0x54, // MTMT
	0x00,
	0xc2, 0xa0, // NO-BREAK SPACE
	0x00,
}

func (u *View) Upsert(field constants.ID, b []byte) uint64 {
	if len(b) == 0 {
		b = emptyKey
	}
	baseKey := sum(b)

	// We use the same translation logic for short strings and large blobs.
	// Instead of using actual blob as part of key we use hash of it
	hash := u.sum(baseKey[:])

	// Key of the blob hash in the current view
	blobHashKey := keys.BlobHash(field, hash, u.View)
	sum := u.sum(blobHashKey)
	if v, ok := u.Tx.store.hashCache.Get(sum); ok {
		return v.(uint64)
	}
	it, err := u.Tx.db.Get(blobHashKey)
	if err != nil {
		if !errors.Is(err, badger.ErrKeyNotFound) {
			util.Exit("unexpected badger error", "err", err)
		}
		// Observability generates a large amount of data. Storing each copy per view
		// is wasteful.
		//
		// We save blobs in content addressable manner. Where content is identified
		// by hash of its content.

		id := u.Seq.NextID(field)

		err = u.Tx.blobSet(baseKey, b)
		if err != nil {
			util.Exit("writing blob data", "err", err)
		}
		u.Tx.store.hashCache.Set(sum, id, 1)
		err = u.Tx.db.Set(blobHashKey, encoding.Uint64Bytes(id))
		if err != nil {
			util.Exit("writing blob hash key", "err", err)
		}
		idKey := keys.BlobID(field, id, u.View)

		// store id => block_checksum
		err = u.Tx.db.Set(idKey, baseKey[:])
		if err != nil {
			util.Exit("writing blob id", "err", err)
		}

		// Speedup find by caching blob_checksum=> blob
		u.Tx.store.valueCache.Set(u.sum(baseKey[:]), b, int64(len(b)))
		return id
	}
	var id uint64
	err = it.Value(func(val []byte) error {
		id = encoding.Uint64(val)
		return nil
	})
	if err != nil {
		util.Exit("reading blob id", "err", err)
	}
	u.Tx.store.hashCache.Set(sum, id, 1)
	return id
}

func (u *View) sum(b []byte) uint64 {
	u.hash.Reset()
	u.hash.Write(b)
	return u.hash.Sum64()
}

func (ctx *Tx) blobGet(key Key) []byte {
	it, err := ctx.blob.Get(key[:])
	if err != nil {
		util.Exit("failed to get a blob value", "err", err)
	}
	value, err := it.ValueCopy(nil)
	if err != nil {
		util.Exit("failed to copy a blob value", "err", err)
	}
	return value
}

func (ctx *Tx) blobSet(key Key, data []byte) error {
	_, err := ctx.blob.Get(key[:])
	if err != nil {
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		err = ctx.blob.Set(key[:], data)
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

func (ctx *Tx) blobSetRef(key Key, data []byte) error {
	_, err := ctx.blob.Get(key[:])
	if err != nil {
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		err = ctx.blob.Set(key[:], bytes.Clone(data))
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

func sum(data []byte) Key {
	return sha512.Sum512_256(data)
}
