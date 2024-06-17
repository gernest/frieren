package store

import (
	"errors"
	"path/filepath"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/ristretto"
	"github.com/gernest/rbf"
)

type Value interface {
	Value(f func(val []byte) error) error
}

type Store struct {
	Path       string
	DB         *badger.DB
	Index      *rbf.DB
	Seq        *Seq
	HashCache  *ristretto.Cache
	ValueCache *ristretto.Cache
}

const hashItems = (16 << 20) / 16

func New(path string) (*Store, error) {
	dbPath := filepath.Join(path, "db")

	db, err := badger.Open(badger.DefaultOptions(dbPath).
		WithLogger(nil))
	if err != nil {
		return nil, err
	}
	idxPath := filepath.Join(path, "index")
	idx := rbf.NewDB(idxPath, nil)
	err = idx.Open()
	if err != nil {
		db.Close()
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
		return nil, err
	}

	return &Store{Path: path, DB: db, Index: idx,
		Seq: NewSequence(db), HashCache: hashCache,
		ValueCache: valueCache}, nil
}

func (s *Store) Close() error {
	s.HashCache.Close()
	s.ValueCache.Close()
	return errors.Join(
		s.Seq.Release(),
		s.Index.Close(), s.DB.Close(),
	)
}
