package store

import (
	"errors"
	"path/filepath"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/ristretto"
	"github.com/gernest/frieren/internal/constants"
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
	SeqCache   *ristretto.Cache
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
	seqCache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: constants.LastID * 10,
		MaxCost:     constants.LastID + 2,
		BufferItems: 64,
		OnEvict: func(item *ristretto.Item) {
			item.Value.(*badger.Sequence).Release()
		},
		OnReject: func(item *ristretto.Item) {
			item.Value.(*badger.Sequence).Release()
		},
		OnExit: func(val interface{}) {
			val.(*badger.Sequence).Release()
		},
	})
	if err != nil {
		idx.Close()
		db.Close()
		hashCache.Close()
		valueCache.Close()
		return nil, err
	}

	return &Store{Path: path, DB: db, Index: idx,
		Seq: NewSequence(db, seqCache), HashCache: hashCache,
		ValueCache: valueCache, SeqCache: seqCache}, nil
}

func (s *Store) Close() error {
	s.HashCache.Close()
	s.ValueCache.Close()
	s.SeqCache.Close()
	return errors.Join(
		s.Index.Close(), s.DB.Close(),
	)
}
