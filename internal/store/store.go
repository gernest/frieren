package store

import (
	"errors"
	"path/filepath"

	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/rbf"
)

type Value interface {
	Value(f func(val []byte) error) error
}

type Store struct {
	Path    string
	DB      *badger.DB
	Index   *rbf.DB
	Seq     *Seq
	BlobSeq *Seq
}

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
		return nil, err
	}
	seq, err := NewSequence(db)
	if err != nil {
		return nil, err
	}
	blob, err := NewBlobSequence(db)
	if err != nil {
		return nil, err
	}
	return &Store{Path: path, DB: db, Index: idx, Seq: seq, BlobSeq: blob}, nil
}

func (s *Store) Close() error {
	return errors.Join(
		s.Seq.Release(), s.BlobSeq.Release(), s.Index.Close(), s.DB.Close(),
	)
}
