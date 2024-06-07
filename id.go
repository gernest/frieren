package ernestdb

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/ernestdb/keys"
)

type Seq struct {
	seq *badger.Sequence
}

func NewSequence(db *badger.DB) (*Seq, error) {
	seq, err := db.GetSequence(keys.Seq{}.Key(), 16<<10)
	if err != nil {
		return nil, err
	}
	return &Seq{seq: seq}, nil
}

func (s *Seq) Release() error {
	return s.seq.Release()
}

func (s *Seq) NextID() uint64 {
	id, err := s.seq.Next()
	if err != nil {
		// Sequence ID is the heart of the storage. Any failure to create new one is
		// fatal.
		exit("generating sequence id", "err", err)
	}
	return id
}