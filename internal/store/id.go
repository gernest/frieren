package store

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/keys"
	"github.com/gernest/frieren/internal/util"
)

type Seq struct {
	seq map[constants.ID]*badger.Sequence
	db  *badger.DB
}

func NewSequence(db *badger.DB) *Seq {
	return &Seq{db: db, seq: make(map[constants.ID]*badger.Sequence)}
}

func (s *Seq) Release() error {
	for _, sq := range s.seq {
		sq.Release()
	}
	return nil
}

func (s *Seq) NextID(id constants.ID) uint64 {
	sq, ok := s.seq[id]
	if !ok {
		var err error
		sq, err = s.db.GetSequence((&keys.Seq{}).Key(), 1<<10)
		if err != nil {
			util.Exit("creating new sequence", "id", id, "err", err)
		}
		s.seq[id] = sq
	}
	next, err := sq.Next()
	if err != nil {
		// Sequence ID is the heart of the storage. Any failure to create new one is
		// fatal.
		util.Exit("generating sequence id", "err", err)
	}
	return next
}
