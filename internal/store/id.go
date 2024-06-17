package store

import (
	"bytes"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/ristretto"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/keys"
	"github.com/gernest/frieren/internal/util"
)

type Sequence struct {
	seq  *Seq
	buf  bytes.Buffer
	view string
	ids  map[constants.ID]*badger.Sequence
}

type Seq struct {
	cache *ristretto.Cache
	db    *badger.DB
}

func (s *Seq) Sequence(view string) *Sequence {
	return &Sequence{
		seq:  s,
		view: view,
		ids:  make(map[constants.ID]*badger.Sequence),
	}
}

func (s *Sequence) Release() {
	clear(s.ids)
}

func NewSequence(db *badger.DB, cache *ristretto.Cache) *Seq {
	return &Seq{db: db, cache: cache}
}

func (s *Sequence) NextID(id constants.ID) uint64 {
	sq, ok := s.ids[id]
	if !ok {
		key := keys.Seq(&s.buf, id, s.view)
		hash := xxhash.Sum64(key)
		if v, ok := s.seq.cache.Get(hash); ok {
			sq = v.(*badger.Sequence)
			s.ids[id] = sq
		} else {
			var err error
			sq, err = s.seq.db.GetSequence(key, 4<<10)
			if err != nil {
				util.Exit("creating new sequence", "id", id, "err", err)
			}
			s.ids[id] = sq
			s.seq.cache.SetWithTTL(hash, sq, 1, 24*time.Hour)
		}
	}
	next, err := sq.Next()
	if err != nil {
		// Sequence ID is the heart of the storage. Any failure to create new one is
		// fatal.
		util.Exit("generating sequence id", "err", err)
	}
	return next
}
