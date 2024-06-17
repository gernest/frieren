package store

import (
	"bytes"
	"math"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/ristretto"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/keys"
	"github.com/gernest/frieren/internal/shardwidth"
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
	mu    sync.Mutex
}

func (s *Seq) Sequence(view string) *Sequence {
	return &Sequence{
		seq:  s,
		view: view,
		ids:  make(map[constants.ID]*badger.Sequence),
	}
}

func (s *Seq) Get(key []byte, f func(seq *badger.Sequence) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	seq, err := s.db.GetSequence(key, 4<<10)
	if err != nil {
		return err
	}
	return f(seq)
}

func (s *Sequence) Release() {
	clear(s.ids)
}

func NewSequence(db *badger.DB, cache *ristretto.Cache) *Seq {
	return &Seq{db: db, cache: cache}
}

const upperLimit = uint64(math.MaxUint64 / shardwidth.ShardWidth)

func (s *Sequence) NextID(id constants.ID) uint64 {
	sq, ok := s.ids[id]
	if !ok {
		key := keys.Seq(&s.buf, id, s.view)
		hash := xxhash.Sum64(key)
		if v, ok := s.seq.cache.Get(hash); ok {
			sq = v.(*badger.Sequence)
		} else {
			s.seq.Get(key, func(seq *badger.Sequence) error {
				sq = seq
				s.seq.cache.SetWithTTL(hash, sq, 1, 24*time.Hour)
				return nil
			})
		}
		s.ids[id] = sq

	}
	next, err := sq.Next()
	if err != nil {
		// Sequence ID is the heart of the storage. Any failure to create new one is
		// fatal.
		util.Exit("generating sequence id", "err", err)
	}
	if next > upperLimit {
		util.Exit("exceeded upper limit for cardinality", "id", id, "limit", upperLimit)
	}
	return next
}
