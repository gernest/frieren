package store

import (
	"bytes"
	"errors"
	"math"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/keys"
	"github.com/gernest/frieren/internal/shardwidth"
	"github.com/gernest/frieren/internal/util"
)

type Seq struct {
	db *badger.DB

	// avoid releasing sequences right away, in case there are still samples being
	// ingested concurrently.
	//
	// Views change in 24 hours. It is a safe delay and makes maintenance easier
	purge *Sequence
	seq   *Sequence
	view  string
	mu    sync.Mutex
}

func (s *Seq) Sequence(view string) *Sequence {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.view == view {
		return s.seq
	}
	s.purge.Release()
	s.purge = s.seq
	s.seq = &Sequence{
		db:   s.db,
		view: view,
		ids:  make(map[constants.ID]*badger.Sequence),
	}
	s.view = view
	return s.seq
}

func (s *Seq) Release() error {
	return errors.Join(s.purge.Release(), s.seq.Release())
}

func NewSequence(db *badger.DB) *Seq {
	return &Seq{db: db}
}

type Sequence struct {
	db   *badger.DB
	view string
	ids  map[constants.ID]*badger.Sequence
	mu   sync.RWMutex
}

func (s *Sequence) Release() (lastErr error) {
	if s == nil {
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, v := range s.ids {
		err := v.Release()
		if err != nil {
			lastErr = err
		}
	}
	clear(s.ids)
	return
}

const upperLimit = uint64(math.MaxUint64 / shardwidth.ShardWidth)

func (s *Sequence) NextID(id constants.ID) uint64 {
	s.mu.RLock()
	sq, ok := s.ids[id]
	s.mu.RUnlock()
	if !ok {
		s.mu.Lock()
		key := keys.Seq(new(bytes.Buffer), id, s.view)
		var err error
		sq, err = s.db.GetSequence(key, 1<<10)
		if err != nil {
			util.Exit("getting sequence", "key", string(key), "err", err)
		}
		s.ids[id] = sq
		s.mu.Unlock()
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
