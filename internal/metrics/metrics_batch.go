package metrics

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/cespare/xxhash/v2"
	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/gernest/frieren/internal/metrics/metricsproto"
	"github.com/gernest/rbf/dsl"
	"github.com/prometheus/prometheus/prompb"
	"go.etcd.io/bbolt"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

var (
	metadataBucket = []byte("meta")
)

type Store struct {
	*dsl.Store[*v1.Sample]
	meta    *bbolt.DB
	metaSet roaring64.Bitmap
	mu      sync.RWMutex
	path    string
}

func New(path string) (*Store, error) {
	dbPath := filepath.Join(path, "metrics")
	os.MkdirAll(dbPath, 0755)
	db, err := dsl.New[*v1.Sample](dbPath, "histogram")
	if err != nil {
		return nil, err
	}
	meta, err := bbolt.Open(filepath.Join(dbPath, "META"), 0600, nil)
	if err != nil {
		db.Close()
		return nil, err
	}
	meta.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(metadataBucket)
		return err
	})
	slog.Info("metrics store ready", "shards", db.Shards().GetCardinality())
	return &Store{Store: db, path: path, meta: meta}, nil
}

func (s *Store) Close() error {
	return errors.Join(s.Store.Close(), s.meta.Close())
}

func (s *Store) Path() string {
	return s.path
}

func (s *Store) Queryable() *Queryable {
	return &Queryable{store: s}
}

func (s *Store) Save(pm pmetric.Metrics) error {
	samples, meta, err := metricsproto.From(pm)
	if err != nil {
		return err
	}
	s.Append(samples)
	return errors.Join(s.Flush(), s.saveMeta(meta))
}

func (s *Store) saveMeta(meta []*prompb.MetricMetadata) error {
	s.mu.RLock()
	set := s.metaSet.Clone()
	s.mu.RUnlock()
	// Avoid opening transaction if we have seen all metadata before
	h := xxhash.New()
	var count int
	for i, m := range meta {
		h.Reset()
		h.WriteString(m.MetricFamilyName)
		sum := h.Sum64()
		if set.Contains(sum) {
			meta[i] = nil
			count++
		} else {
			set.Add(sum)
		}
	}
	if count == len(meta) {
		return nil
	}
	defer func() {
		s.mu.Lock()
		s.metaSet.Or(set)
		s.mu.Unlock()
	}()
	return s.meta.Update(func(tx *bbolt.Tx) error {
		m := tx.Bucket(metadataBucket)
		for _, p := range meta {
			if p != nil {
				data, _ := p.Marshal()
				err := m.Put([]byte(p.MetricFamilyName), data)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}
