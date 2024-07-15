package metrics

import (
	"os"
	"path/filepath"

	"github.com/gernest/frieren/internal/store"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type Store struct {
	db *store.DB
}

func New(path string) (*Store, error) {
	dbPath := filepath.Join(path, "metrics")
	os.MkdirAll(dbPath, 0755)
	db, err := store.New(dbPath)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Queryable() *Queryable {
	return &Queryable{store: s}
}

func (s *Store) Save(pm pmetric.Metrics) error {
	return s.db.Save(pm)
}
