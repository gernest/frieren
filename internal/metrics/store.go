package metrics

import (
	"os"
	"path/filepath"

	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/gernest/frieren/internal/metrics/metricsproto"
	"github.com/gernest/rbf/dsl"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type Store struct {
	*dsl.Store[*v1.Sample]
	path string
}

func New(path string) (*Store, error) {
	dbPath := filepath.Join(path, "metrics")
	os.MkdirAll(dbPath, 0755)
	db, err := dsl.New[*v1.Sample](dbPath)
	if err != nil {
		return nil, err
	}
	return &Store{Store: db, path: path}, nil
}

func (s *Store) Path() string {
	return s.path
}

func (s *Store) Queryable() *Queryable {
	return &Queryable{store: s}
}

func (s *Store) Save(pm pmetric.Metrics) error {
	samples, err := metricsproto.From(pm)
	if err != nil {
		return err
	}
	return s.Append(samples)
}
