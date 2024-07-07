package traces

import (
	"os"
	"path/filepath"

	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/gernest/frieren/internal/traces/traceproto"
	"github.com/gernest/rbf/dsl"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type Store struct {
	*dsl.Store[*v1.Span]
}

func New(path string) (*Store, error) {
	dbPth := filepath.Join(path, "traces")
	os.MkdirAll(dbPth, 0755)
	db, err := dsl.New[*v1.Span](dbPth)
	if err != nil {
		return nil, err
	}
	return &Store{Store: db}, nil
}

func (s *Store) Save(td ptrace.Traces) error {
	return s.Append(traceproto.From(td))
}
