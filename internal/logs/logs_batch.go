package logs

import (
	"os"
	"path/filepath"

	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/gernest/frieren/internal/logs/logproto"
	"github.com/gernest/rbf/dsl"
	"go.opentelemetry.io/collector/pdata/plog"
)

type Store struct {
	*dsl.Store[*v1.Entry]
}

func New(path string) (*Store, error) {
	dbPath := filepath.Join(path, "logs")
	os.MkdirAll(dbPath, 0755)
	db, err := dsl.New(
		dbPath,
		dsl.WithSkip[*v1.Entry]("metadata"),
	)
	if err != nil {
		return nil, err
	}
	return &Store{Store: db}, nil
}

func (s *Store) Save(ld plog.Logs) error {
	return s.Append(logproto.FromLogs(ld))
}
