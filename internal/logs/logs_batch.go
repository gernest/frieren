package logs

import (
	"log/slog"
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
	db, err := dsl.New[*v1.Entry](dbPath)
	if err != nil {
		return nil, err
	}
	slog.Info("logs store ready", "shards", db.Shards().GetCardinality())
	return &Store{Store: db}, nil
}

func (s *Store) Save(ld plog.Logs) error {
	s.Append(logproto.FromLogs(ld))
	return s.Flush()
}
