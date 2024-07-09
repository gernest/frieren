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
	db, err := dsl.New(dbPth,
		dsl.WithTimestampField[*v1.Span]("span_start_nano"),
		dsl.WithSkip[*v1.Span](
			"link_span_id", "link_trace_id",
			"parent_id", "span_id", "trace_id",
		),
	)
	if err != nil {
		return nil, err
	}
	return &Store{Store: db}, nil
}

func (s *Store) Save(td ptrace.Traces) error {
	return s.Append(traceproto.From(td))
}
