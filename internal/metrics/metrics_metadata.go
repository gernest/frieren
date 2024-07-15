package metrics

import (
	"github.com/prometheus/prometheus/model/metadata"
)

func (s *Store) Metadata(name string) (o map[string][]metadata.Metadata, err error) {
	return s.db.Metadata(name)
}
