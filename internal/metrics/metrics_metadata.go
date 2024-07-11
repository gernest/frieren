package metrics

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/prompb"
	"go.etcd.io/bbolt"
)

func (s *Store) Metadata(name string) (o map[string][]metadata.Metadata, err error) {
	o = make(map[string][]metadata.Metadata)
	err = s.meta.View(func(tx *bbolt.Tx) error {
		var ms prompb.MetricMetadata
		b := tx.Bucket(metadataBucket)
		if name != "" {
			data := b.Get([]byte(name))
			if len(data) > 0 {
				ms.Unmarshal(data)
				o[name] = []metadata.Metadata{protoToMeta(&ms)}
			}
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			ms.Reset()
			ms.Unmarshal(v)
			o[string(k)] = []metadata.Metadata{protoToMeta(&ms)}
			return nil
		})
	})
	return
}

func protoToMeta(m *prompb.MetricMetadata) metadata.Metadata {
	return metadata.Metadata{
		Type: model.MetricType(m.Type),
		Unit: m.Unit,
		Help: m.Help,
	}
}
