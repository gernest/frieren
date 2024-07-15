package store

import (
	"hash"
	"strings"

	"github.com/RoaringBitmap/roaring"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/prompb"
	"go.etcd.io/bbolt"
)

func (db *DB) Metadata(name string) (result map[string][]metadata.Metadata, err error) {
	result = make(map[string][]metadata.Metadata)
	err = db.db.View(func(tx *bbolt.Tx) error {
		m := tx.Bucket(metaBucket)
		if m == nil {
			return nil
		}
		var b prompb.MetricMetadata

		if name != "" {
			data := m.Get([]byte(name))
			if len(data) == 0 {
				return nil
			}
			err := b.Unmarshal(data)
			if err != nil {
				return err
			}
			result[name] = []metadata.Metadata{protoToMeta(&b)}
			return nil
		}
		return m.ForEach(func(k, v []byte) error {
			b.Reset()
			err := b.Unmarshal(v)
			if err != nil {
				return err
			}
			result[string(k)] = []metadata.Metadata{
				protoToMeta(&b),
			}
			return nil
		})
	})
	return
}

func protoToMeta(m *prompb.MetricMetadata) metadata.Metadata {
	return metadata.Metadata{
		Type: model.MetricType(strings.ToLower(m.Type.String())),
		Unit: m.Unit,
		Help: m.Help,
	}
}

func (db *DB) metadata(meta []*prompb.MetricMetadata) func(tx *bbolt.Tx) error {
	return func(tx *bbolt.Tx) error {
		m, err := bucket(tx, metaBucket)
		if err != nil {
			return err
		}
		r := roaring.New()
		for i := range meta {
			hash := Hash32(func(h hash.Hash32) {
				h.Write([]byte(meta[i].MetricFamilyName))
			})
			if db.meta.Contains(hash) {
				continue
			}
			r.Add(hash)
			data, _ := meta[i].Marshal()
			err = m.Put([]byte(meta[i].MetricFamilyName), data)
			if err != nil {
				return err
			}
		}
		db.meta.Or(r)
		return nil
	}
}
