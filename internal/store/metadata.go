package store

import (
	"hash"

	"github.com/RoaringBitmap/roaring"
	"github.com/prometheus/prometheus/prompb"
	"go.etcd.io/bbolt"
)

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
