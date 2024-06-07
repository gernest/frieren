package metrics

import (
	"bytes"
	"fmt"

	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/keys"
	"github.com/gernest/frieren/internal/store"
	"github.com/prometheus/prometheus/prompb"
)

func StoreMetadata(txn *badger.Txn, meta []*prompb.MetricMetadata) error {
	slice := (&keys.Metadata{}).Slice()
	key := make([]byte, 0, len(slice)*8)
	var h xxhash.Digest
	for _, m := range meta {
		h.Reset()
		h.WriteString(m.MetricFamilyName)
		slice[len(slice)-1] = h.Sum64()
		key = keys.Encode(key, slice)
		if store.Has(txn, key) {
			continue
		}
		data, err := m.Marshal()
		if err != nil {
			return fmt.Errorf("marshal metadata %w", err)
		}
		err = txn.Set(bytes.Clone(key), data)
		if err != nil {
			return fmt.Errorf("saving metadata %w", err)
		}
	}
	return nil
}

func GetMetadata(txn *badger.Txn, name string) (*prompb.MetricMetadata, error) {
	var p prompb.MetricMetadata
	key := (&keys.Metadata{MetricID: xxhash.Sum64String(name)}).Key()
	err := store.Get(txn, key, p.Unmarshal)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func ListMetadata(txn *badger.Txn) (o []*prompb.MetricMetadata, err error) {
	key := (&keys.Metadata{}).Key()
	err = store.Prefix(txn, key[:len(key)-8], func(key []byte, value store.Value) error {
		var p prompb.MetricMetadata
		err := value.Value(p.Unmarshal)
		if err != nil {
			return err
		}
		o = append(o, &p)
		return nil
	})
	return
}
