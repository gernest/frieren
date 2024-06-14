package metrics

import (
	"bytes"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/keys"
	"github.com/gernest/frieren/internal/store"
	"github.com/prometheus/prometheus/prompb"
)

func StoreMetadata(txn *badger.Txn, meta []*prompb.MetricMetadata) error {
	buf := new(bytes.Buffer)
	for _, m := range meta {
		key := keys.Metadata(buf, m.MetricFamilyName)
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
	p := &prompb.MetricMetadata{}
	err := store.Get(txn, keys.Metadata(new(bytes.Buffer), name), p.Unmarshal)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func ListMetadata(txn *badger.Txn) (o []*prompb.MetricMetadata, err error) {
	err = store.Prefix(txn, keys.Metadata(new(bytes.Buffer), ""), func(key []byte, value store.Value) error {
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
