package ernestdb

import (
	"bytes"
	"fmt"

	"github.com/cespare/xxhash/v2"
	"github.com/gernest/ernestdb/keys"
	"github.com/prometheus/prometheus/prompb"
)

func StoreMetadata(db Store, meta []*prompb.MetricMetadata) error {
	slice := (&keys.Metadata{}).Slice()
	key := make([]byte, 0, len(slice)*8)
	var h xxhash.Digest
	for _, m := range meta {
		h.Reset()
		h.WriteString(m.MetricFamilyName)
		slice[len(slice)-1] = h.Sum64()
		key = keys.Encode(key, slice)
		if db.Has(key) {
			continue
		}
		data, err := m.Marshal()
		if err != nil {
			return fmt.Errorf("marshal metadata %w", err)
		}
		err = db.Set(bytes.Clone(key), data)
		if err != nil {
			return fmt.Errorf("saving metadata %w", err)
		}
	}
	return nil
}
