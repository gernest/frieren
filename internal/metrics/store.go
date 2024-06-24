package metrics

import (
	"bytes"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/store"
	"github.com/prometheus/prometheus/prompb"
)

func UpsertLabels(b *store.View) LabelFunc {
	m := roaring64.New()
	var h bytes.Buffer
	return func(l []prompb.Label) []uint64 {
		m.Clear()
		for i := range l {
			h.Reset()
			h.WriteString(l[i].Name)
			h.WriteByte('=')
			h.WriteString(l[i].Value)
			id := b.Upsert(constants.MetricsLabels, bytes.Clone(h.Bytes()))
			m.Add(id)
		}
		return m.ToArray()
	}
}
