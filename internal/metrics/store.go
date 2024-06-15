package metrics

import (
	"bytes"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/prometheus/prometheus/prompb"
)

func UpsertLabels(b blob.Func) LabelFunc {
	m := roaring64.New()
	var h bytes.Buffer
	return func(l []prompb.Label) []uint64 {
		m.Clear()
		for i := range l {
			h.Reset()
			h.WriteString(l[i].Name)
			h.WriteByte('=')
			h.WriteString(l[i].Value)
			id := b(constants.MetricsLabels, bytes.Clone(h.Bytes()))
			m.Add(id)
		}
		return m.ToArray()
	}
}
