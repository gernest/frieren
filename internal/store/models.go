package store

import (
	"math"

	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func (b *Batch) Append(ts *prompb.TimeSeries) {
	labels, series := b.Write(func(w Labels) {
		for i := range ts.Labels {
			w.Write([]byte(ts.Labels[i].Name), []byte(ts.Labels[i].Value))
		}
	})
	b.buffer.Clear()

	for i := range ts.Exemplars {
		data, _ := ts.Exemplars[i].Marshal()
		b.buffer.Add(b.Blob(data))
	}
	ex := b.buffer.ToArray()
	for i := range ts.Samples {
		b.ids = append(b.ids, 0)
		b.lbls = append(b.lbls, labels)
		b.series = append(b.series, series)
		b.ts = append(b.ts, ts.Samples[i].Timestamp)
		b.vals = append(b.vals, math.Float64bits(ts.Samples[i].Value))

		b.exemplars = append(b.exemplars, ex)
		b.histograms = append(b.histograms, 0)
	}
	for i := range ts.Histograms {
		b.ids = append(b.ids, 0)
		b.lbls = append(b.lbls, labels)
		b.series = append(b.series, series)
		b.ts = append(b.ts, ts.Histograms[i].Timestamp)
		data, _ := ts.Histograms[i].Marshal()
		b.histograms = append(b.histograms, b.Blob(data))
		b.exemplars = append(b.exemplars, ex)
		b.vals = append(b.vals, 0)
	}
}

func (db *DB) From(m pmetric.Metrics) error {
	ts, err := prometheusremotewrite.FromMetrics(m, prometheusremotewrite.Settings{
		AddMetricSuffixes: true,
	})
	if err != nil {
		return err
	}
	b := NewBatch()
	defer b.Release()
	b.Resize(m.DataPointCount())
	for _, t := range ts {
		b.Append(t)
	}
	meta := prometheusremotewrite.OtelMetricsToMetadata(m, true)
	return db.apply(b, db.metadata(meta))
}
