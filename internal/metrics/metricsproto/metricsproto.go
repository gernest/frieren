package metricsproto

import (
	"bytes"

	"github.com/cespare/xxhash/v2"
	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

var eq = []byte("=")

func From(md pmetric.Metrics) ([]*v1.Sample, []*prompb.MetricMetadata, error) {
	s, err := prometheusremotewrite.FromMetrics(md, prometheusremotewrite.Settings{
		AddMetricSuffixes: true,
	})
	if err != nil {
		return nil, nil, err
	}
	size := md.DataPointCount()
	o := make([]*v1.Sample, 0, size)
	for i := range s {
		o = fromTS(s[i], o)
	}
	meta := prometheusremotewrite.OtelMetricsToMetadata(md, true)
	return o, meta, nil
}

func fromTS(ts *prompb.TimeSeries, o []*v1.Sample) []*v1.Sample {
	series := xxhash.New()
	var b bytes.Buffer
	labels := make([]string, len(ts.Labels))
	for i := range ts.Labels {
		b.Reset()
		b.WriteString(ts.Labels[i].Name)
		b.Write(eq)
		b.WriteString(ts.Labels[i].Value)
		labels[i] = b.String()
		series.Write(b.Bytes())
	}
	id := series.Sum(nil)
	var exemplars []byte
	if len(ts.Exemplars) > 0 {
		exemplars, _ = (&prompb.TimeSeries{Exemplars: ts.Exemplars}).Marshal()
	}
	if len(ts.Samples) > 0 {
		for i := range ts.Samples {
			o = append(o, &v1.Sample{
				Series:    id,
				Labels:    labels,
				Exemplars: exemplars,
				Kind:      v1.Sample_FLOAT,
				Value:     ts.Samples[i].Value,
				Timestamp: ts.Samples[i].Timestamp,
			})
		}
	}
	if len(ts.Histograms) > 0 {
		for i := range ts.Histograms {
			data, _ := ts.Histograms[i].Marshal()
			o = append(o, &v1.Sample{
				Series:    id,
				Kind:      v1.Sample_HISTOGRAM,
				Labels:    labels,
				Exemplars: exemplars,
				Histogram: data,
				Timestamp: ts.Histograms[i].Timestamp,
			})
		}
	}
	return o
}
