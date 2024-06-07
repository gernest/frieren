package ernestdb

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

func TestBach_Append(t *testing.T) {
	db, err := NewStore(t.TempDir())
	require.NoError(t, err)
	defer db.Close()

	batch := NewBatch()
	ts := generateMetrics(t)
	err = AppendBatch(db, batch, ts)
	require.NoError(t, err)
}

func generateMetrics(t *testing.T) map[string]*prompb.TimeSeries {
	t.Helper()
	o := generateOTLPWriteRequest(t)
	m, err := prometheusremotewrite.FromMetrics(o.Metrics(), prometheusremotewrite.Settings{})
	require.NoError(t, err)
	return m
}

func generateOTLPWriteRequest(t *testing.T) pmetricotlp.ExportRequest {
	d := pmetric.NewMetrics()

	// Generate One Counter, One Gauge, One Histogram, One Exponential-Histogram
	// with resource attributes: service.name="test-service", service.instance.id="test-instance", host.name="test-host"
	// with metric attribute: foo.bar="baz"

	timestamp := time.Now()

	resourceMetric := d.ResourceMetrics().AppendEmpty()
	resourceMetric.Resource().Attributes().PutStr("service.name", "test-service")
	resourceMetric.Resource().Attributes().PutStr("service.instance.id", "test-instance")
	resourceMetric.Resource().Attributes().PutStr("host.name", "test-host")

	scopeMetric := resourceMetric.ScopeMetrics().AppendEmpty()

	// Generate One Counter
	counterMetric := scopeMetric.Metrics().AppendEmpty()
	counterMetric.SetName("test-counter")
	counterMetric.SetDescription("test-counter-description")
	counterMetric.SetEmptySum()
	counterMetric.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	counterMetric.Sum().SetIsMonotonic(true)

	counterDataPoint := counterMetric.Sum().DataPoints().AppendEmpty()
	counterDataPoint.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	counterDataPoint.SetDoubleValue(10.0)
	counterDataPoint.Attributes().PutStr("foo.bar", "baz")

	counterExemplar := counterDataPoint.Exemplars().AppendEmpty()
	counterExemplar.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	counterExemplar.SetDoubleValue(10.0)
	counterExemplar.SetSpanID(pcommon.SpanID{0, 1, 2, 3, 4, 5, 6, 7})
	counterExemplar.SetTraceID(pcommon.TraceID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})

	// Generate One Gauge
	gaugeMetric := scopeMetric.Metrics().AppendEmpty()
	gaugeMetric.SetName("test-gauge")
	gaugeMetric.SetDescription("test-gauge-description")
	gaugeMetric.SetEmptyGauge()

	gaugeDataPoint := gaugeMetric.Gauge().DataPoints().AppendEmpty()
	gaugeDataPoint.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	gaugeDataPoint.SetDoubleValue(10.0)
	gaugeDataPoint.Attributes().PutStr("foo.bar", "baz")

	// Generate One Histogram
	histogramMetric := scopeMetric.Metrics().AppendEmpty()
	histogramMetric.SetName("test-histogram")
	histogramMetric.SetDescription("test-histogram-description")
	histogramMetric.SetEmptyHistogram()
	histogramMetric.Histogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	histogramDataPoint := histogramMetric.Histogram().DataPoints().AppendEmpty()
	histogramDataPoint.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	histogramDataPoint.ExplicitBounds().FromRaw([]float64{0.0, 1.0, 2.0, 3.0, 4.0, 5.0})
	histogramDataPoint.BucketCounts().FromRaw([]uint64{2, 2, 2, 2, 2, 2})
	histogramDataPoint.SetCount(10)
	histogramDataPoint.SetSum(30.0)
	histogramDataPoint.Attributes().PutStr("foo.bar", "baz")

	// Generate One Exponential-Histogram
	exponentialHistogramMetric := scopeMetric.Metrics().AppendEmpty()
	exponentialHistogramMetric.SetName("test-exponential-histogram")
	exponentialHistogramMetric.SetDescription("test-exponential-histogram-description")
	exponentialHistogramMetric.SetEmptyExponentialHistogram()
	exponentialHistogramMetric.ExponentialHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	exponentialHistogramDataPoint := exponentialHistogramMetric.ExponentialHistogram().DataPoints().AppendEmpty()
	exponentialHistogramDataPoint.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	exponentialHistogramDataPoint.SetScale(2.0)
	exponentialHistogramDataPoint.Positive().BucketCounts().FromRaw([]uint64{2, 2, 2, 2, 2})
	exponentialHistogramDataPoint.SetZeroCount(2)
	exponentialHistogramDataPoint.SetCount(10)
	exponentialHistogramDataPoint.SetSum(30.0)
	exponentialHistogramDataPoint.Attributes().PutStr("foo.bar", "baz")

	return pmetricotlp.NewExportRequestFromMetrics(d)
}
