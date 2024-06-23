package metrics

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/fields"
	"github.com/gernest/frieren/internal/query"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/frieren/internal/util"
	"github.com/gernest/rbf/short_txkey"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

func TestBach_Append(t *testing.T) {
	db, err := store.New(t.TempDir())
	require.NoError(t, err)
	defer db.Close()

	m := generateOTLPWriteRequest()
	err = Append(context.TODO(), db, m.Metrics(), util.TS())
	require.NoError(t, err)
	tx, err := db.Index.Begin(false)
	require.NoError(t, err)
	defer tx.Rollback()

	t.Run("check views", func(t *testing.T) {
		want := []short_txkey.FieldView{
			{Field: "1", View: "_20060102"},
			{Field: "2", View: "_20060102"},
			{Field: "3", View: "_20060102"},
			{Field: "3", View: "_20060102_exists"},
			{Field: "4", View: "_20060102"},
			{Field: "5", View: "_20060102"},
			{Field: "6", View: "_20060102"},
			{Field: "6", View: "_20060102_exists"},
		}
		vs, err := tx.GetSortedFieldViewList()
		require.NoError(t, err)
		require.Equal(t, want, vs)
	})
	t.Run("series", func(t *testing.T) {
		fra := fields.New(constants.MetricsSeries, 0, "_20060102")
		all, err := fra.TransposeBSI(tx, nil)
		require.NoError(t, err)
		want := []uint64{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc}
		require.Equal(t, want, all.ToArray())
	})
	t.Run("views", func(t *testing.T) {
		var views []string
		var shards []uint64
		var depth []map[uint64]uint64
		err := db.DB.View(func(txn *badger.Txn) error {
			view, err := query.New(txn, tx, constants.METRICS, util.TS(), util.TS())
			if err != nil {
				return err
			}
			return view.Traverse(func(shard *v1.Shard, view string) error {
				views = append(views, view)
				shards = append(shards, shard.Id)
				depth = append(depth, shard.BitDepth)
				return nil
			})
		})
		require.NoError(t, err)
		require.Equal(t, []uint64{0}, shards)
		require.Equal(t, []string{"_20060102"}, views)
		require.Equal(t, []map[uint64]uint64{
			{1: 63, 2: 41, 5: 4},
		}, depth)
	})
}

func BenchmarkAppend(b *testing.B) {
	db, err := store.New(b.TempDir())
	require.NoError(b, err)
	defer db.Close()

	m := generateOTLPWriteRequest()

	ctx := context.TODO()
	b.ResetTimer()
	for range b.N {
		Append(ctx, db, m.Metrics(), util.TS())
	}
}

func generateOTLPWriteRequest() pmetricotlp.ExportRequest {
	d := pmetric.NewMetrics()

	// Generate One Counter, One Gauge, One Histogram, One Exponential-Histogram
	// with resource attributes: service.name="test-service", service.instance.id="test-instance", host.name="test-host"
	// with metric attribute: foo.bar="baz"

	timestamp := util.TS()

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
