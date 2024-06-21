package metricsproto

import (
	"bytes"
	"encoding/hex"
	"math"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/metrics/metricsproto/prometheusremotewrite"
	"github.com/gernest/frieren/internal/px"
	"github.com/gernest/frieren/internal/util"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
	prometheustranslator "github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheus"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const (
	sumStr        = "_sum"
	countStr      = "_count"
	bucketStr     = "_bucket"
	leStr         = "le"
	quantileStr   = "quantile"
	pInfStr       = "+Inf"
	createdSuffix = "_created"
	// maxExemplarRunes is the maximum number of UTF-8 exemplar characters
	// according to the prometheus specification
	// https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#exemplars
	maxExemplarRunes = 128
	// Trace and Span id keys are defined as part of the spec:
	// https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification%2Fmetrics%2Fdatamodel.md#exemplars-2
	traceIDKey       = "trace_id"
	spanIDKey        = "span_id"
	infoType         = "info"
	targetMetricName = "target_info"
)

type SeriesMap map[uint64]*Series

func (m SeriesMap) Release() {
	for _, v := range m {
		v.Release()
	}
	clear(m)
}

func (m SeriesMap) Get(a *px.Ctx, remove ...uint64) *Series {
	id := a.ID(constants.MetricsSeries)
	s, ok := m[id]
	if !ok {
		s = newSeries(id, a.Bitmap())
		m[id] = s
	}
	a.Remove(remove...)
	return s
}

type Series struct {
	ID          uint64
	Labels      []uint64
	Exemplars   []uint64
	Timestamp   []uint64
	Values      []uint64
	Histograms  []uint64
	HistogramTS []uint64
}

func newSeries(id uint64, b *roaring64.Bitmap) *Series {
	return seriesPool.Get().(*Series).Reset(id, b)
}

var seriesPool = &sync.Pool{New: func() any { return new(Series) }}

func (s *Series) Release() {
	s.Reset(0, nil)
	seriesPool.Put(s)
}

func (s *Series) Reset(id uint64, b *roaring64.Bitmap) *Series {
	s.ID = id
	s.Labels = s.Labels[:0]
	s.Exemplars = s.Exemplars[:0]
	s.Timestamp = s.Timestamp[:0]
	s.Values = s.Values[:0]
	s.Histograms = s.Histograms[:0]
	s.HistogramTS = s.HistogramTS[:0]
	if b != nil {
		s.Labels = slices.Grow(s.Labels, int(b.GetCardinality()))
		it := b.Iterator()
		for it.HasNext() {
			s.Labels = append(s.Labels, it.Next())
		}
	}
	return s
}

func (s *Series) Add(ts int64, v float64, flags pmetric.DataPointFlags) {
	s.Timestamp = append(s.Timestamp, uint64(ts))
	if flags.NoRecordedValue() {
		s.Values = append(s.Values, value.StaleNaN)
	} else {
		s.Values = append(s.Values, math.Float64bits(v))
	}
}

func From(md pmetric.Metrics, tr blob.Func) SeriesMap {
	if md.DataPointCount() == 0 {
		return nil
	}
	rm := md.ResourceMetrics()
	series := make(SeriesMap)
	rsCtx := px.New(constants.MetricsLabels, tr)
	scopeCtx := px.New(constants.MetricsLabels, tr)
	attrCtx := px.New(constants.MetricsLabels, tr)

	for i := 0; i < rm.Len(); i++ {
		sm := rm.At(i).ScopeMetrics()
		res := rm.At(i).Resource()
		rsCtx.Reset()
		rsCtx.Resource(res)

		for j := 0; j < sm.Len(); j++ {
			scope := sm.At(j).Scope()
			metrics := sm.At(j).Metrics()
			scopeCtx.Reset()
			scope.Attributes().Range(scopeCtx.SetProm)
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				name := prometheustranslator.BuildCompliantName(metric, "", true)
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					dataPoints := metric.Gauge().DataPoints()
					if dataPoints.Len() == 0 {
						break
					}
					addGaugeNumberDataPoints(name, dataPoints, rsCtx, scopeCtx, attrCtx, series)
				case pmetric.MetricTypeSum:
					dataPoints := metric.Sum().DataPoints()
					if dataPoints.Len() == 0 {
						break
					}
					addSumNumberDataPoints(name, tr, dataPoints, rsCtx, scopeCtx, attrCtx, series)
				case pmetric.MetricTypeHistogram:
					dataPoints := metric.Histogram().DataPoints()
					if dataPoints.Len() == 0 {
						break
					}
					addHistogramDataPoints(name, tr, dataPoints, rsCtx, scopeCtx, attrCtx, series)
				case pmetric.MetricTypeExponentialHistogram:
					dataPoints := metric.ExponentialHistogram().DataPoints()
					if dataPoints.Len() == 0 {
						break
					}
					addExponentialHistogramDataPoints(name, tr, dataPoints, rsCtx, scopeCtx, attrCtx, series)
				case pmetric.MetricTypeSummary:
					dataPoints := metric.Summary().DataPoints()
					if dataPoints.Len() == 0 {
						break
					}
					addSummaryDataPoints(name, dataPoints, rsCtx, scopeCtx, attrCtx, series)
				default:
				}

			}
		}
	}
	return series

}

func addGaugeNumberDataPoints(name string,
	data pmetric.NumberDataPointSlice,
	resource, scope, attr *px.Ctx, series SeriesMap) {
	nameID := attr.Set(model.MetricNameLabel, name)
	for x := 0; x < data.Len(); x++ {
		point := data.At(x)
		attr.Reset()
		attr.Or(resource)
		attr.Or(scope)
		attr.Add(nameID)
		point.Attributes().Range(attr.SetProm)
		ts := convertTimeStamp(point.Timestamp())
		var value float64
		switch point.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			value = float64(point.IntValue())
		case pmetric.NumberDataPointValueTypeDouble:
			value = point.DoubleValue()
		}
		sample := series.Get(attr)
		sample.Add(ts, value, point.Flags())
	}
}

func addSumNumberDataPoints(name string,
	tr blob.Func,
	data pmetric.NumberDataPointSlice,
	resource, scope, attr *px.Ctx, series SeriesMap) {
	nameID := attr.Set(model.MetricNameLabel, name)
	for x := 0; x < data.Len(); x++ {
		point := data.At(x)
		attr.Reset()
		attr.Or(resource)
		attr.Or(scope)
		attr.Add(nameID)
		point.Attributes().Range(attr.SetProm)
		ts := convertTimeStamp(point.Timestamp())
		var value float64
		switch point.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			value = float64(point.IntValue())
		case pmetric.NumberDataPointValueTypeDouble:
			value = point.DoubleValue()
		}
		sample := series.Get(attr)
		sample.Add(ts, value, point.Flags())
		ex := point.Exemplars().Len()
		if ex > 0 {
			sample.Exemplars = slices.Grow(sample.Exemplars, ex)
			getPromExemplars(point, tr, func(_ *prompb.Exemplar, u uint64) {
				sample.Exemplars = append(sample.Exemplars, u)
			})
		}
	}
}

func addHistogramDataPoints(name string,
	tr blob.Func,
	data pmetric.HistogramDataPointSlice,
	resource, scope, attr *px.Ctx, series SeriesMap) {
	sumID := attr.Set(model.MetricNameLabel, name+sumStr)
	countID := attr.Set(model.MetricNameLabel, name+countStr)
	bucketID := attr.Set(model.MetricNameLabel, name+bucketStr)

	for x := 0; x < data.Len(); x++ {
		point := data.At(x)
		attr.Reset()
		attr.Or(resource)
		attr.Or(scope)
		point.Attributes().Range(attr.SetProm)
		ts := convertTimeStamp(point.Timestamp())
		if point.HasSum() {
			attr.Add(sumID)
			sample := series.Get(attr, sumID)
			sample.Add(ts, point.Sum(), point.Flags())
		}
		attr.Add(countID)
		sample := series.Get(attr, countID)
		sample.Add(ts, float64(point.Count()), point.Flags())

		// cumulative count for conversion to cumulative histogram
		var cumulativeCount uint64

		var bucketBounds []bucketBoundsData

		// process each bound, based on histograms proto definition, # of buckets = # of explicit bounds + 1
		for i := 0; i < point.ExplicitBounds().Len() && i < point.BucketCounts().Len(); i++ {
			bound := point.ExplicitBounds().At(i)
			cumulativeCount += point.BucketCounts().At(i)
			attr.Add(bucketID)
			boundStr := strconv.FormatFloat(bound, 'f', -1, 64)
			leID := attr.Set(leStr, boundStr)

			bs := series.Get(attr, bucketID, leID)
			bs.Add(ts, float64(cumulativeCount), point.Flags())

			bucketBounds = append(bucketBounds, bucketBoundsData{ts: bs, bound: bound})
		}

		attr.Add(bucketID)
		attr.Set(leStr, pInfStr)
		is := series.Get(attr)
		is.Add(ts, float64(point.Count()), point.Flags())

		bucketBounds = append(bucketBounds, bucketBoundsData{ts: is, bound: math.Inf(1)})
		sort.Sort(byBucketBoundsData(bucketBounds))
		getPromExemplars(point, tr, func(e *prompb.Exemplar, u uint64) {
			for _, bound := range bucketBounds {
				if len(bound.ts.Values) > 0 && e.Value <= bound.bound {
					bound.ts.Exemplars = append(bound.ts.Exemplars, u)
					break
				}
			}
		})
	}
}

type bucketBoundsData struct {
	ts    *Series
	bound float64
}

// byBucketBoundsData enables the usage of sort.Sort() with a slice of bucket bounds
type byBucketBoundsData []bucketBoundsData

func (m byBucketBoundsData) Len() int           { return len(m) }
func (m byBucketBoundsData) Less(i, j int) bool { return m[i].bound < m[j].bound }
func (m byBucketBoundsData) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }

func addExponentialHistogramDataPoints(name string,
	tr blob.Func,
	data pmetric.ExponentialHistogramDataPointSlice,
	resource, scope, attr *px.Ctx, series SeriesMap) error {
	nameID := attr.Set(model.MetricNameLabel, name)
	var buf []byte
	for x := 0; x < data.Len(); x++ {
		point := data.At(x)
		attr.Reset()
		attr.Or(resource)
		attr.Or(scope)
		attr.Add(nameID)
		point.Attributes().Range(attr.SetProm)
		hs, err := prometheusremotewrite.ExponentialToNativeHistogram(point)
		if err != nil {
			return err
		}
		size := hs.Size()
		buf = slices.Grow(buf, size)[:size]
		hs.MarshalToSizedBuffer(buf)
		id := tr(constants.MetricsHistogram, bytes.Clone(buf))

		sample := series.Get(attr)
		sample.Histograms = append(sample.Histograms, id)
		sample.HistogramTS = append(sample.HistogramTS, uint64(hs.Timestamp))
	}
	return nil
}

func addSummaryDataPoints(name string,
	data pmetric.SummaryDataPointSlice,
	resource, scope, attr *px.Ctx, series SeriesMap) {
	sumID := attr.Set(model.MetricNameLabel, name+sumStr)
	countID := attr.Set(model.MetricNameLabel, name+countStr)
	for x := 0; x < data.Len(); x++ {
		point := data.At(x)
		attr.Reset()
		attr.Or(resource)
		attr.Or(scope)
		point.Attributes().Range(attr.SetProm)
		ts := convertTimeStamp(point.Timestamp())

		ss := series.Get(attr, sumID)
		ss.Add(ts, point.Sum(), point.Flags())

		attr.Add(countID)
		sc := series.Get(attr, countID)
		sc.Add(ts, float64(point.Count()), point.Flags())

		for i := 0; i < point.QuantileValues().Len(); i++ {
			qt := point.QuantileValues().At(i)
			percentileStr := strconv.FormatFloat(qt.Quantile(), 'f', -1, 64)
			pid := attr.Set(quantileStr, percentileStr)
			ps := series.Get(attr, pid)
			ps.Add(ts, qt.Value(), point.Flags())
		}
	}
}

type exemplarType interface {
	pmetric.ExponentialHistogramDataPoint | pmetric.HistogramDataPoint | pmetric.NumberDataPoint
	Exemplars() pmetric.ExemplarSlice
}

func getPromExemplars[T exemplarType](pt T, tr blob.Func, f ...func(*prompb.Exemplar, uint64)) {
	promExemplar := &prompb.Exemplar{}
	var buf []byte
	for i := 0; i < pt.Exemplars().Len(); i++ {
		exemplar := pt.Exemplars().At(i)
		exemplarRunes := 0

		promExemplar.Reset()
		promExemplar.Value = exemplar.DoubleValue()
		promExemplar.Timestamp = timestamp.FromTime(exemplar.Timestamp().AsTime())

		if traceID := exemplar.TraceID(); !traceID.IsEmpty() {
			val := hex.EncodeToString(traceID[:])
			exemplarRunes += utf8.RuneCountInString(traceIDKey) + utf8.RuneCountInString(val)
			promLabel := prompb.Label{
				Name:  traceIDKey,
				Value: val,
			}
			promExemplar.Labels = append(promExemplar.Labels, promLabel)
		}
		if spanID := exemplar.SpanID(); !spanID.IsEmpty() {
			val := hex.EncodeToString(spanID[:])
			exemplarRunes += utf8.RuneCountInString(spanIDKey) + utf8.RuneCountInString(val)
			promLabel := prompb.Label{
				Name:  spanIDKey,
				Value: val,
			}
			promExemplar.Labels = append(promExemplar.Labels, promLabel)
		}

		attrs := exemplar.FilteredAttributes()
		labelsFromAttributes := make([]prompb.Label, 0, attrs.Len())
		attrs.Range(func(key string, value pcommon.Value) bool {
			val := value.AsString()
			exemplarRunes += utf8.RuneCountInString(key) + utf8.RuneCountInString(val)
			promLabel := prompb.Label{
				Name:  key,
				Value: val,
			}

			labelsFromAttributes = append(labelsFromAttributes, promLabel)

			return true
		})
		if exemplarRunes <= maxExemplarRunes {
			// only append filtered attributes if it does not cause exemplar
			// labels to exceed the max number of runes
			promExemplar.Labels = append(promExemplar.Labels, labelsFromAttributes...)
		}
		size := promExemplar.Size()
		buf = slices.Grow(buf, size)[:size]
		promExemplar.MarshalToSizedBuffer(buf)
		v := tr(constants.MetricsExemplars, bytes.Clone(buf))
		if len(f) > 0 {
			f[0](promExemplar, v)
		}
	}
}

func convertTimeStamp(timestamp pcommon.Timestamp) int64 {
	return timestamp.AsTime().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
}

func Sample() pmetric.Metrics {
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
	return d
}
