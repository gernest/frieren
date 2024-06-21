package metricsproto

import (
	"bytes"
	"encoding/hex"
	"math"
	"slices"
	"sort"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/metrics/metricsproto/prometheusremotewrite"
	"github.com/gernest/frieren/internal/px"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
	prometheustranslator "github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheus"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type Series struct {
	Labels      []uint64
	Exemplars   []uint64
	Timestamp   []uint64
	Values      []uint64
	Histograms  []uint64
	HistogramTS []uint64
}

func FromLogs(md pmetric.Metrics, tr blob.Func) map[uint64]*Series {
	if md.DataPointCount() == 0 {
		return nil
	}
	rm := md.ResourceMetrics()
	series := make(map[uint64]*Series, rm.Len())
	hash := new(blob.Hash)
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
				name := prometheustranslator.BuildCompliantName(metric, "", false)
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					dataPoints := metric.Gauge().DataPoints()
					if dataPoints.Len() == 0 {
						break
					}
					addGaugeNumberDataPoints(name, dataPoints, rsCtx, scopeCtx, attrCtx, series, hash)
				case pmetric.MetricTypeSum:
					dataPoints := metric.Sum().DataPoints()
					if dataPoints.Len() == 0 {
						break
					}
					addSumNumberDataPoints(name, tr, dataPoints, rsCtx, scopeCtx, attrCtx, series, hash)
				case pmetric.MetricTypeHistogram:
					dataPoints := metric.Histogram().DataPoints()
					if dataPoints.Len() == 0 {
						break
					}
					addHistogramDataPoints(name, tr, dataPoints, rsCtx, scopeCtx, attrCtx, series, hash)
				case pmetric.MetricTypeExponentialHistogram:
					dataPoints := metric.ExponentialHistogram().DataPoints()
					if dataPoints.Len() == 0 {
						break
					}
					addExponentialHistogramDataPoints(name, tr, dataPoints, rsCtx, scopeCtx, attrCtx, series, hash)
				case pmetric.MetricTypeSummary:
					dataPoints := metric.Summary().DataPoints()
					if dataPoints.Len() == 0 {
						break
					}
					addSummaryDataPoints(name, dataPoints, rsCtx, scopeCtx, attrCtx, series, hash)
				default:
				}

			}
		}
	}
	return series

}

func addGaugeNumberDataPoints(name string,
	data pmetric.NumberDataPointSlice,
	resource, scope, attr *px.Ctx, series map[uint64]*Series, hash *blob.Hash) {
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
		b := attr.Bitmap()
		b.RunOptimize()
		hash.Reset()
		b.WriteTo(hash)
		seriesID := hash.Sum64()
		sample, ok := series[seriesID]
		if !ok {
			sample = &Series{
				Labels: b.ToArray(),
			}
			series[seriesID] = sample
		}
		sample.Timestamp = append(sample.Timestamp, uint64(ts))
		sample.Values = append(sample.Values, math.Float64bits(value))
	}
}

func addSumNumberDataPoints(name string,
	tr blob.Func,
	data pmetric.NumberDataPointSlice,
	resource, scope, attr *px.Ctx, series map[uint64]*Series, hash *blob.Hash) {
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
		b := attr.Bitmap()
		b.RunOptimize()
		hash.Reset()
		b.WriteTo(hash)
		seriesID := hash.Sum64()
		sample, ok := series[seriesID]
		if !ok {
			sample = &Series{
				Labels: b.ToArray(),
			}
			series[seriesID] = sample
		}
		sample.Timestamp = append(sample.Timestamp, uint64(ts))
		sample.Values = append(sample.Values, math.Float64bits(value))
		sample.Exemplars = append(sample.Exemplars, getPromExemplars(point, tr)...)
	}
}

func addHistogramDataPoints(name string,
	tr blob.Func,
	data pmetric.HistogramDataPointSlice,
	resource, scope, attr *px.Ctx, series map[uint64]*Series, hash *blob.Hash) {
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
			b := attr.Bitmap()
			seriesID := hash.Bitmap(b)
			sample, ok := series[seriesID]
			if !ok {
				sample = &Series{Labels: b.ToArray()}
				series[seriesID] = sample
			}
			sample.Timestamp = append(sample.Timestamp, uint64(ts))
			sample.Values = append(sample.Values, math.Float64bits(point.Sum()))

			// remove the sum label
			b.Remove(sumID)
		}
		attr.Add(countID)
		b := attr.Bitmap()
		seriesID := hash.Bitmap(b)
		sample, ok := series[seriesID]
		if !ok {
			sample = &Series{}
			series[seriesID] = sample
		}
		sample.Timestamp = append(sample.Timestamp, uint64(ts))
		sample.Values = append(sample.Values, math.Float64bits(float64(point.Count())))

		// remove count label
		b.Remove(sumID)

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
			b := attr.Bitmap()
			seriesID := hash.Bitmap(b)
			bs, ok := series[seriesID]
			if !ok {
				bs = &Series{Labels: b.ToArray()}
				series[seriesID] = bs
			}
			bs.Timestamp = append(bs.Timestamp, uint64(ts))
			bs.Values = append(bs.Values, math.Float64bits(float64(cumulativeCount)))

			bucketBounds = append(bucketBounds, bucketBoundsData{ts: bs, bound: bound})

			b.Remove(bucketID)
			b.Remove(leID)
		}
		var v float64
		if point.Flags().NoRecordedValue() {
			v = math.Float64frombits(value.StaleNaN)
		} else {
			v = float64(point.Count())
		}
		attr.Add(bucketID)
		attr.Set(leStr, pInfStr)
		b = attr.Bitmap()
		seriesID = hash.Bitmap(b)
		is, ok := series[seriesID]
		if !ok {
			is = &Series{Labels: b.ToArray()}
		}
		is.Timestamp = append(is.Timestamp, uint64(ts))
		is.Values = append(is.Values, math.Float64bits(v))
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
	resource, scope, attr *px.Ctx, series map[uint64]*Series, hash *blob.Hash) error {
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

		b := attr.Bitmap()
		seriesID := hash.Bitmap(b)
		sample, ok := series[seriesID]
		if !ok {
			sample = &Series{Labels: b.ToArray()}
			series[seriesID] = sample
		}
		sample.Histograms = append(sample.Histograms, id)
		sample.HistogramTS = append(sample.HistogramTS, uint64(hs.Timestamp))
	}
	return nil
}

func addSummaryDataPoints(name string,
	data pmetric.SummaryDataPointSlice,
	resource, scope, attr *px.Ctx, series map[uint64]*Series, hash *blob.Hash) {
}

type exemplarType interface {
	pmetric.ExponentialHistogramDataPoint | pmetric.HistogramDataPoint | pmetric.NumberDataPoint
	Exemplars() pmetric.ExemplarSlice
}

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

func getPromExemplars[T exemplarType](pt T, tr blob.Func, f ...func(*prompb.Exemplar, uint64)) []uint64 {
	promExemplar := &prompb.Exemplar{}
	var promExemplars []uint64
	if len(f) == 0 {
		promExemplars = make([]uint64, 0, pt.Exemplars().Len())
	}
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
		v := tr(constants.MetricsExemplars, buf)
		if len(f) > 0 {
			f[0](promExemplar, v)
		} else {
			promExemplars = append(promExemplars, v)
		}
	}

	return promExemplars
}

func convertTimeStamp(timestamp pcommon.Timestamp) int64 {
	return timestamp.AsTime().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
}
