// Copyright 2024 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// Provenance-includes-location: https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/95e8f8fdc2a9dc87230406c9a3cf02be4fd68bea/pkg/translator/prometheusremotewrite/helper.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Copyright The OpenTelemetry Authors.

package prometheusremotewrite

import (
	"encoding/hex"
	"math"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/gernest/frieren/util"
	"github.com/prometheus/common/model"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/prometheus/prometheus/model/timestamp"
	pv "github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
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

type bucketBoundsData struct {
	ts    *TimeSeries
	bound float64
}

// byBucketBoundsData enables the usage of sort.Sort() with a slice of bucket bounds
type byBucketBoundsData []bucketBoundsData

func (m byBucketBoundsData) Len() int           { return len(m) }
func (m byBucketBoundsData) Less(i, j int) bool { return m[i].bound < m[j].bound }
func (m byBucketBoundsData) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }

// isValidAggregationTemporality checks whether an OTel metric has a valid
// aggregation temporality for conversion to a Prometheus metric.
func isValidAggregationTemporality(metric pmetric.Metric) bool {
	//exhaustive:enforce
	switch metric.Type() {
	case pmetric.MetricTypeGauge, pmetric.MetricTypeSummary:
		return true
	case pmetric.MetricTypeSum:
		return metric.Sum().AggregationTemporality() == pmetric.AggregationTemporalityCumulative
	case pmetric.MetricTypeHistogram:
		return metric.Histogram().AggregationTemporality() == pmetric.AggregationTemporalityCumulative
	case pmetric.MetricTypeExponentialHistogram:
		return metric.ExponentialHistogram().AggregationTemporality() == pmetric.AggregationTemporalityCumulative
	}
	return false
}

func (c *PrometheusConverter) addHistogramDataPoints(dataPoints pmetric.HistogramDataPointSlice,
	resource pcommon.Resource, settings Settings, baseName string) {
	for x := 0; x < dataPoints.Len(); x++ {
		pt := dataPoints.At(x)
		timestamp := convertTimeStamp(pt.Timestamp())
		baseLabels := c.createAttributes(resource, pt.Attributes(), settings.ExternalLabels, nil, false)

		// If the sum is unset, it indicates the _sum metric point should be
		// omitted
		if pt.HasSum() {
			value := pt.Sum()
			if pt.Flags().NoRecordedValue() {
				value = math.Float64frombits(pv.StaleNaN)
			}
			sumlabels := c.createLabels(baseName+sumStr, baseLabels)
			c.addSample(value, timestamp, sumlabels)
		}

		value := float64(pt.Count())
		if pt.Flags().NoRecordedValue() {
			value = math.Float64frombits(pv.StaleNaN)
		}

		countlabels := c.createLabels(baseName+countStr, baseLabels)
		c.addSample(value, timestamp, countlabels)

		// cumulative count for conversion to cumulative histogram
		var cumulativeCount uint64

		var bucketBounds []bucketBoundsData

		// process each bound, based on histograms proto definition, # of buckets = # of explicit bounds + 1
		for i := 0; i < pt.ExplicitBounds().Len() && i < pt.BucketCounts().Len(); i++ {
			bound := pt.ExplicitBounds().At(i)
			cumulativeCount += pt.BucketCounts().At(i)
			value := float64(cumulativeCount)
			if pt.Flags().NoRecordedValue() {
				value = math.Float64frombits(pv.StaleNaN)
			}
			boundStr := strconv.FormatFloat(bound, 'f', -1, 64)
			labels := c.createLabels(baseName+bucketStr, baseLabels, leStr, boundStr)
			ts := c.addSample(value, timestamp, labels)
			bucketBounds = append(bucketBounds, bucketBoundsData{ts: ts, bound: bound})
		}
		// add le=+Inf bucket
		value = float64(pt.Count())
		if pt.Flags().NoRecordedValue() {
			value = math.Float64frombits(pv.StaleNaN)
		}
		infLabels := c.createLabels(baseName+bucketStr, baseLabels, leStr, pInfStr)
		ts := c.addSample(value, timestamp, infLabels)

		bucketBounds = append(bucketBounds, bucketBoundsData{ts: ts, bound: math.Inf(1)})
		c.addExemplars(pt, bucketBounds)
	}
}

type exemplarType interface {
	pmetric.ExponentialHistogramDataPoint | pmetric.HistogramDataPoint | pmetric.NumberDataPoint
	Exemplars() pmetric.ExemplarSlice
}

func getPromExemplars[T exemplarType](c *PrometheusConverter, pt T) []uint64 {
	promExemplar := &prompb.Exemplar{}
	c.bm.Clear()
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
		data, _ := promExemplar.Marshal()
		c.bm.Add(c.tr(data))
	}

	return c.bm.ToArray()
}

// mostRecentTimestampInMetric returns the latest timestamp in a batch of metrics
func mostRecentTimestampInMetric(metric pmetric.Metric) pcommon.Timestamp {
	var ts pcommon.Timestamp
	// handle individual metric based on type
	//exhaustive:enforce
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		dataPoints := metric.Gauge().DataPoints()
		for x := 0; x < dataPoints.Len(); x++ {
			ts = max(ts, dataPoints.At(x).Timestamp())
		}
	case pmetric.MetricTypeSum:
		dataPoints := metric.Sum().DataPoints()
		for x := 0; x < dataPoints.Len(); x++ {
			ts = max(ts, dataPoints.At(x).Timestamp())
		}
	case pmetric.MetricTypeHistogram:
		dataPoints := metric.Histogram().DataPoints()
		for x := 0; x < dataPoints.Len(); x++ {
			ts = max(ts, dataPoints.At(x).Timestamp())
		}
	case pmetric.MetricTypeExponentialHistogram:
		dataPoints := metric.ExponentialHistogram().DataPoints()
		for x := 0; x < dataPoints.Len(); x++ {
			ts = max(ts, dataPoints.At(x).Timestamp())
		}
	case pmetric.MetricTypeSummary:
		dataPoints := metric.Summary().DataPoints()
		for x := 0; x < dataPoints.Len(); x++ {
			ts = max(ts, dataPoints.At(x).Timestamp())
		}
	}
	return ts
}

func (c *PrometheusConverter) addSummaryDataPoints(dataPoints pmetric.SummaryDataPointSlice, resource pcommon.Resource,
	settings Settings, baseName string) {
	for x := 0; x < dataPoints.Len(); x++ {
		pt := dataPoints.At(x)
		timestamp := convertTimeStamp(pt.Timestamp())
		baseLabels := c.createAttributes(resource, pt.Attributes(), settings.ExternalLabels, nil, false)

		// treat sum as a sample in an individual TimeSeries
		value := pt.Sum()
		if pt.Flags().NoRecordedValue() {
			value = math.Float64frombits(pv.StaleNaN)
		}
		// sum and count of the summary should append suffix to baseName
		sumlabels := c.createLabels(baseName+sumStr, baseLabels)
		c.addSample(value, timestamp, sumlabels)

		// treat count as a sample in an individual TimeSeries
		value = float64(pt.Count())
		if pt.Flags().NoRecordedValue() {
			value = math.Float64frombits(pv.StaleNaN)
		}
		countlabels := c.createLabels(baseName+countStr, baseLabels)
		c.addSample(value, timestamp, countlabels)

		// process each percentile/quantile
		for i := 0; i < pt.QuantileValues().Len(); i++ {
			qt := pt.QuantileValues().At(i)
			value := qt.Value()
			if pt.Flags().NoRecordedValue() {
				value = math.Float64frombits(pv.StaleNaN)
			}
			percentileStr := strconv.FormatFloat(qt.Quantile(), 'f', -1, 64)
			qtlabels := c.createLabels(baseName, baseLabels, quantileStr, percentileStr)
			c.addSample(value, timestamp, qtlabels)
		}
	}
}

// createLabels returns a copy of baseLabels, adding to it the pair model.MetricNameLabel=name.
// If extras are provided, corresponding label pairs are also added to the returned slice.
// If extras is uneven length, the last (unpaired) extra will be ignored.
func (c *PrometheusConverter) createLabels(name string, baseLabels []uint64, extras ...string) []uint64 {
	c.bm.Clear()
	c.bm.AddMany(baseLabels)
	n := len(extras)
	n -= n % 2
	for extrasIdx := 0; extrasIdx < n; extrasIdx += 2 {
		c.addLabel(extras[extrasIdx], extras[extrasIdx+1])
	}

	c.addLabel(model.MetricNameLabel, name)
	return c.bm.ToArray()
}

// getOrCreateTimeSeries returns the time series corresponding to the label set if existent, and false.
// Otherwise it creates a new one and returns that, and true.
func (c *PrometheusConverter) getOrCreateTimeSeries(lbls []uint64) (*TimeSeries, bool) {
	c.h.Reset()
	c.h.Write(util.Uint64ToBytes(lbls))
	id := c.h.Sum64()
	ts, ok := c.unique[id]
	if !ok {
		ts = NewTS()
		ts.ID = id
		ts.Labels = lbls
		c.unique[id] = ts
		return ts, true
	}
	return ts, false
}

// convertTimeStamp converts OTLP timestamp in ns to timestamp in ms
func convertTimeStamp(timestamp pcommon.Timestamp) int64 {
	return timestamp.AsTime().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
}
