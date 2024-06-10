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
// Provenance-includes-location: https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/95e8f8fdc2a9dc87230406c9a3cf02be4fd68bea/pkg/translator/prometheusremotewrite/number_data_points.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Copyright The OpenTelemetry Authors.

package prometheusremotewrite

import (
	"math"

	"github.com/prometheus/common/model"
	pv "github.com/prometheus/prometheus/model/value"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func (c *PrometheusConverter) addGaugeNumberDataPoints(dataPoints pmetric.NumberDataPointSlice,
	resource pcommon.Resource, settings Settings, name string) {
	for x := 0; x < dataPoints.Len(); x++ {
		pt := dataPoints.At(x)
		labels := c.createAttributes(
			resource,
			pt.Attributes(),
			settings.ExternalLabels,
			nil,
			true,
			model.MetricNameLabel,
			name,
		)
		timestmap := convertTimeStamp(pt.Timestamp())
		var value float64
		switch pt.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			value = float64(pt.IntValue())
		case pmetric.NumberDataPointValueTypeDouble:
			value = pt.DoubleValue()
		}
		if pt.Flags().NoRecordedValue() {
			value = math.Float64frombits(pv.StaleNaN)
		}
		c.addSample(value, timestmap, labels)
	}
}

func (c *PrometheusConverter) addSumNumberDataPoints(dataPoints pmetric.NumberDataPointSlice,
	resource pcommon.Resource, metric pmetric.Metric, settings Settings, name string) {
	for x := 0; x < dataPoints.Len(); x++ {
		pt := dataPoints.At(x)
		lbls := c.createAttributes(
			resource,
			pt.Attributes(),
			settings.ExternalLabels,
			nil,
			true,
			model.MetricNameLabel,
			name,
		)
		timestamp := convertTimeStamp(pt.Timestamp())
		var value float64
		switch pt.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			value = float64(pt.IntValue())
		case pmetric.NumberDataPointValueTypeDouble:
			value = pt.DoubleValue()
		}
		if pt.Flags().NoRecordedValue() {
			value = math.Float64frombits(pv.StaleNaN)
		}
		ts := c.addSample(value, timestamp, lbls)
		if ts != nil {
			exemplars := getPromExemplars[pmetric.NumberDataPoint](c, pt)
			ts.Exemplars = append(ts.Exemplars, exemplars...)
		}
	}
}
