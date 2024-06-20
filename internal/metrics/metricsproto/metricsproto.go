package metricsproto

import (
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/px"
	prometheustranslator "github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheus"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type Series struct {
	Labels     []uint64
	Exemplar   []uint64
	Timestamp  []uint64
	Values     []uint64
	Histograms []uint64
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
					addSumNumberDataPoints(name, dataPoints, rsCtx, scopeCtx, attrCtx, series, hash)
				case pmetric.MetricTypeHistogram:
					dataPoints := metric.Histogram().DataPoints()
					if dataPoints.Len() == 0 {
						break
					}
					addHistogramDataPoints(name, dataPoints, rsCtx, scopeCtx, attrCtx, series, hash)
				case pmetric.MetricTypeExponentialHistogram:
					dataPoints := metric.ExponentialHistogram().DataPoints()
					if dataPoints.Len() == 0 {
						break
					}
					addExponentialHistogramDataPoints(name, dataPoints, rsCtx, scopeCtx, attrCtx, series, hash)
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
}

func addSumNumberDataPoints(name string,
	data pmetric.NumberDataPointSlice,
	resource, scope, attr *px.Ctx, series map[uint64]*Series, hash *blob.Hash) {
}
func addHistogramDataPoints(name string,
	data pmetric.HistogramDataPointSlice,
	resource, scope, attr *px.Ctx, series map[uint64]*Series, hash *blob.Hash) {
}
func addExponentialHistogramDataPoints(name string,
	data pmetric.ExponentialHistogramDataPointSlice,
	resource, scope, attr *px.Ctx, series map[uint64]*Series, hash *blob.Hash) {
}

func addSummaryDataPoints(name string,
	data pmetric.SummaryDataPointSlice,
	resource, scope, attr *px.Ctx, series map[uint64]*Series, hash *blob.Hash) {
}
