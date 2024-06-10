package prometheusremotewrite

import (
	"fmt"
	"sync"

	"github.com/prometheus/common/model"
	prometheustranslator "github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheus"
	"go.opentelemetry.io/collector/pdata/pcommon"
	conventions "go.opentelemetry.io/collector/semconv/v1.6.1"
)

type TimeSeries struct {
	ID              uint64
	Labels          []uint64
	SampleValue     []uint64
	SampleTimestamp []uint64
	HistValue       []uint64
	HistTimestamp   []uint64
	Exemplars       []uint64
}

func NewTS() *TimeSeries {
	return tsPool.Get().(*TimeSeries)
}

func (ts *TimeSeries) Release() {
	ts.Reset()
	tsPool.Put(ts)
}

func (ts *TimeSeries) Reset() {
	ts.ID = 0
	ts.Labels = ts.Labels[:0]
	ts.SampleValue = ts.SampleValue[:0]
	ts.SampleTimestamp = ts.SampleTimestamp[:0]
	ts.HistValue = ts.HistValue[:0]
	ts.HistTimestamp = ts.HistTimestamp[:0]
	ts.Exemplars = ts.Exemplars[:0]
}

var tsPool = &sync.Pool{New: func() any { return new(TimeSeries) }}

func (c *PrometheusConverter) createAttributes(resource pcommon.Resource, attributes pcommon.Map, externalLabels map[string]string,
	ignoreAttrs []string, logOnOverwrite bool, extras ...string) []uint64 {
	c.buf.Reset()
	c.bm.Clear()
	resourceAttrs := resource.Attributes()
	serviceName, haveServiceName := resourceAttrs.Get(conventions.AttributeServiceName)
	instance, haveInstanceID := resourceAttrs.Get(conventions.AttributeServiceInstanceID)

	// Calculate the maximum possible number of labels we could return so we can preallocate l
	maxLabelCount := attributes.Len() + len(externalLabels) + len(extras)/2

	if haveServiceName {
		maxLabelCount++
	}

	if haveInstanceID {
		maxLabelCount++
	}

	// XXX: Should we always drop service namespace/service name/service instance ID from the labels
	// (as they get mapped to other Prometheus labels)?
	attributes.Range(func(key string, value pcommon.Value) bool {
		c.addLabel(prometheustranslator.NormalizeLabel(key), value.AsString())
		return true
	})

	// Map service.name + service.namespace to job
	if haveServiceName {
		val := serviceName.AsString()
		if serviceNamespace, ok := resourceAttrs.Get(conventions.AttributeServiceNamespace); ok {
			val = fmt.Sprintf("%s/%s", serviceNamespace.AsString(), val)
		}
		c.addLabel(model.JobLabel, val)
	}
	// Map service.instance.id to instance
	if haveInstanceID {
		c.addLabel(model.InstanceLabel, instance.AsString())
	}
	for key, value := range externalLabels {
		c.addLabel(key, value)
	}

	for i := 0; i < len(extras); i += 2 {
		if i+1 >= len(extras) {
			break
		}
		// internal labels should be maintained
		name := extras[i]
		if !(len(name) > 4 && name[:2] == "__" && name[len(name)-2:] == "__") {
			name = prometheustranslator.NormalizeLabel(name)
		}
		c.addLabel(name, extras[i+1])
	}
	return c.bm.ToArray()
}

func (c *PrometheusConverter) addLabel(key, value string) {
	c.buf.Reset()
	c.buf.WriteString(key)
	c.buf.WriteByte('=')
	c.buf.WriteString(value)
	c.bm.Add(c.tr(c.buf.Bytes()))
}
