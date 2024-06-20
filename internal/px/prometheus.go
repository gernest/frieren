package px

import (
	"github.com/prometheus/common/model"
	prometheustranslator "github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheus"
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

func (ctx *Ctx) Resource(resource pcommon.Resource) {
	a := resource.Attributes()
	name, haveServiceName := a.Get(string(semconv.ServiceNameKey))
	instance, haveInstanceID := a.Get(string(semconv.ServiceInstanceIDKey))
	ns, hasNs := a.Get(string(semconv.ServiceNamespaceKey))

	if haveServiceName {
		val := name.AsString()
		if hasNs {
			val = ns.AsString() + "/" + val
		}
		ctx.Set(model.JobLabel, val)
	}
	if haveInstanceID {
		ctx.Set(model.InstanceLabel, instance.AsString())
	}
	a.Range(ctx.SetProm)
}

func (ctx *Ctx) SetProm(key string, value pcommon.Value) bool {
	ctx.Set(
		prometheustranslator.NormalizeLabel(key), value.AsString(),
	)
	return true
}
