package traceproto

import (
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/px"
	"github.com/gernest/frieren/internal/util"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

type Span struct {
	Resource uint64
	Scope    uint64
	Span     uint64
	Tags     *roaring64.Bitmap
	Start    uint64
	End      uint64
	Duration uint64
}

// Spans can be processed individually during ingest. To save memory, instead of
// returning a list we accept a callback that receives a fully decoded span.
func From(td ptrace.Traces, tr blob.Func, f func(span *Span)) {
	if td.SpanCount() == 0 {
		return
	}
	tx, err := from(td)
	if err != nil {
		util.Exit("converting ptrace.Traces to tempopb.Traces")
	}
	rls := td.ResourceSpans()
	scopeCtx := px.New(constants.TracesFST, tr)
	attrCtx := px.New(constants.TracesFST, tr)
	o := &Span{}
	for i := 0; i < rls.Len(); i++ {
		sls := rls.At(i).ScopeSpans()
		res := rls.At(i).Resource()
		resAttrs := res.Attributes()
		rd, _ := tx.Batches[i].GetResource().Marshal()
		resourceID := tr(constants.TracesResource, rd)
		var serviceName string
		if v, ok := resAttrs.Get(string(semconv.ServiceNameKey)); ok {
			serviceName = v.AsString()
		}
		for j := 0; j < sls.Len(); j++ {
			scope := sls.At(j).Scope()
			spans := sls.At(j).Spans()
			scopeAttrs := scope.Attributes()
			scopeCtx.Reset()
			scopeAttrs.Range(func(k string, v pcommon.Value) bool {
				scopeCtx.Set("scope."+k, v.AsString())
				return true
			})
			if scopeName := scope.Name(); scopeName != "" {
				scopeCtx.Set("scope:name", scopeName)
			}
			if scopeVersion := scope.Version(); scopeVersion != "" {
				scopeCtx.Set("scope:version", scopeVersion)
			}
			sd, _ := tx.Batches[i].ScopeSpans[j].GetScope().Marshal()
			scopeID := tr(constants.TracesScope, sd)

			for k := 0; k < spans.Len(); k++ {
				data, _ := tx.Batches[i].ScopeSpans[j].Spans[k].Marshal()
				spanID := tr(constants.TracesSpan, data)
				span := spans.At(k)
				*o = Span{}
				o.Resource = resourceID
				o.Scope = scopeID
				o.Span = spanID

				attrCtx.Reset()
				attrCtx.Or(scopeCtx)
				span.Attributes().Range(func(k string, v pcommon.Value) bool {
					attrCtx.Set("span."+k, v.AsString())
					return true
				})
				attrCtx.Set("span:name", span.Name())
				attrCtx.Set("span:statusMessage", span.Status().Message())
				attrCtx.Set("span:kind", span.Kind().String())
				ev := span.Events()
				for idx := 0; idx < ev.Len(); idx++ {
					e := ev.At(idx)
					attrCtx.Set("event:name", e.Name())
					e.Attributes().Range(func(k string, v pcommon.Value) bool {
						attrCtx.Set("event."+k, v.AsString())
						return true
					})
				}
				lk := span.Links()
				for idx := 0; idx < lk.Len(); idx++ {
					e := lk.At(idx)
					attrCtx.Set("link:spanID", e.SpanID().String())
					attrCtx.Set("link:traceID", e.TraceID().String())
					e.Attributes().Range(func(k string, v pcommon.Value) bool {
						attrCtx.Set("link."+k, v.AsString())
						return true
					})
				}
				attrCtx.Set("parent:id", span.ParentSpanID().String())
				attrCtx.Set("trace:id", span.TraceID().String())
				attrCtx.Set("span:id", span.SpanID().String())
				if span.ParentSpanID().IsEmpty() {
					attrCtx.Set("trace:rootName", span.Name())
					attrCtx.Set("trace:rootService", serviceName)
				}
				o.Tags = attrCtx.Bitmap()
				o.Start = uint64(span.StartTimestamp())
				o.End = uint64(span.EndTimestamp())
				o.Duration = uint64(span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime()).Nanoseconds())
				f(o)
			}
		}

	}
}
