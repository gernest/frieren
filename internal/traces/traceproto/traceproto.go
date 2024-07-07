package traceproto

import (
	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/gernest/frieren/internal/px"
	"github.com/gernest/frieren/internal/util"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

// Spans can be processed individually during ingest. To save memory, instead of
// returning a list we accept a callback that receives a fully decoded span.
func From(td ptrace.Traces) (result []*v1.Span) {
	if td.SpanCount() == 0 {
		return []*v1.Span{}
	}
	tx, err := from(td)
	if err != nil {
		util.Exit("converting ptrace.Traces to tempopb.Traces")
	}
	result = make([]*v1.Span, 0, td.SpanCount())
	rls := td.ResourceSpans()
	resourceCtx := px.New()
	scopeCtx := px.New()
	attrCtx := px.New()

	durations := map[string]*v1.Span{}

	defer clear(durations)

	update := func(e *v1.Span, id string) {
		x, ok := durations[id]
		if !ok {
			e.TraceStartNano = e.SpanStartNano
			e.TraceEndNano = e.SpanEndNano
			e.TraceDuration = e.TraceEndNano - e.TraceStartNano
			return
		}
		e.TraceStartNano = min(e.SpanStartNano, x.TraceStartNano)
		e.TraceEndNano = max(e.SpanEndNano, x.TraceEndNano)
		e.TraceDuration = e.TraceEndNano - e.TraceStartNano
	}
	for i := 0; i < rls.Len(); i++ {
		sls := rls.At(i).ScopeSpans()
		res := rls.At(i).Resource()
		resAttrs := res.Attributes()
		resourceCtx.Reset()
		resAttrs.Range(resourceCtx.SetAttribute)
		resource, _ := tx.Batches[i].GetResource().Marshal()
		resourceLabels := resourceCtx.Labels()
		var serviceName string
		if v, ok := resAttrs.Get(string(semconv.ServiceNameKey)); ok {
			serviceName = v.AsString()
		}
		for j := 0; j < sls.Len(); j++ {
			scope := sls.At(j).Scope()
			spans := sls.At(j).Spans()
			scopeAttrs := scope.Attributes()
			scopeCtx.Reset()
			scopeAttrs.Range(scopeCtx.SetAttribute)

			scopeName := scope.Name()
			scopeVersion := scope.Version()
			scopeData, _ := tx.Batches[i].ScopeSpans[j].GetScope().Marshal()
			scopeLabels := scopeCtx.Labels()
			for k := 0; k < spans.Len(); k++ {
				spanData, _ := tx.Batches[i].ScopeSpans[j].Spans[k].Marshal()
				span := spans.At(k)
				o := &v1.Span{
					Resource:           resource,
					ResourceAttributes: resourceLabels,
					Scope:              scopeData,
					ScopeAttributes:    scopeLabels,
					ScopeName:          scopeName,
					ScopeVersion:       scopeVersion,
					Span:               spanData,

					SpanName:          span.Name(),
					SpanStatus:        status[span.Status().Code()],
					SpanStatusMessage: span.Status().Message(),
					SpanKind:          kind[span.Kind()],
				}

				attrCtx.Reset()
				span.Attributes().Range(attrCtx.SetAttribute)
				o.SpanAttributes = attrCtx.Labels()

				ev := span.Events()
				attrCtx.Reset()
				if ev.Len() > 0 {
					names := map[string]struct{}{}
					attrCtx.Reset()
					for idx := 0; idx < ev.Len(); idx++ {
						e := ev.At(idx)
						names[e.Name()] = struct{}{}
						e.Attributes().Range(attrCtx.SetAttribute)
					}
					o.EventName = make([]string, 0, len(names))
					for n := range names {
						o.EventName = append(o.EventName, n)
					}
					o.EventAttributes = attrCtx.Labels()
				}

				lk := span.Links()
				if lk.Len() > 0 {
					sid := map[string]struct{}{}
					tid := map[string]struct{}{}

					attrCtx.Reset()

					for idx := 0; idx < lk.Len(); idx++ {
						e := lk.At(idx)
						sid[e.SpanID().String()] = struct{}{}
						tid[e.TraceID().String()] = struct{}{}
						e.Attributes().Range(attrCtx.SetAttribute)
					}

					o.LinkSpanId = make([][]byte, 0, len(sid))
					for k := range sid {
						o.LinkSpanId = append(o.LinkSpanId, []byte(k))
					}
					o.LinkTraceId = make([][]byte, 0, len(sid))
					for k := range tid {
						o.LinkTraceId = append(o.LinkTraceId, []byte(k))
					}
				}
				traceId := span.TraceID().String()
				o.TraceId = []byte(traceId)
				o.SpanId = []byte(span.SpanID().String())
				parent := span.ParentSpanID()
				if parent.IsEmpty() {
					o.TraceRootName = span.Name()
					o.TraceRootService = serviceName
				} else {
					o.ParentId = []byte(parent.String())
				}

				o.SpanStartNano = int64(span.StartTimestamp())
				o.SpanEndNano = int64(span.EndTimestamp())
				update(o, traceId)
			}
		}
	}
	return
}
