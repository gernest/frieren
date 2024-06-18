package traceproto

import (
	"math"
	"slices"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/px"
	"github.com/gernest/frieren/internal/util"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

type Trace struct {
	Start uint64
	End   uint64
	Spans []*Span
}

type Traces map[uint64]*Trace

func (t Traces) add(id uint64, span *Span) {
	x, ok := t[id]
	if !ok {
		x = &Trace{
			Spans: make([]*Span, 0, 64),
			Start: math.MaxUint64,
		}
		t[id] = x
	}
	x.Start = min(x.Start, span.Start)
	x.End = max(x.End, span.End)
	x.Spans = append(x.Spans, span)
}

type Span struct {
	Resource   uint64
	Scope      uint64
	Span       uint64
	Tags       *roaring64.Bitmap
	Start, End uint64
}

// Spans can be processed individually during ingest. To save memory, instead of
// returning a list we accept a callback that receives a fully decoded span.
func From(td ptrace.Traces, tr blob.Func) Traces {
	if td.SpanCount() == 0 {
		return Traces{}
	}
	tx, err := from(td)
	if err != nil {
		util.Exit("converting ptrace.Traces to tempopb.Traces")
	}
	traces := make(Traces)
	rls := td.ResourceSpans()
	resourceCtx := px.New(constants.TracesFST, tr)
	scopeCtx := px.New(constants.TracesFST, tr)
	attrCtx := px.New(constants.TracesFST, tr)
	encode := marshal(tr)
	for i := 0; i < rls.Len(); i++ {
		sls := rls.At(i).ScopeSpans()
		res := rls.At(i).Resource()
		resAttrs := res.Attributes()
		resourceCtx.Reset()
		resAttrs.Range(func(k string, v pcommon.Value) bool {
			resourceCtx.Set(k, v.AsString())
			return true
		})
		resourceID := encode(constants.TracesResource, tx.Batches[i].GetResource())
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
			scopeID := encode(constants.TracesScope, tx.Batches[i].ScopeSpans[j].GetScope())
			for k := 0; k < spans.Len(); k++ {
				spanID := encode(constants.TracesSpan, tx.Batches[i].ScopeSpans[j].Spans[k])
				span := spans.At(k)
				o := &Span{}
				o.Resource = resourceID
				o.Scope = scopeID
				o.Span = spanID

				attrCtx.Reset()
				attrCtx.Or(resourceCtx)
				attrCtx.Or(scopeCtx)
				span.Attributes().Range(func(k string, v pcommon.Value) bool {
					attrCtx.Set("span."+k, v.AsString())
					return true
				})
				attrCtx.Set("span:name", span.Name())
				attrCtx.Set("span:status", status[span.Status().Code()])
				attrCtx.Set("span:statusMessage", span.Status().Message())
				attrCtx.Set("span:kind", kind[span.Kind()])
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
				traceID := attrCtx.Set("trace:id", span.TraceID().String())
				attrCtx.Set("span:id", span.SpanID().String())
				parent := span.ParentSpanID()
				if parent.IsEmpty() {
					attrCtx.Set("trace:rootName", span.Name())
					attrCtx.Set("trace:rootService", serviceName)
				} else {
					attrCtx.Set("parent:id", parent.String())
				}
				o.Tags = attrCtx.Bitmap().Clone()
				o.Start = uint64(span.StartTimestamp())
				o.End = uint64(span.EndTimestamp())
				traces.add(traceID, o)
			}
		}

	}
	return traces
}

type message interface {
	Size() int
	MarshalTo([]byte) (int, error)
}

func marshal(tr blob.Func) func(id constants.ID, msg message) uint64 {
	var buf []byte
	return func(id constants.ID, msg message) uint64 {
		size := msg.Size()
		if size == 0 {
			return tr(id, []byte{})
		}
		buf = slices.Grow(buf, size)[:size]
		_, err := msg.MarshalTo(buf)
		if err != nil {
			util.Exit("marshal trace message", "err", err)
		}
		return tr(id, buf)
	}
}
