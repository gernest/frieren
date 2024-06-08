package traceproto

import (
	"fmt"

	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/px"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/protobuf/proto"
)

type Span struct {
	Resource      []uint64
	Tags          []uint64
	TraceID       uint64
	SpanID        uint64
	State         uint64
	Parent        uint64
	Name          uint64
	Kind          uint64
	Start         uint64
	End           uint64
	Duration      uint64
	Events        uint64
	Links         uint64
	StatusCode    uint64
	StatusMessage uint64
}

// Spans can be processed individually during ingest. To save memory, instead of
// returning a list we accept a callback that receives a fully decoded span.
func From(td ptrace.Traces, tr blob.Func, f func(span *Span)) {
	if td.SpanCount() == 0 {
		return
	}
	rls := td.ResourceSpans()
	streamCtx := px.New(tr)
	rsCtx := px.New(tr)
	scopeCtx := px.New(tr)
	attrCtx := px.New(tr)
	eventsCtx := px.New(tr)
	o := &Span{}
	for i := 0; i < rls.Len(); i++ {
		sls := rls.At(i).ScopeSpans()
		res := rls.At(i).Resource()
		resAttrs := res.Attributes()

		streamCtx.Reset()
		rsCtx.Reset()

		resAttrs.Range(func(k string, v pcommon.Value) bool {
			rsCtx.Set(k, v.AsString())
			return true
		})
		ra := rsCtx.ToArray()

		for j := 0; j < sls.Len(); j++ {
			scope := sls.At(j).Scope()
			spans := sls.At(j).Spans()
			scopeAttrs := scope.Attributes()
			scopeCtx.Reset()
			scopeAttrs.Range(func(k string, v pcommon.Value) bool {
				scopeCtx.Set(k, v.AsString())
				return true
			})
			if scopeName := scope.Name(); scopeName != "" {
				scopeCtx.Set("scope_name", scopeName)
			}
			if scopeVersion := scope.Version(); scopeVersion != "" {
				scopeCtx.Set("scope_version", scopeVersion)
			}
			if scopeDroppedAttributesCount := scope.DroppedAttributesCount(); scopeDroppedAttributesCount != 0 {
				scopeCtx.Set("scope_dropped_attributes_count", fmt.Sprintf("%d", scopeDroppedAttributesCount))
			}
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				*o = Span{}
				o.Resource = ra
				attrCtx.Reset()
				attrCtx.Or(scopeCtx)
				spanEntry(attrCtx, eventsCtx, span, o)
				f(o)
			}
		}

	}
}

func spanEntry(ctx, events *px.Ctx, sp ptrace.Span, o *Span) {
	sp.Attributes().Range(func(k string, v pcommon.Value) bool {
		ctx.Set(k, v.AsString())
		return true
	})
	o.Tags = ctx.ToArray()
	o.TraceID = ctx.Tr([]byte(sp.TraceID().String()))
	o.SpanID = ctx.Tr([]byte(sp.SpanID().String()))
	o.TraceID = ctx.Tr([]byte(sp.TraceState().AsRaw()))
	if p := sp.ParentSpanID(); !p.IsEmpty() {
		o.Parent = ctx.Tr([]byte(p.String()))
	}
	o.Name = ctx.Tr([]byte(sp.Name()))
	o.Kind = uint64(sp.Kind())
	o.Start = uint64(sp.StartTimestamp())
	o.End = uint64(sp.EndTimestamp())
	o.Duration = uint64(sp.EndTimestamp().AsTime().Sub(sp.StartTimestamp().AsTime()).Nanoseconds())

	if e := sp.Events(); e.Len() > 0 {
		ed, _ := proto.Marshal(event(events, e))
		o.Events = ctx.Tr(ed)
	}
	if e := sp.Links(); e.Len() > 0 {
		ed, _ := proto.Marshal(links(events, e))
		o.Links = ctx.Tr(ed)
	}
	s := sp.Status()
	o.StatusCode = uint64(s.Code())
	o.StatusMessage = ctx.Tr([]byte(s.Message()))

}

func event(ctx *px.Ctx, pe ptrace.SpanEventSlice) *v1.Events {
	o := &v1.Events{
		Timestamp: make([]uint64, pe.Len()),
		Name:      make([]uint64, pe.Len()),
		Attr:      make([]*v1.Attr, pe.Len()),
	}
	for i := 0; i < pe.Len(); i++ {
		x := pe.At(i)
		o.Timestamp[i] = uint64(x.Timestamp())
		o.Name[i] = ctx.Tr([]byte(x.Name()))
		o.Attr[i] = attr(ctx, x.Attributes())
	}
	return o
}

func links(ctx *px.Ctx, pe ptrace.SpanLinkSlice) *v1.Links {
	o := &v1.Links{
		TraceId:    make([]uint64, pe.Len()),
		SpanId:     make([]uint64, pe.Len()),
		Attr:       make([]*v1.Attr, pe.Len()),
		TraceState: make([]uint64, pe.Len()),
	}
	for i := 0; i < pe.Len(); i++ {
		x := pe.At(i)
		o.TraceId[i] = ctx.Tr([]byte(x.TraceID().String()))
		o.SpanId[i] = ctx.Tr([]byte(x.SpanID().String()))
		o.TraceState[i] = ctx.Tr([]byte(x.TraceState().AsRaw()))
		o.Attr[i] = attr(ctx, x.Attributes())
	}
	return o
}

func attr(ctx *px.Ctx, a pcommon.Map) *v1.Attr {
	ctx.Reset()
	a.Range(func(k string, v pcommon.Value) bool {
		ctx.Set(k, v.AsString())
		return true
	})
	return &v1.Attr{Value: ctx.ToArray()}
}
