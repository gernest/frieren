package traceproto

import (
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/px"
	"github.com/gernest/frieren/internal/util"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type Span struct {
	Resource      uint64
	Scope         uint64
	Span          uint64
	Tags          *roaring64.Bitmap
	TraceID       uint64
	SpanID        uint64
	Parent        uint64
	Name          uint64
	Kind          uint64
	Start         uint64
	End           uint64
	Duration      uint64
	StatusCode    uint64
	StatusMessage uint64
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
	rsCtx := px.New(constants.TracesFST, tr)
	scopeCtx := px.New(constants.TracesFST, tr)
	attrCtx := px.New(constants.TracesFST, tr)
	eventsCtx := px.New(constants.TracesFST, tr)
	o := &Span{}
	for i := 0; i < rls.Len(); i++ {
		sls := rls.At(i).ScopeSpans()
		res := rls.At(i).Resource()
		resAttrs := res.Attributes()

		rsCtx.Reset()

		resAttrs.Range(func(k string, v pcommon.Value) bool {
			rsCtx.Set(k, v.AsString())
			return true
		})
		rd, _ := tx.Batches[i].GetResource().Marshal()
		resourceID := tr(constants.TracesResource, rd)

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
				attrCtx.Or(rsCtx)
				attrCtx.Or(scopeCtx)
				span.Attributes().Range(func(k string, v pcommon.Value) bool {
					attrCtx.Set(k, v.AsString())
					return true
				})
				o.Tags = attrCtx.Bitmap()
				spanEntry(eventsCtx, span, o)
				f(o)
			}
		}

	}
}

func spanEntry(ctx *px.Ctx, sp ptrace.Span, o *Span) {

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

	s := sp.Status()
	o.StatusCode = uint64(s.Code())
	o.StatusMessage = ctx.Tr([]byte(s.Message()))
}
