package logproto

import (
	"encoding/hex"
	"fmt"
	"time"

	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/gernest/frieren/internal/px"
	"github.com/gernest/frieren/internal/store"
	"github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheus"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

type Stream struct {
	ID      uint64
	Labels  []uint64
	Entries []*Entry
}

type Entry struct {
	Timestamp uint64
	Line      uint64
	Metadata  []uint64
}

var resourceAttrAsIndex = map[string]struct{}{
	"service.name":            {},
	"service.namespace":       {},
	"service.instance.id":     {},
	"deployment.environment":  {},
	"cloud.region":            {},
	"cloud.availability_zone": {},
	"k8s.cluster.name":        {},
	"k8s.namespace.name":      {},
	"k8s.pod.name":            {},
	"k8s.container.name":      {},
	"container.name":          {},
	"k8s.replicaset.name":     {},
	"k8s.deployment.name":     {},
	"k8s.statefulset.name":    {},
	"k8s.daemonset.name":      {},
	"k8s.cronjob.name":        {},
	"k8s.job.name":            {},
}

const (
	attrServiceName = "service.name"
)

func FromLogs(ld plog.Logs, tr *store.View) []*v1.Entry {
	if ld.LogRecordCount() == 0 {
		return nil
	}
	entries := make([]*v1.Entry, 0, ld.LogRecordCount())
	rls := ld.ResourceLogs()
	streamCtx := px.New()
	rsCtx := px.New()
	scopeCtx := px.New()
	attrCtx := px.New()
	for i := 0; i < rls.Len(); i++ {
		sls := rls.At(i).ScopeLogs()
		res := rls.At(i).Resource()
		resAttrs := res.Attributes()

		if v, ok := resAttrs.Get(attrServiceName); !ok || v.AsString() == "" {
			resAttrs.PutStr(attrServiceName, "unknown_service")
		}

		streamCtx.Reset()
		rsCtx.Reset()

		resAttrs.Range(func(k string, v pcommon.Value) bool {
			_, idx := resourceAttrAsIndex[k]
			if idx {
				attributeToLabels(streamCtx, k, v, "")
			} else {
				attributeToLabels(rsCtx, k, v, "")
			}
			return true
		})

		streamID, labels := streamCtx.Sum()

		for j := 0; j < sls.Len(); j++ {
			scope := sls.At(j).Scope()
			logs := sls.At(j).LogRecords()
			scopeAttrs := scope.Attributes()

			scopeCtx.Reset()
			scopeAttrs.Range(func(k string, v pcommon.Value) bool {
				attributeToLabels(scopeCtx, k, v, "")
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

			for k := 0; k < logs.Len(); k++ {
				log := logs.At(k)
				attrCtx.Reset()
				attrCtx.Or(rsCtx)
				attrCtx.Or(scopeCtx)
				entry := entry(attrCtx, log, streamID, labels)
				entries = append(entries, entry)
			}
		}
	}
	return entries
}

func entry(x *px.Ctx, log plog.LogRecord, stream []byte, labels []string) *v1.Entry {
	x.Reset()
	// copy log attributes and all the fields from log(except log.Body) to structured metadata
	logAttrs := log.Attributes()
	logAttrs.Range(func(k string, v pcommon.Value) bool {
		attributeToLabels(x, k, v, "")
		return true
	})

	// if log.Timestamp() is 0, we would have already stored log.ObservedTimestamp as log timestamp so no need to store again in structured metadata
	if log.Timestamp() != 0 && log.ObservedTimestamp() != 0 {
		x.Set("observed_timestamp", fmt.Sprintf("%d", log.ObservedTimestamp().AsTime().UnixNano()))
	}

	if severityNum := log.SeverityNumber(); severityNum != plog.SeverityNumberUnspecified {
		x.Set("severity_number", fmt.Sprintf("%d", severityNum))
	}
	if severityText := log.SeverityText(); severityText != "" {
		x.Set("severity_text", severityText)
	}

	if droppedAttributesCount := log.DroppedAttributesCount(); droppedAttributesCount != 0 {
		x.Set("dropped_attributes_count", fmt.Sprintf("%d", droppedAttributesCount))
	}
	if logRecordFlags := log.Flags(); logRecordFlags != 0 {
		x.Set("flags", fmt.Sprintf("%d", logRecordFlags))
	}

	if traceID := log.TraceID(); !traceID.IsEmpty() {
		x.Set("trace_id", hex.EncodeToString(traceID[:]))
	}
	if spanID := log.SpanID(); !spanID.IsEmpty() {
		x.Set("span_id", hex.EncodeToString(spanID[:]))
	}

	return &v1.Entry{
		Stream:    stream,
		Labels:    labels,
		Timestamp: timestampFromLogRecord(log),
		Line:      []byte(log.Body().AsString()),
		Metadata:  x.Metadata(),
	}
}

func attributeToLabels(x *px.Ctx, k string, v pcommon.Value, prefix string) {
	keyWithPrefix := k
	if prefix != "" {
		keyWithPrefix = prefix + "_" + k
	}
	keyWithPrefix = prometheus.NormalizeLabel(keyWithPrefix)
	typ := v.Type()
	if typ == pcommon.ValueTypeMap {
		mv := v.Map()
		mv.Range(func(k string, v pcommon.Value) bool {
			attributeToLabels(x, k, v, keyWithPrefix)
			return true
		})
	} else {
		x.Set(keyWithPrefix, v.AsString())
	}
}

func timestampFromLogRecord(lr plog.LogRecord) uint64 {
	if lr.Timestamp() != 0 {
		return uint64(lr.Timestamp())
	}

	if lr.ObservedTimestamp() != 0 {
		return uint64(lr.ObservedTimestamp())
	}

	return uint64(time.Now().UTC().UnixNano())
}
