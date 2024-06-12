package logproto

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/px"
	"github.com/gernest/frieren/internal/util"
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

func FromLogs(ld plog.Logs, tr blob.Func) map[uint64]*Stream {
	if ld.LogRecordCount() == 0 {
		return nil
	}

	rls := ld.ResourceLogs()
	var h xxhash.Digest
	pushRequestsByStream := make(map[uint64]*Stream, rls.Len())
	streamCtx := px.New(tr)
	rsCtx := px.New(tr)
	scopeCtx := px.New(tr)
	attrCtx := px.New(tr)
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

		lbs := streamCtx.ToArray()
		h.Reset()
		h.Write(util.Uint64ToBytes(lbs))
		streamID := h.Sum64()

		if _, ok := pushRequestsByStream[streamID]; !ok {
			pushRequestsByStream[streamID] = &Stream{
				ID:     streamID,
				Labels: lbs,
			}
		}

		for j := 0; j < sls.Len(); j++ {
			scope := sls.At(j).Scope()
			logs := sls.At(j).LogRecords()
			scopeAttrs := scope.Attributes()

			// it would be rare to have multiple scopes so if the entries slice is empty, pre-allocate it for the number of log entries
			if cap(pushRequestsByStream[streamID].Entries) == 0 {
				stream := pushRequestsByStream[streamID]
				stream.Entries = make([]*Entry, 0, logs.Len())
				pushRequestsByStream[streamID] = stream
			}

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
				entry := entry(attrCtx, log)
				stream := pushRequestsByStream[streamID]
				stream.Entries = append(stream.Entries, entry)
			}
		}
	}
	return pushRequestsByStream
}

func entry(x *px.Ctx, log plog.LogRecord) *Entry {
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

	return &Entry{
		Timestamp: timestampFromLogRecord(log),
		Line:      x.Tr([]byte(log.Body().AsString())),
		Metadata:  x.ToArray(),
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
