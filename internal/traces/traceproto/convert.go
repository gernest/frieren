package traceproto

import (
	"github.com/grafana/tempo/pkg/tempopb"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func from(m ptrace.Traces) (*tempopb.Trace, error) {
	convert, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(m)
	if err != nil {
		return nil, err
	}
	o := &tempopb.Trace{}
	err = o.Unmarshal(convert)
	if err != nil {
		return nil, err
	}
	return o, nil
}

const (
	LabelDuration = "duration"

	StatusCodeTag   = "status.code"
	StatusCodeUnset = "unset"
	StatusCodeOK    = "ok"
	StatusCodeError = "error"

	KindUnspecified = "unspecified"
	KindInternal    = "internal"
	KindClient      = "client"
	KindServer      = "server"
	KindProducer    = "producer"
	KindConsumer    = "consumer"
)

var status = map[ptrace.StatusCode]string{
	ptrace.StatusCodeUnset: StatusCodeUnset,
	ptrace.StatusCodeOk:    StatusCodeOK,
	ptrace.StatusCodeError: StatusCodeError,
}

var kind = map[ptrace.SpanKind]string{
	ptrace.SpanKindUnspecified: KindUnspecified,
	ptrace.SpanKindInternal:    KindInternal,
	ptrace.SpanKindClient:      KindClient,
	ptrace.SpanKindServer:      KindServer,
	ptrace.SpanKindProducer:    KindProducer,
	ptrace.SpanKindConsumer:    KindConsumer,
}
