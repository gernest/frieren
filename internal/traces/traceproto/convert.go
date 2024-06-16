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
