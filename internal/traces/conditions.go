package traces

import (
	"fmt"
	"strings"

	"github.com/gernest/rbf/dsl/bsi"
	"github.com/gernest/rbf/dsl/mutex"
	"github.com/gernest/rbf/dsl/query"
	"github.com/grafana/tempo/pkg/traceql"
)

const (
	// a fake intrinsic scope at the trace lvl
	intrinsicScopeTrace = -1
	intrinsicScopeSpan  = -2
	intrinsicScopeEvent = -3
	intrinsicScopeLink  = -4
)

var intrinsicColumnLookups = map[traceql.Intrinsic]struct {
	scope traceql.AttributeScope
	typ   traceql.StaticType
}{
	traceql.IntrinsicName:                 {intrinsicScopeSpan, traceql.TypeString},
	traceql.IntrinsicStatus:               {intrinsicScopeSpan, traceql.TypeStatus},
	traceql.IntrinsicStatusMessage:        {intrinsicScopeSpan, traceql.TypeString},
	traceql.IntrinsicDuration:             {intrinsicScopeSpan, traceql.TypeDuration},
	traceql.IntrinsicKind:                 {intrinsicScopeSpan, traceql.TypeKind},
	traceql.IntrinsicSpanID:               {intrinsicScopeSpan, traceql.TypeString},
	traceql.IntrinsicSpanStartTime:        {intrinsicScopeSpan, traceql.TypeString},
	traceql.IntrinsicStructuralDescendant: {intrinsicScopeSpan, traceql.TypeNil}, // Not a real column, this entry is only used to assign default scope.
	traceql.IntrinsicStructuralChild:      {intrinsicScopeSpan, traceql.TypeNil}, // Not a real column, this entry is only used to assign default scope.
	traceql.IntrinsicStructuralSibling:    {intrinsicScopeSpan, traceql.TypeNil}, // Not a real column, this entry is only used to assign default scope.

	traceql.IntrinsicTraceRootService: {intrinsicScopeTrace, traceql.TypeString},
	traceql.IntrinsicTraceRootSpan:    {intrinsicScopeTrace, traceql.TypeString},
	traceql.IntrinsicTraceDuration:    {intrinsicScopeTrace, traceql.TypeString},
	traceql.IntrinsicTraceID:          {intrinsicScopeTrace, traceql.TypeString},
	traceql.IntrinsicTraceStartTime:   {intrinsicScopeTrace, traceql.TypeDuration},

	// Not used in vparquet2, the following entries are only used to assign the default scope
	traceql.IntrinsicEventName:    {intrinsicScopeEvent, traceql.TypeNil},
	traceql.IntrinsicLinkTraceID:  {intrinsicScopeLink, traceql.TypeNil},
	traceql.IntrinsicLinkSpanID:   {intrinsicScopeLink, traceql.TypeNil},
	traceql.IntrinsicServiceStats: {intrinsicScopeTrace, traceql.TypeNil},
}

func CompileConditions(conds []traceql.Condition, start, end int64, allConditions bool) (query.Filter, error) {
	// Categorize conditions into span-level or resource-level
	var (
		mingledConditions  bool
		spanConditions     []traceql.Condition
		resourceConditions []traceql.Condition
		traceConditions    []traceql.Condition
		eventConditions    []traceql.Condition
		linkConditions     []traceql.Condition
	)
	for _, cond := range conds {
		// If no-scoped intrinsic then assign default scope
		scope := cond.Attribute.Scope
		if cond.Attribute.Scope == traceql.AttributeScopeNone {
			if lookup, ok := intrinsicColumnLookups[cond.Attribute.Intrinsic]; ok {
				scope = lookup.scope
			}
		}

		switch scope {
		case traceql.AttributeScopeNone:
			mingledConditions = true
			spanConditions = append(spanConditions, cond)
			resourceConditions = append(resourceConditions, cond)
		case traceql.AttributeScopeSpan, intrinsicScopeSpan:
			spanConditions = append(spanConditions, cond)
		case traceql.AttributeScopeResource:
			resourceConditions = append(resourceConditions, cond)
		case intrinsicScopeTrace:
			traceConditions = append(traceConditions, cond)
		case traceql.AttributeScopeEvent, intrinsicScopeEvent:
			eventConditions = append(eventConditions, cond)

		case traceql.AttributeScopeLink, intrinsicScopeLink:
			linkConditions = append(linkConditions, cond)
		default:
			return nil, fmt.Errorf("unsupported traceql scope: %s", cond.Attribute)
		}
	}
	// Optimization for queries like {resource.x... && span.y ...}
	// Requires no mingled scopes like .foo=x, which could be satisfied
	// one either resource or span.
	allConditions = allConditions && !mingledConditions
	tr, err := createTracePredicates(traceConditions, start, end)
	if err != nil {
		return nil, err
	}
	r, err := createResourcePredicates(resourceConditions)
	if err != nil {
		return nil, err
	}
	s, err := createSpanPredicates(spanConditions)
	if err != nil {
		return nil, err
	}
	ev, err := createEventIterator(eventConditions)
	if err != nil {
		return nil, err
	}
	ls, err := createLinkIterator(linkConditions)
	if err != nil {
		return nil, err
	}
	all := append(tr, r...)
	all = append(all, s...)
	all = append(all, ev...)
	all = append(all, ls...)
	if allConditions {
		return query.And(all), nil
	}
	return query.Or(all), nil
}

func createEventIterator(conditions []traceql.Condition) (preds []query.Filter, err error) {
	if len(conditions) == 0 {
		return nil, nil
	}
	preds = make([]query.Filter, 0, len(conditions))

	for _, cond := range conditions {
		switch cond.Attribute.Intrinsic {
		case traceql.IntrinsicEventName:
			op, ok := validForBytes(cond.Op)
			if !ok {
				return nil, fmt.Errorf("%v not valid for event:name", cond.Op)
			}
			preds = append(preds, &mutex.MatchString{
				Field: "event_name",
				Op:    op,
				Value: cond.Operands[0].S,
			})

		default:
			op, ok := validForString(cond.Op)
			if !ok {
				return nil, fmt.Errorf("%v not valid for event:name", cond.Op)
			}
			preds = append(preds, &mutex.MatchString{
				Field: "event_attributes",
				Op:    op,
				Value: formatAttribute(op, cond.Attribute.Name, cond.Operands[0].EncodeToString(false)),
			})
		}
	}
	return
}

func createLinkIterator(conditions []traceql.Condition) (preds []query.Filter, err error) {
	if len(conditions) == 0 {
		return nil, nil
	}
	preds = make([]query.Filter, 0, len(conditions))

	for _, cond := range conditions {
		switch cond.Attribute.Intrinsic {
		case traceql.IntrinsicLinkTraceID:
			op, ok := validForBytes(cond.Op)
			if !ok {
				return nil, fmt.Errorf("%v not valid for link:traceID", cond.Op)
			}
			preds = append(preds, &mutex.Blob{
				Field: "link_trace_id",
				Op:    op,
				Value: []byte(cond.Operands[0].S),
			})
		case traceql.IntrinsicLinkSpanID:
			op, ok := validForBytes(cond.Op)
			if !ok {
				return nil, fmt.Errorf("%v not valid for link:spanID", cond.Op)
			}
			preds = append(preds, &mutex.Blob{
				Field: "link_span_id",
				Op:    op,
				Value: []byte(cond.Operands[0].S),
			})
		}
	}
	return
}

func createSpanPredicates(conds []traceql.Condition) (preds []query.Filter, err error) {
	preds = make([]query.Filter, 0, len(conds))
	for _, cond := range conds {
		// Intrinsic
		switch cond.Attribute.Intrinsic {
		case traceql.IntrinsicSpanID:
			op, ok := validForBytes(cond.Op)
			if !ok {
				return nil, fmt.Errorf("%v not valid for span:id", cond.Op)
			}
			preds = append(preds, &mutex.Blob{
				Field: "span_id",
				Op:    op,
				Value: []byte(cond.Operands[0].S),
			})

		case traceql.IntrinsicSpanStartTime:
			op, ok := validForInt(cond.Op)
			if !ok {
				return nil, fmt.Errorf("%v not valid for span:start", cond.Op)
			}
			preds = append(preds, bsi.Filter(
				"span_start_nano", op, int64(cond.Operands[0].N), 0,
			))
		case traceql.IntrinsicName:
			op, ok := validForString(cond.Op)
			if !ok {
				return nil, fmt.Errorf("%v not valid for span:name", cond.Op)
			}
			preds = append(preds, &mutex.MatchString{
				Field: "span_name",
				Op:    op,
				Value: cond.Operands[0].S,
			})

		case traceql.IntrinsicKind:
			op, ok := validForString(cond.Op)
			if !ok {
				return nil, fmt.Errorf("%v not valid for span:name", cond.Op)
			}
			preds = append(preds, &mutex.MatchString{
				Field: "span_kind",
				Op:    op,
				Value: cond.Operands[0].S,
			})
		case traceql.IntrinsicDuration:
			op, ok := validForInt(cond.Op)
			if !ok {
				return nil, fmt.Errorf("%v not valid for span:start", cond.Op)
			}
			preds = append(preds, bsi.Filter(
				"span_duration",
				op, cond.Operands[0].D.Nanoseconds(), 0,
			))
		case traceql.IntrinsicStatus:
			op, ok := validForString(cond.Op)
			if !ok {
				return nil, fmt.Errorf("%v not valid for span:name", cond.Op)
			}
			preds = append(preds, &mutex.MatchString{
				Field: "span_status",
				Op:    op,
				Value: cond.Operands[0].S,
			})
		case traceql.IntrinsicStatusMessage:
			op, ok := validForString(cond.Op)
			if !ok {
				return nil, fmt.Errorf("%v not valid for span:name", cond.Op)
			}
			preds = append(preds, &mutex.MatchString{
				Field: "span_status_message",
				Op:    op,
				Value: cond.Operands[0].S,
			})
		case traceql.IntrinsicStructuralDescendant:
		case traceql.IntrinsicStructuralChild:
		case traceql.IntrinsicStructuralSibling:
		default:
			op, ok := validForString(cond.Op)
			if !ok {
				return nil, fmt.Errorf("%v not valid for span.*", cond.Op)
			}
			preds = append(preds, &mutex.MatchString{
				Field: "resource_attributes",
				Op:    op,
				Value: formatAttribute(op, cond.Attribute.Name, cond.Operands[0].EncodeToString(false)),
			})
		}
	}
	return
}

func createResourcePredicates(conds []traceql.Condition) (preds []query.Filter, err error) {
	preds = make([]query.Filter, 0, len(conds))
	for _, cond := range conds {
		op, ok := validForString(cond.Op)
		if !ok {
			return nil, fmt.Errorf("%v not valid for resource.*", cond.Op)
		}
		preds = append(preds, &mutex.MatchString{
			Field: "resource_attributes",
			Op:    op,
			Value: formatAttribute(op, cond.Attribute.Name, cond.Operands[0].EncodeToString(false)),
		})
	}
	return
}

func formatAttribute(o mutex.OP, key, value string) string {
	switch o {
	case mutex.EQ, mutex.NEQ:
		return key + "=" + value
	case mutex.RE, mutex.NRE:
		return key + "=" + clean(value)
	default:
		return ""
	}
}

func clean(s string) string {
	s = strings.TrimPrefix(s, "$")
	s = strings.TrimSuffix(s, "^")
	return s
}

func createTracePredicates(conds []traceql.Condition, start, end int64) (preds []query.Filter, err error) {
	// add conditional iterators first. this way if someone searches for { traceDuration > 1s && span.foo = "bar"} the query will
	// be sped up by searching for traceDuration first. note that we can only set the predicates if all conditions is true.
	// otherwise we just pass the info up to the engine to make a choice
	preds = make([]query.Filter, 0, len(conds))
	for _, cond := range conds {
		switch cond.Attribute.Intrinsic {
		case traceql.IntrinsicTraceID:
			op, ok := validForBytes(cond.Op)
			if !ok {
				return nil, fmt.Errorf("%v not valid for trace:id", cond.Op)
			}
			preds = append(preds, &mutex.Blob{
				Field: "trace_id",
				Op:    op,
				Value: []byte(cond.Operands[0].S),
			})
		case traceql.IntrinsicTraceDuration:
			op, ok := validForInt(cond.Op)
			if !ok {
				return nil, fmt.Errorf("%v not valid for trace:duration", cond.Op)
			}
			preds = append(preds, bsi.Filter(
				"trace_duration",
				op,
				cond.Operands[0].D.Nanoseconds(),
				0,
			))
		case traceql.IntrinsicTraceStartTime:
			op, ok := validForInt(cond.Op)
			if !ok {
				return nil, fmt.Errorf("%v not valid for trace:duration", cond.Op)
			}
			preds = append(preds, bsi.Filter(
				"trace_start_nano",
				op,
				cond.Operands[0].D.Nanoseconds(),
				0,
			))
		case traceql.IntrinsicTraceRootSpan:
			op, ok := validForString(cond.Op)
			if !ok {
				return nil, fmt.Errorf("%v not valid for trace:rootSpan", cond.Op)
			}
			preds = append(preds, &mutex.MatchString{
				Field: "trace_root_span",
				Op:    op,
				Value: cond.Operands[0].S,
			})

		case traceql.IntrinsicTraceRootService:
			op, ok := validForString(cond.Op)
			if !ok {
				return nil, fmt.Errorf("%v not valid for trace:rootService", cond.Op)
			}
			preds = append(preds, &mutex.MatchString{
				Field: "trace_root_service",
				Op:    op,
				Value: cond.Operands[0].S,
			})
		}
	}
	preds = append(preds, bsi.Filter("trace_start_nano", bsi.GE, start, 0))
	preds = append(preds, bsi.Filter("trace_end_nano", bsi.GE, end, 0))
	return
}

var stringOp = map[traceql.Operator]mutex.OP{
	traceql.OpEqual:    mutex.EQ,
	traceql.OpNotEqual: mutex.NEQ,
	traceql.OpRegex:    mutex.RE,
	traceql.OpNotRegex: mutex.NRE,
}

var bytesOp = map[traceql.Operator]mutex.OP{
	traceql.OpEqual:    mutex.EQ,
	traceql.OpNotEqual: mutex.NEQ,
}

func validForBytes(op traceql.Operator) (o mutex.OP, ok bool) {
	o, ok = bytesOp[op]
	return
}

func validForString(op traceql.Operator) (o mutex.OP, ok bool) {
	o, ok = stringOp[op]
	return
}

var intOp = map[traceql.Operator]bsi.Operation{
	traceql.OpEqual:        bsi.EQ,
	traceql.OpNotEqual:     bsi.NEQ,
	traceql.OpGreater:      bsi.GT,
	traceql.OpGreaterEqual: bsi.GE,
	traceql.OpLess:         bsi.LT,
	traceql.OpLessEqual:    bsi.LE,
}

func validForInt(op traceql.Operator) (o bsi.Operation, ok bool) {
	o, ok = intOp[op]
	return
}
