package traces

import (
	"fmt"
	"math"

	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/predicate"
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

func CompileConditions(conds []traceql.Condition, start, end uint64, allConditions bool) (predicate.Predicate, error) {
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
		return predicate.And(all), nil
	}
	return predicate.Or(all), nil
}

func createEventIterator(conditions []traceql.Condition) (preds []predicate.Predicate, err error) {
	if len(conditions) == 0 {
		return nil, nil
	}
	preds = make([]predicate.Predicate, 0, len(conditions))

	for _, cond := range conditions {
		switch cond.Attribute.Intrinsic {
		case traceql.IntrinsicEventName:
			if !predicate.ValidForStrings(cond.Op) {
				return nil, fmt.Errorf("%v not valid for event:name", cond.Op)
			}
			preds = append(preds, createString(
				cond.Op, "event:name", cond.Operands[0].S,
			))
		default:
			preds = append(preds, createString(
				cond.Op, "event."+cond.Attribute.Name, cond.Operands[0].S,
			))
		}
	}
	return
}

func createLinkIterator(conditions []traceql.Condition) (preds []predicate.Predicate, err error) {
	if len(conditions) == 0 {
		return nil, nil
	}
	preds = make([]predicate.Predicate, 0, len(conditions))

	for _, cond := range conditions {
		switch cond.Attribute.Intrinsic {
		case traceql.IntrinsicLinkTraceID:
			if !validForBytes(cond.Op) {
				return nil, fmt.Errorf("%v not valid for link:traceID", cond.Op)
			}
			preds = append(preds, createString(
				cond.Op, "link:traceID", cond.Operands[0].S,
			))
		case traceql.IntrinsicLinkSpanID:
			if !validForBytes(cond.Op) {
				return nil, fmt.Errorf("%v not valid for link:spanID", cond.Op)
			}
			preds = append(preds, createString(
				cond.Op, "link:spanID", cond.Operands[0].S,
			))
		}
	}
	return
}

func createSpanPredicates(conds []traceql.Condition) (preds []predicate.Predicate, err error) {
	preds = make([]predicate.Predicate, 0, len(conds))
	for _, cond := range conds {
		// Intrinsic
		switch cond.Attribute.Intrinsic {
		case traceql.IntrinsicSpanID:
			if !validForBytes(cond.Op) {
				return nil, fmt.Errorf("%v not valid for span:id", cond.Op)
			}
			preds = append(preds, createString(
				cond.Op, "span:id", cond.Operands[0].S,
			))

		case traceql.IntrinsicSpanStartTime:
			if !predicate.ValidForInts(cond.Op) {
				return nil, fmt.Errorf("%v not valid for span:start", cond.Op)
			}
			preds = append(preds, predicate.NewInt(
				constants.TracesSpanStart, cond.Op, uint64(cond.Operands[0].N),
			))
		case traceql.IntrinsicName:
			if !predicate.ValidForStrings(cond.Op) {
				return nil, fmt.Errorf("%v not valid for span:name", cond.Op)
			}
			preds = append(preds, createString(
				cond.Op, "span:name", cond.Operands[0].S,
			))

		case traceql.IntrinsicKind:
			if !predicate.ValidForStrings(cond.Op) {
				return nil, fmt.Errorf("%v not valid for span:name", cond.Op)
			}
			preds = append(preds, createString(
				cond.Op, "span:kind", cond.Operands[0].S,
			))
		case traceql.IntrinsicDuration:
			if !predicate.ValidForInts(cond.Op) {
				return nil, fmt.Errorf("%v not valid for span:start", cond.Op)
			}
			preds = append(preds, predicate.NewInt(
				constants.TracesSpanDuration, cond.Op, uint64(cond.Operands[0].D.Nanoseconds()),
			))
		case traceql.IntrinsicStatus:
			if !predicate.ValidForStrings(cond.Op) {
				return nil, fmt.Errorf("%v not valid for span:name", cond.Op)
			}
			preds = append(preds, createString(
				cond.Op, "span:status", cond.Operands[0].S,
			))
		case traceql.IntrinsicStatusMessage:
			if !predicate.ValidForStrings(cond.Op) {
				return nil, fmt.Errorf("%v not valid for span:name", cond.Op)
			}
			preds = append(preds, createString(
				cond.Op, "span:statusMessage", cond.Operands[0].S,
			))

		case traceql.IntrinsicStructuralDescendant:
		case traceql.IntrinsicStructuralChild:
		case traceql.IntrinsicStructuralSibling:
		default:
			if !predicate.ValidForStrings(cond.Op) {
				return nil, fmt.Errorf("%v not valid for span.*", cond.Op)
			}
			preds = append(preds, createString(
				cond.Op, "span."+cond.Attribute.Name, cond.Operands[0].EncodeToString(false),
			))
		}
	}
	return
}

func createString(op traceql.Operator, key, value string) predicate.Predicate {
	return predicate.NewString(constants.TracesLabels, op, key, value)
}

func createResourcePredicates(conds []traceql.Condition) (preds []predicate.Predicate, err error) {
	preds = make([]predicate.Predicate, 0, len(conds))
	for _, cond := range conds {
		if !predicate.ValidForStrings(cond.Op) {
			return nil, fmt.Errorf("%v not valid for resource.*", cond.Op)
		}
		preds = append(preds, createString(
			cond.Op, "resource."+cond.Attribute.Name, cond.Operands[0].EncodeToString(false),
		))
	}
	return
}

func createTracePredicates(conds []traceql.Condition, start, end uint64) (preds []predicate.Predicate, err error) {
	// add conditional iterators first. this way if someone searches for { traceDuration > 1s && span.foo = "bar"} the query will
	// be sped up by searching for traceDuration first. note that we can only set the predicates if all conditions is true.
	// otherwise we just pass the info up to the engine to make a choice
	preds = make([]predicate.Predicate, 0, len(conds))
	for _, cond := range conds {
		switch cond.Attribute.Intrinsic {
		case traceql.IntrinsicTraceID:
			if !validForBytes(cond.Op) {
				return nil, fmt.Errorf("%v not valid for trace:id", cond.Op)
			}
			preds = append(preds, createString(
				cond.Op, "trace:id", cond.Operands[0].S,
			))
		case traceql.IntrinsicTraceDuration:
			if !predicate.ValidForInts(cond.Op) {
				return nil, fmt.Errorf("%v not valid for trace:duration", cond.Op)
			}
			preds = append(preds, predicate.NewInt(
				constants.TracesDuration, cond.Op,
				uint64(cond.Operands[0].D.Nanoseconds()),
			))
		case traceql.IntrinsicTraceStartTime:
		case traceql.IntrinsicTraceRootSpan:
			if !predicate.ValidForStrings(cond.Op) {
				return nil, fmt.Errorf("%v not valid for trace:rootSpan", cond.Op)
			}
			preds = append(preds, createString(
				cond.Op, "trace:rootSpan", cond.Operands[0].S,
			))

		case traceql.IntrinsicTraceRootService:
			if !predicate.ValidForStrings(cond.Op) {
				return nil, fmt.Errorf("%v not valid for trace:rootService", cond.Op)
			}
			preds = append(preds, createString(
				cond.Op, "trace:rootService", cond.Operands[0].S,
			))
		}
	}
	if end == 0 {
		end = math.MaxUint64
	}
	preds = append(preds, predicate.NewInt(constants.TracesStart, traceql.OpGreaterEqual, start))
	preds = append(preds, predicate.NewInt(constants.TracesEnd, traceql.OpLess, start))
	return
}

func validForBytes(op traceql.Operator) bool {
	return op == traceql.OpEqual || op == traceql.OpNotEqual
}
