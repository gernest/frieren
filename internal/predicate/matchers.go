package predicate

import (
	"github.com/gernest/frieren/internal/constants"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/prometheus/prometheus/model/labels"
)

func MultiMatchers(field constants.ID, matchers ...[]*labels.Matcher) Predicate {
	o := make([]Predicate, 0, len(matchers))
	for _, m := range matchers {
		o = append(o, Matchers(field, m...))
	}
	return Or(o)
}

func Matchers(field constants.ID, matchers ...*labels.Matcher) Predicate {
	o := make([]Predicate, 0, len(matchers))
	for _, m := range matchers {
		switch m.Type {
		case labels.MatchEqual:
			o = append(o,
				NewString(
					field, traceql.OpEqual, m.Name, m.Value,
				),
			)
		case labels.MatchNotEqual:
			o = append(o,
				NewString(
					field, traceql.OpNotEqual, m.Name, m.Value,
				),
			)
		case labels.MatchRegexp:
			o = append(o,
				NewString(
					field, traceql.OpRegex, m.Name, m.Value,
				),
			)
		case labels.MatchNotRegexp:
			o = append(o,
				NewString(
					field, traceql.OpNotRegex, m.Name, m.Value,
				),
			)
		}
	}
	o = Optimize(o, true)
	return And(o)
}
