package predicate

import (
	"bytes"
	"fmt"

	"github.com/blevesearch/vellum"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/keys"
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
	o := Optimize(MatchersPlain(field, matchers...), true)
	return And(o)
}

func MatchersPlain(field constants.ID, matchers ...*labels.Matcher) []Predicate {
	if len(matchers) == 0 {
		return nil
	}
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
	return o
}

func NewLabels(field constants.ID, matches ...*labels.Matcher) *Labels {
	if len(matches) == 0 {
		return &Labels{
			Field: field,
		}
	}
	return &Labels{
		Field:     field,
		Predicate: Matchers(field, matches...),
	}
}

func NewLabelValues(field constants.ID, name string, matches ...*labels.Matcher) *Labels {
	return &Labels{
		Field: field,
		Predicate: Matchers(field, append(matches, &labels.Matcher{
			Type:  labels.MatchRegexp,
			Name:  name,
			Value: ".*",
		})...),
	}
}

type Labels struct {
	Predicate
	Field constants.ID
}

func (l *Labels) Match(ctx *Context, f func(val []byte) error) error {
	if l.Predicate == nil {
		return l.fst(ctx, f)
	}
	b, err := l.Extract(ctx)
	if err != nil {
		return err
	}
	it := b.Iterator()
	for it.HasNext() {
		id := it.Next()
		err := ctx.TrCall(l.Field, id, f)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *Labels) fst(ctx *Context, f func(val []byte) error) error {
	b := new(bytes.Buffer)
	key := keys.FST(b, l.Field, ctx.Shard, ctx.View)
	it, err := ctx.Txn.Get(key)
	if err != nil {
		return fmt.Errorf("reading fst %s %w", b.String(), err)
	}
	return it.Value(func(val []byte) error {
		xf, err := vellum.Load(val)
		if err != nil {
			return err
		}
		itr, err := xf.Iterator(nil, nil)
		for err == nil {
			k, _ := itr.Current()
			xe := f(k)
			if xe != nil {
				return xe
			}
			err = itr.Next()
		}
		return nil
	})
}
