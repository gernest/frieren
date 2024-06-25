package predicate

import (
	"bytes"
	"fmt"

	"github.com/blevesearch/vellum"
	re "github.com/blevesearch/vellum/regexp"
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
		return []Predicate{}
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
	return &Labels{
		field:    field,
		matchers: matches,
	}
}

func NewLabelValues(field constants.ID, name string, matches ...*labels.Matcher) (*Labels, error) {
	rx, err := re.New(name + "=.*")
	if err != nil {
		return nil, err
	}
	return &Labels{
		field:    field,
		name:     name,
		re:       rx,
		matchers: matches,
	}, nil
}

type Labels struct {
	matchers []*labels.Matcher
	name     string
	re       *re.Regexp
	field    constants.ID
}

var sep = []byte("=")

func (l *Labels) Match(ctx *Context) (result map[string]struct{}, err error) {
	result = make(map[string]struct{})
	if l.name != "" {
		err = l.fstName(ctx, func(val []byte) error {
			_, value, _ := bytes.Cut(val, sep)
			if len(l.matchers) == 0 {
				result[string(value)] = struct{}{}
				return nil
			}
			str := string(value)
			for _, m := range l.matchers {
				if !m.Matches(str) {
					return nil
				}
			}
			result[str] = struct{}{}
			return nil
		})
		return
	}
	err = l.fst(ctx, func(val []byte) error {
		fmt.Println(string(val))
		key, value, _ := bytes.Cut(val, sep)
		if len(l.matchers) == 0 {
			result[string(key)] = struct{}{}
			return nil
		}
		str := string(value)
		for _, m := range l.matchers {
			if !m.Matches(str) {
				return nil
			}
		}
		result[str] = struct{}{}
		return nil
	})
	return
}

func (l *Labels) fstName(ctx *Context, f func(val []byte) error) error {
	key := keys.FST(l.field, ctx.Shard.Id, ctx.View)
	it, err := ctx.Txn().Get(key)
	if err != nil {
		return fmt.Errorf("reading fst %s %w", string(key), err)
	}
	return it.Value(func(val []byte) error {
		xf, err := vellum.Load(val)
		if err != nil {
			return fmt.Errorf("loading vellum fst %w", err)
		}
		prefix := []byte(l.name + "=")
		itr, err := xf.Search(l.re, prefix, nil)
		for err == nil {
			k, _ := itr.Current()
			if !bytes.HasPrefix(k, prefix) {
				return nil
			}
			xe := f(k)
			if xe != nil {
				return xe
			}
			err = itr.Next()
		}
		return nil
	})
}

func (l *Labels) fst(ctx *Context, f func(val []byte) error) error {
	key := keys.FST(l.field, ctx.Shard.Id, ctx.View)
	it, err := ctx.Txn().Get(key)
	if err != nil {
		return fmt.Errorf("reading fst %s %w", string(key), err)
	}
	return it.Value(func(val []byte) error {
		xf, err := vellum.Load(val)
		if err != nil {
			return fmt.Errorf("loading fst %w", err)
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
