package predicate

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"

	str "strings"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/blevesearch/vellum"
	re "github.com/blevesearch/vellum/regexp"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/fields"
	"github.com/gernest/frieren/internal/keys"
	"github.com/gernest/rbf"
	"github.com/gernest/rows"
	"github.com/grafana/tempo/pkg/traceql"
)

// Collections of strings with the same field and operator.
type Strings struct {
	Predicates []*String
	All        bool
}

func (f *Strings) Index() int {
	return f.Predicates[0].Index()
}

func (f *Strings) Apply(ctx *Context) (*rows.Row, error) {
	switch f.Predicates[0].op {
	case traceql.OpEqual:
		return f.EQ(ctx)
	case traceql.OpNotEqual:
		return f.NEQ(ctx)
	case traceql.OpRegex:
		return f.RE(ctx)
	case traceql.OpNotRegex:
		return f.NRE(ctx)
	case traceql.OpLess:
		return f.LT(ctx)
	case traceql.OpLessEqual:
		return f.LTE(ctx)
	case traceql.OpGreater:
		return f.GT(ctx)
	case traceql.OpGreaterEqual:
		return f.GTE(ctx)
	}
	return rows.NewRow(), nil
}

func (s *Strings) GTE(ctx *Context) (*rows.Row, error) {
	b := new(bytes.Buffer)
	return s.applyFST(ctx, func(p *String) (opts fstOptions, err error) {
		b.WriteString(p.key)
		b.WriteByte('=')
		prefix := bytes.Clone(b.Bytes())
		b.WriteString(p.value)
		full := bytes.Clone(b.Bytes())
		return fstOptions{
			a:     &vellum.AlwaysMatch{},
			start: full,
			end:   nil,
			match: func(b []byte) bool {
				return bytes.HasPrefix(b, prefix)
			},
		}, nil
	})
}
func (s *Strings) GT(ctx *Context) (*rows.Row, error) {
	b := new(bytes.Buffer)
	return s.applyFST(ctx, func(p *String) (opts fstOptions, err error) {
		b.WriteString(p.key)
		b.WriteByte('=')
		prefix := bytes.Clone(b.Bytes())
		b.WriteString(p.value)
		full := bytes.Clone(b.Bytes())
		return fstOptions{
			a:     &vellum.AlwaysMatch{},
			start: full,
			end:   nil,
			match: func(b []byte) bool {
				return bytes.HasPrefix(b, prefix)
			},
			after: func(it *vellum.FST, b *roaring64.Bitmap, err error) {
				v, ok, _ := it.Get(full)
				if ok {
					b.Remove(v)
				}
			},
		}, nil
	})
}

func (s *Strings) LT(ctx *Context) (*rows.Row, error) {
	b := new(bytes.Buffer)
	return s.applyFST(ctx, func(p *String) (opts fstOptions, err error) {
		b.WriteString(p.key)
		b.WriteByte('=')
		prefix := bytes.Clone(b.Bytes())
		b.WriteString(p.value)
		full := bytes.Clone(b.Bytes())
		return fstOptions{
			a:     &vellum.AlwaysMatch{},
			start: prefix,
			end:   full,
			match: func(b []byte) bool {
				return bytes.HasPrefix(b, prefix)
			},
		}, nil
	})
}

func (s *Strings) LTE(ctx *Context) (*rows.Row, error) {
	b := new(bytes.Buffer)
	return s.applyFST(ctx, func(p *String) (opts fstOptions, err error) {
		b.WriteString(p.key)
		b.WriteByte('=')
		prefix := bytes.Clone(b.Bytes())
		b.WriteString(p.value)
		full := bytes.Clone(b.Bytes())
		return fstOptions{
			a:     &vellum.AlwaysMatch{},
			start: prefix,
			end:   full,
			match: func(b []byte) bool {
				return bytes.HasPrefix(b, prefix)
			},
			after: func(it *vellum.FST, b *roaring64.Bitmap, err error) {
				if err != nil {
					return
				}
				v, ok, _ := it.Get(full)
				if ok {
					b.Add(v)
				}
			},
		}, nil
	})
}

func (s *Strings) NRE(ctx *Context) (*rows.Row, error) {
	field := s.Predicates[0].field
	f := fields.New(field, ctx.Shard, ctx.View)
	exists, err := f.ExistsSet(ctx.Tx)
	if err != nil {
		return nil, err
	}
	if exists.IsEmpty() {
		return exists, nil
	}
	r, err := s.RE(ctx)
	if err != nil {
		return nil, err
	}
	return exists.Difference(r), nil
}

func (s *Strings) RE(ctx *Context) (*rows.Row, error) {
	b := new(bytes.Buffer)
	return s.applyFST(ctx, func(p *String) (opts fstOptions, err error) {
		re, err := Compile(b, p.key, p.value)
		if err != nil {
			return fstOptions{}, err
		}
		return fstOptions{a: re, match: always}, nil
	})
}

func Compile(b *bytes.Buffer, key, value string) (*re.Regexp, error) {
	value = str.TrimPrefix(value, "^")
	value = str.TrimSuffix(value, "$")
	b.Reset()
	b.WriteString(key)
	b.WriteByte('=')
	b.WriteString(value)
	return re.New(b.String())
}

type fstPredicate func(p *String) (opts fstOptions, err error)

type fstOptions struct {
	a          vellum.Automaton
	start, end []byte
	before     func(it *vellum.FST, b *roaring64.Bitmap, err error)
	after      func(it *vellum.FST, b *roaring64.Bitmap, err error)
	match      func([]byte) bool
}

func always(_ []byte) bool { return true }

func (s *Strings) applyFST(ctx *Context, fp fstPredicate) (*rows.Row, error) {
	o, err := s.matchFST(ctx, fp)
	if err != nil {
		return nil, err
	}
	if o.IsEmpty() {
		return rows.NewRow(), nil
	}
	return s.apply(ctx, o.ToArray())
}

func (s *Strings) matchFST(ctx *Context, fp fstPredicate) (*roaring64.Bitmap, error) {
	field := s.Predicates[0].field
	b := new(bytes.Buffer)
	key := keys.FST(b, field, ctx.Shard, ctx.View)
	it, err := ctx.Txn.Get(key)
	if err != nil {
		return nil, fmt.Errorf("reading fst %s %w", b.String(), err)
	}
	o := roaring64.New()
	err = it.Value(func(val []byte) error {
		xf, err := vellum.Load(val)
		if err != nil {
			return err
		}
		for _, v := range s.Predicates {
			a, err := fp(v)
			if err != nil {
				return err
			}
			itr, err := xf.Search(a.a, a.start, a.end)
			if a.before != nil {
				a.before(xf, o, err)
			}
			for err == nil {
				key, value := itr.Current()
				if !a.match(key) {
					break
				}
				o.Add(value)
				err = itr.Next()
			}
			if a.after != nil {
				a.after(xf, o, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return o, nil
}

func (s *Strings) NEQ(ctx *Context) (*rows.Row, error) {
	field := s.Predicates[0].field
	f := fields.New(field, ctx.Shard, ctx.View)
	r, err := s.EQ(ctx)
	if err != nil {
		return nil, err
	}
	all, err := f.ExistsSet(ctx.Tx)
	if err != nil {
		return nil, err
	}
	return all.Difference(r), nil
}

func (s *Strings) EQ(ctx *Context) (*rows.Row, error) {
	field := s.Predicates[0].field
	buf := new(bytes.Buffer)
	ids := make([]uint64, 0, len(s.Predicates))
	for _, v := range s.Predicates {
		buf.Reset()
		buf.WriteString(v.key)
		buf.WriteByte('=')
		buf.WriteString(v.value)
		id, ok := ctx.Find(field, buf.Bytes())
		if !ok {
			if s.All {
				return rows.NewRow(), nil
			}
			continue
		}
		ids = append(ids, id)
	}
	return s.apply(ctx, ids)
}

func (s *Strings) apply(ctx *Context, columns []uint64) (*rows.Row, error) {
	field := s.Predicates[0].field
	f := fields.New(field, ctx.Shard, ctx.View)
	r := rows.NewRow()
	for _, id := range columns {
		rx, err := f.Row(ctx.Tx, id)
		if err != nil {
			return nil, err
		}
		if rx.IsEmpty() && s.All {
			return rx, nil
		}
		if r.IsEmpty() {
			r = rx
			continue
		}
		if s.All {
			r = r.Intersect(rx)
			if r.IsEmpty() {
				return r, nil
			}
			continue
		}
		r = r.Union(rx)
	}
	return r, nil
}

// Ints is a collection of strings with the same field and operator
type Ints struct {
	Predicates []*Int
	All        bool
}

func (f *Ints) Index() int {
	return f.Predicates[0].Index()
}

func (f *Ints) Apply(ctx *Context) (*rows.Row, error) {
	field := f.Predicates[0].field
	fx := fields.New(field, ctx.Shard, ctx.View)
	switch f.Predicates[0].op {
	case traceql.OpEqual:
		return f.apply(ctx, fx.EqBSI)
	case traceql.OpNotEqual:
		return f.apply(ctx, fx.NotEqBSI)
	case traceql.OpGreater:
		return f.apply(ctx, fx.GTBSI)
	case traceql.OpGreaterEqual:
		return f.apply(ctx, fx.GTEBSI)
	case traceql.OpLess:
		return f.apply(ctx, fx.LTBSI)
	case traceql.OpLessEqual:
		return f.apply(ctx, fx.LTEBSI)
	}
	return rows.NewRow(), nil
}

func (f *Ints) apply(ctx *Context, fn func(*rbf.Tx, uint64) (*rows.Row, error)) (*rows.Row, error) {
	r := rows.NewRow()
	for i, v := range f.Predicates {
		rx, err := fn(ctx.Tx, v.value)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			r = rx
			if f.All && r.IsEmpty() {
				break
			}
			continue
		}
		if f.All {
			r = r.Intersect(rx)
			if r.IsEmpty() {
				break
			}
		} else {
			r = r.Union(rx)
		}
	}
	return r, nil
}

// Optimize groups predicates with same field and operator
func Optimize(a []Predicate, all bool) []Predicate {
	ints := make(map[constants.ID]map[traceql.Operator][]*Int)
	strings := make(map[constants.ID]map[traceql.Operator][]*String)
	var o []Predicate

	for _, v := range a {
		switch e := v.(type) {
		case *String:
			x, ok := strings[e.field]
			if !ok {
				x = make(map[traceql.Operator][]*String)
				strings[e.field] = x
			}
			x[e.op] = append(x[e.op], e)
		case *Int:
			x, ok := ints[e.field]
			if !ok {
				x = make(map[traceql.Operator][]*Int)
				ints[e.field] = x
			}
			x[e.op] = append(x[e.op], e)
		default:
			o = append(o, e)
		}
	}
	for _, v := range ints {
		for _, e := range v {
			slices.SortFunc(e, compare)
			o = append(o, &Ints{
				Predicates: e, All: all,
			})
		}
	}
	for _, v := range strings {
		for _, e := range v {
			slices.SortFunc(e, compare)
			o = append(o, &Strings{
				Predicates: e, All: all,
			})
		}
	}
	slices.SortFunc(o, compare)
	return o
}

func compare[T Predicate](a, b T) int {
	return cmp.Compare(a.Index(), b.Index())
}
