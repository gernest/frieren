package predicate

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/blevesearch/vellum"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/fields"
	"github.com/gernest/frieren/internal/fst"
	"github.com/gernest/frieren/internal/keys"
	"github.com/gernest/rbf"
	"github.com/gernest/roaring"
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
		return f.applyEqual(ctx)
	case traceql.OpNotEqual:
		return f.applyNotEqual(ctx)
	case traceql.OpRegex:
		return f.applyRegex(ctx)
	case traceql.OpNotRegex:
		return f.applyNotRegex(ctx)
	case traceql.OpLess:
		return f.applyLess(ctx, false)
	case traceql.OpLessEqual:
		return f.applyLess(ctx, true)
	case traceql.OpGreater:
		return f.applyGreater(ctx, false)
	case traceql.OpGreaterEqual:
		return f.applyGreater(ctx, true)
	}
	return rows.NewRow(), nil
}

func (s *Strings) applyNotRegex(ctx *Context) (*rows.Row, error) {
	field := s.Predicates[0].field
	b := new(bytes.Buffer)
	key := keys.FST(b, field, ctx.Shard, ctx.View)
	it, err := ctx.Txn.Get(key)
	if err != nil {
		return nil, fmt.Errorf("reading fst %s %w", b.String(), err)
	}
	f := fields.New(field, ctx.Shard, ctx.View)
	filters := make([]roaring.BitmapFilter, 0, 16)

	o := roaring64.New()
	r := roaring64.New()
	all := roaring64.New()
	err = f.RowsBitmap(ctx.Tx, 0, all)
	if err != nil {
		return nil, err
	}
	if all.IsEmpty() {
		return rows.NewRow(), nil
	}

	err = it.Value(func(val []byte) error {
		xf, err := vellum.Load(val)
		if err != nil {
			return err
		}
		for i, v := range s.Predicates {
			re, err := fst.Compile(b, v.key, v.value)
			if err != nil {
				return err
			}
			filters = filters[:0]
			itr, err := xf.Search(re, nil, nil)
			for err == nil {
				_, value := itr.Current()
				filters = append(filters, roaring.NewBitmapColumnFilter(value))
				err = itr.Next()
			}
			o.Clear()
			err = f.RowsBitmap(ctx.Tx, 0, o, filters...)
			if err != nil {
				return err
			}
			clone := all.Clone()
			clone.AndNot(o)
			if i == 0 {
				r.Or(o)
				continue
			}
			if s.All {
				r.And(o)
				if r.IsEmpty() {
					return nil
				}
				continue
			}
			r.Or(o)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if o.IsEmpty() {
		return rows.NewRow(), nil
	}
	return rows.NewRow(r.ToArray()...), nil
}

func (s *Strings) applyGreater(ctx *Context, equal bool) (*rows.Row, error) {
	field := s.Predicates[0].field
	b := new(bytes.Buffer)
	key := keys.FST(b, field, ctx.Shard, ctx.View)
	it, err := ctx.Txn.Get(key)
	if err != nil {
		return nil, fmt.Errorf("reading fst %s %w", b.String(), err)
	}
	f := fields.New(field, ctx.Shard, ctx.View)
	filters := make([]roaring.BitmapFilter, 0, 16)

	o := roaring64.New()
	r := roaring64.New()
	err = it.Value(func(val []byte) error {
		xf, err := vellum.Load(val)
		if err != nil {
			return err
		}
		for i, v := range s.Predicates {
			b.Reset()
			b.WriteString(v.key)
			b.WriteByte('=')
			prefix := bytes.Clone(b.Bytes())
			b.WriteString(v.value)
			filters = filters[:0]
			itr, err := xf.Search(&vellum.AlwaysMatch{}, key, nil)
			match := true
			start := true
			for err == nil && match {
				currKey, value := itr.Current()
				match = bytes.HasPrefix(currKey, prefix)
				err = itr.Next()
				if start && !equal {
					start = false
					// Start key is inclusive
					continue
				}
				filters = append(filters, roaring.NewBitmapColumnFilter(value))
			}
			o.Clear()
			err = f.RowsBitmap(ctx.Tx, 0, o, filters...)
			if err != nil {
				return err
			}
			if i == 0 {
				r.Or(o)
				continue
			}
			if s.All {
				r.And(o)
				if r.IsEmpty() {
					return nil
				}
				continue
			}
			r.Or(o)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if o.IsEmpty() {
		return rows.NewRow(), nil
	}
	return rows.NewRow(r.ToArray()...), nil
}

func (s *Strings) applyLess(ctx *Context, equal bool) (*rows.Row, error) {
	field := s.Predicates[0].field
	b := new(bytes.Buffer)
	key := keys.FST(b, field, ctx.Shard, ctx.View)
	it, err := ctx.Txn.Get(key)
	if err != nil {
		return nil, fmt.Errorf("reading fst %s %w", b.String(), err)
	}
	f := fields.New(field, ctx.Shard, ctx.View)
	filters := make([]roaring.BitmapFilter, 0, 16)

	o := roaring64.New()
	r := roaring64.New()
	err = it.Value(func(val []byte) error {
		xf, err := vellum.Load(val)
		if err != nil {
			return err
		}
		for i, v := range s.Predicates {
			b.Reset()
			b.WriteString(v.key)
			b.WriteByte('=')
			prefix := bytes.Clone(b.Bytes())
			b.WriteString(v.value)
			filters = filters[:0]
			itr, err := xf.Search(&vellum.AlwaysMatch{}, prefix, b.Bytes())
			for err == nil {
				_, value := itr.Current()
				filters = append(filters, roaring.NewBitmapColumnFilter(value))
				err = itr.Next()
			}
			if equal {
				// end key is exclusive
				value, ok, err := xf.Get(b.Bytes())
				if err != nil {
					return fmt.Errorf("reading key from fst %w", err)
				}
				if ok {
					filters = append(filters, roaring.NewBitmapColumnFilter(value))
				}
			}
			o.Clear()
			err = f.RowsBitmap(ctx.Tx, 0, o, filters...)
			if err != nil {
				return err
			}
			if i == 0 {
				r.Or(o)
				continue
			}
			if s.All {
				r.And(o)
				if r.IsEmpty() {
					return nil
				}
				continue
			}
			r.Or(o)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if o.IsEmpty() {
		return rows.NewRow(), nil
	}
	return rows.NewRow(r.ToArray()...), nil
}

func (s *Strings) applyRegex(ctx *Context) (*rows.Row, error) {
	field := s.Predicates[0].field
	b := new(bytes.Buffer)
	key := keys.FST(b, field, ctx.Shard, ctx.View)
	it, err := ctx.Txn.Get(key)
	if err != nil {
		return nil, fmt.Errorf("reading fst %s %w", b.String(), err)
	}
	f := fields.New(field, ctx.Shard, ctx.View)
	filters := make([]roaring.BitmapFilter, 0, 16)

	o := roaring64.New()
	r := roaring64.New()
	err = it.Value(func(val []byte) error {
		xf, err := vellum.Load(val)
		if err != nil {
			return err
		}
		for i, v := range s.Predicates {
			re, err := fst.Compile(b, v.key, v.value)
			if err != nil {
				return err
			}
			filters = filters[:0]
			itr, err := xf.Search(re, nil, nil)
			for err == nil {
				_, value := itr.Current()
				filters = append(filters, roaring.NewBitmapColumnFilter(value))
				err = itr.Next()
			}
			o.Clear()
			err = f.RowsBitmap(ctx.Tx, 0, o, filters...)
			if err != nil {
				return err
			}
			if i == 0 {
				r.Or(o)
				continue
			}
			if s.All {
				r.And(o)
				if r.IsEmpty() {
					return nil
				}
				continue
			}
			r.Or(o)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if o.IsEmpty() {
		return rows.NewRow(), nil
	}
	return rows.NewRow(r.ToArray()...), nil
}

func (s *Strings) applyNotEqual(ctx *Context) (*rows.Row, error) {
	field := s.Predicates[0].field
	buf := new(bytes.Buffer)
	f := fields.New(field, ctx.Shard, ctx.View)
	ids := make([]uint64, 0, len(s.Predicates))
	for _, v := range s.Predicates {
		buf.Reset()
		buf.WriteString(v.key)
		buf.WriteByte('=')
		buf.WriteString(v.value)
		id, ok := ctx.Find(field, buf.Bytes())
		if !ok {
			return rows.NewRow(), nil
		}
		ids = append(ids, id)
	}
	all := roaring64.New()
	err := f.RowsBitmap(ctx.Tx, 0, all)
	if err != nil {
		return nil, err
	}
	if s.All {
		filters := make([]roaring.BitmapFilter, 0, len(ids))
		for i := range ids {
			filters[i] = roaring.NewBitmapColumnFilter(ids[i])
		}
		matchBitmap := roaring64.New()
		// Find all rows that match
		err = f.RowsBitmap(ctx.Tx, 0, matchBitmap, filters...)
		if err != nil {
			return nil, err
		}
		all.AndNot(matchBitmap)
		return rows.NewRow(all.ToArray()...), nil
	}
	matchBitmap := roaring64.New()
	r := roaring64.New()
	for i := range ids {
		matchBitmap.Clear()
		err = f.RowsBitmap(ctx.Tx, 0, matchBitmap, roaring.NewBitmapColumnFilter(ids[i]))
		if err != nil {
			return nil, err
		}
		clone := all.Clone()
		clone.AndNot(matchBitmap)
		r.Or(clone)
	}
	return rows.NewRow(r.ToArray()...), nil
}

func (s *Strings) applyEqual(ctx *Context) (*rows.Row, error) {
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
	if len(ids) == 0 {
		return rows.NewRow(), nil
	}
	f := fields.New(field, ctx.Shard, ctx.View)
	if s.All {
		// faster path
		filters := make([]roaring.BitmapFilter, 0, len(ids))
		for i := range ids {
			filters[i] = roaring.NewBitmapColumnFilter(ids[i])
		}
		match, err := f.Rows(ctx.Tx, 0, filters...)
		if err != nil {
			return nil, err
		}
		return rows.NewRow(match...), nil
	}
	r := rows.NewRow()

	for i := range ids {
		x, err := f.EqSet(ctx.Tx, ids[i])
		if err != nil {
			return nil, err
		}
		if i == 0 {
			r = x
			continue
		}
		r = r.Union(x)
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