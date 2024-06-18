package predicate

import (
	"bytes"
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/blevesearch/vellum"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/fields"
	"github.com/gernest/frieren/internal/fst"
	"github.com/gernest/frieren/internal/keys"
	"github.com/gernest/roaring"
	"github.com/gernest/rows"
	"github.com/grafana/tempo/pkg/traceql"
)

// Collections of strings with the same field and operator.
type Strings struct {
	Predicates []*String
	All        bool
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

func (f *Ints) Apply(ctx *Context) (*rows.Row, error) {
	return rows.NewRow(), nil
}

// Optimize groups predicates with same field and operator
func Optimize(a []Predicate, all bool) []Predicate {
	ints := make(map[constants.ID]map[traceql.Operator][]*Int)
	strings := make(map[constants.ID]map[traceql.Operator][]*String)
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
		}
	}
	o := make([]Predicate, 0, len(ints)+len(strings))
	for _, v := range ints {
		for _, e := range v {
			o = append(o, &Ints{
				Predicates: e, All: all,
			})
		}
	}
	for _, v := range strings {
		for _, e := range v {
			o = append(o, &Strings{
				Predicates: e, All: all,
			})
		}
	}
	return o
}
