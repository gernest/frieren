package predicate

import (
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/frieren/internal/blob"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/rbf"
	"github.com/gernest/rows"
	"github.com/grafana/tempo/pkg/traceql"
)

type Context struct {
	Shard  uint64
	View   string
	Tx     *rbf.Tx
	Txn    *badger.Txn
	Find   blob.Find
	Tr     blob.Tr
	TrCall blob.TrCall
}

type Predicate interface {
	Apply(ctx *Context) (*rows.Row, error)
	Extract(_ *Context) (*roaring64.Bitmap, error)
	Index() int
}

type String struct {
	key, value string
	field      constants.ID
	op         traceql.Operator
}

var _ Predicate = (*String)(nil)

func (f *String) Index() int {
	return int(f.field) + int(f.op)
}

func (f *String) Extract(_ *Context) (*roaring64.Bitmap, error) {
	return roaring64.New(), nil
}

func NewString(field constants.ID, op traceql.Operator, key, value string) *String {
	return &String{field: field, key: key, value: value, op: op}
}

func (f *String) Apply(ctx *Context) (*rows.Row, error) {
	return rows.NewRow(), nil
}

var strings = map[traceql.Operator]struct{}{
	traceql.OpEqual:        {},
	traceql.OpNotEqual:     {},
	traceql.OpRegex:        {},
	traceql.OpNotRegex:     {},
	traceql.OpLess:         {},
	traceql.OpLessEqual:    {},
	traceql.OpGreater:      {},
	traceql.OpGreaterEqual: {},
}

func ValidForStrings(o traceql.Operator) bool {
	_, ok := strings[o]
	return ok
}

type Int struct {
	field constants.ID
	op    traceql.Operator
	value uint64
}

var _ Predicate = (*Int)(nil)

func (f *Int) Index() int {
	return int(f.field) + int(f.op)
}

func (f *Int) Extract(_ *Context) (*roaring64.Bitmap, error) {
	return roaring64.New(), nil
}

func (f *Int) Apply(ctx *Context) (*rows.Row, error) {
	return rows.NewRow(), nil
}

var ints = map[traceql.Operator]struct{}{
	traceql.OpEqual:        {},
	traceql.OpNotEqual:     {},
	traceql.OpGreater:      {},
	traceql.OpGreaterEqual: {},
	traceql.OpLess:         {},
	traceql.OpLessEqual:    {},
}

func ValidForInts(o traceql.Operator) bool {
	_, ok := ints[o]
	return ok
}

func NewInt(field constants.ID, op traceql.Operator, value uint64) *Int {
	return &Int{field: field, op: op, value: value}
}

type And []Predicate

var _ Predicate = (*And)(nil)

func (f And) Index() int {
	return 0
}

func (f And) Apply(ctx *Context) (*rows.Row, error) {
	if len(f) == 0 {
		return rows.NewRow(), nil
	}
	if len(f) == 1 {
		return f[0].Apply(ctx)
	}
	r := rows.NewRow()
	for i := range f {
		x, err := f[i].Apply(ctx)
		if err != nil {
			return nil, err
		}
		if x.IsEmpty() {
			return x, nil
		}
		if i == 0 {
			r = x
			continue
		}
		r = r.Intersect(x)
		if r.IsEmpty() {
			return r, nil
		}
	}
	return r, nil
}

func (f And) Extract(ctx *Context) (*roaring64.Bitmap, error) {
	if len(f) == 0 {
		return roaring64.New(), nil
	}
	if len(f) == 1 {
		return f[0].Extract(ctx)
	}
	r := roaring64.New()
	for i := range f {
		x, err := f[i].Extract(ctx)
		if err != nil {
			return nil, err
		}
		if x.IsEmpty() {
			return x, nil
		}
		if i == 0 {
			r = x
			continue
		}
		r.And(x)
		if r.IsEmpty() {
			return r, nil
		}
	}
	return r, nil
}

type Or []Predicate

var _ Predicate = (*Or)(nil)

func (f Or) Index() int {
	return 0
}

func (f Or) Apply(ctx *Context) (*rows.Row, error) {
	if len(f) == 0 {
		return rows.NewRow(), nil
	}
	if len(f) == 1 {
		return f[0].Apply(ctx)
	}
	r := rows.NewRow()
	for i := range f {
		x, err := f[i].Apply(ctx)
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
func (f Or) Extract(ctx *Context) (*roaring64.Bitmap, error) {
	if len(f) == 0 {
		return roaring64.NewBitmap(), nil
	}
	if len(f) == 1 {
		return f[0].Extract(ctx)
	}
	r := roaring64.New()
	for i := range f {
		x, err := f[i].Extract(ctx)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			r = x
			continue
		}
		r.Or(x)
	}
	return r, nil
}
