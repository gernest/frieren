package predicate

import (
	"github.com/gernest/rows"
)

// Collections of strings with the same field and operator.
type Strings struct {
	Predicates []*String
	All        bool
}

var _ Predicate = (*Strings)(nil)

func (f *Strings) Index() int {
	return f.Predicates[0].Index()
}

func (f *Strings) Apply(ctx *Context) (*rows.Row, error) {
	return rows.NewRow(), nil
}
