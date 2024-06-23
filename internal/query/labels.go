package query

import (
	"slices"
	"time"

	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/predicate"
	"github.com/gernest/frieren/internal/store"
	"github.com/prometheus/prometheus/model/labels"
)

func Labels(db *store.Store, resource constants.Resource,
	field constants.ID,
	start, end time.Time, name string, matchers ...*labels.Matcher) ([]string, error) {
	mx := predicate.NewLabels(field, matchers...)
	if name != "" {
		var err error
		mx, err = predicate.NewLabelValues(constants.LogsLabels, name, matchers...)
		if err != nil {
			return nil, err
		}
	}
	txn := db.DB.NewTransaction(false)
	defer txn.Discard()
	tx, err := db.Index.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	view, err := New(txn, tx, resource, start, end)
	if err != nil {
		return nil, err
	}
	if view.IsEmpty() {
		return []string{}, nil
	}
	o := map[string]struct{}{}
	err = view.Traverse(func(shard *v1.Shard, view string) error {
		ctx := predicate.NewContext(shard, view, db, tx, txn)
		r, err := mx.Match(ctx)
		if err != nil {
			return err
		}
		for k := range r {
			o[k] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(o))
	for k := range o {
		result = append(result, k)
	}
	slices.Sort(result)
	return result, nil
}
