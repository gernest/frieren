package query

import (
	"slices"
	"time"

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
		mx, err = predicate.NewLabelValues(field, name, matchers...)
		if err != nil {
			return nil, err
		}
	}
	o := map[string]struct{}{}
	err := Query(db, resource, start, end, func(view *store.View) error {
		r, err := mx.Match(view)
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
	if len(o) == 0 {
		return []string{}, nil
	}
	result := make([]string, 0, len(o))
	for k := range o {
		result = append(result, k)
	}
	slices.Sort(result)
	return result, nil
}
