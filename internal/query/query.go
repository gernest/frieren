package query

import (
	"fmt"
	"slices"
	"time"

	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/gernest/frieren/internal/batch"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/frieren/internal/store"
	"github.com/gernest/rbf/quantum"
)

type xView struct {
	info  []*v1.FieldViewInfo
	views []string
}

type Callback func(view *store.View) error

func Query(db *store.Store, resource constants.Resource, start, end time.Time, f Callback) error {
	return db.View(func(tx *store.Tx) error {
		views, err := newView(tx, resource, start, end)
		if err != nil {
			return err
		}
		return views.Traverse(func(shard *v1.Shard, view string) error {
			return f(tx.View(shard, view))
		})
	})
}

func newView(tx *store.Tx, resource constants.Resource, start, end time.Time) (*xView, error) {
	var views []string
	if date(start).Equal(date(end)) {
		// Same day generate a single view
		views = []string{quantum.ViewByTimeUnit("", start, 'D')}
	} else {
		// We want view that might contain maxts to be included too, we need to add
		// extra date
		views = quantum.ViewsByTimeRange("",
			start, end.AddDate(0, 0, 1),
			quantum.TimeQuantum("D"))
	}
	ids := make([]string, 0, len(views))
	shards := make([]*v1.FieldViewInfo, 0, len(views))
	for i := range views {
		info, err := batch.FieldViewInfo(tx.Txn(), resource, views[i])
		if err != nil {
			return nil, fmt.Errorf("reading view info   %w", err)
		}
		if len(info.Shards) == 0 {
			continue
		}
		ids = append(ids, views[i])
		shards = append(shards, info)
	}
	return &xView{info: shards, views: ids}, nil
}

func (s *xView) IsEmpty() bool {
	return len(s.views) == 0
}

func (s *xView) Iter() *Iter {
	return &Iter{s: s, pos: -1}
}

func (s *xView) Traverse(f func(shard *v1.Shard, view string) error) error {
	if s.IsEmpty() {
		return nil
	}
	shards := make([]uint64, 0, 64)
	for i := range s.views {
		info := s.info[i]
		// we traverse shards in order. This ensures that data will always be
		// processed in ascending order and saves the need to sort.
		shards = shards[:0]
		for x := range info.Shards {
			shards = append(shards, x)
		}
		slices.Sort(shards)

		for j := range shards {
			err := f(info.Shards[shards[j]], s.views[i])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type Iter struct {
	s      *xView
	shards []uint64
	pos    int
}

func (v *Iter) Next() bool {
	if v.pos < len(v.s.views) {
		v.pos++
		return true
	}
	return false
}

func (v *Iter) Traverse(f func(shard *v1.Shard, view string) error) error {
	v.shards = v.shards[:0]
	info := v.s.info[v.pos]
	for k := range info.Shards {
		v.shards = append(v.shards, k)
	}
	slices.Sort(v.shards)

	for j := range v.shards {
		err := f(info.Shards[v.shards[j]], v.s.views[v.pos])
		if err != nil {
			return err
		}
	}
	return nil
}

func date(ts time.Time) time.Time {
	y, m, d := ts.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}
