package query

import (
	"fmt"
	"slices"
	"time"

	"github.com/dgraph-io/badger/v4"
	v1 "github.com/gernest/frieren/gen/go/fri/v1"
	"github.com/gernest/frieren/internal/batch"
	"github.com/gernest/frieren/internal/constants"
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/quantum"
)

type View struct {
	info  []*v1.FieldViewInfo
	views []string
}

func New(txn *badger.Txn, tx *rbf.Tx, resource constants.Resource, start, end int64) (*View, error) {
	var views []string
	if date(start).Equal(date(end)) {
		// Same day generate a single view
		views = []string{quantum.ViewByTimeUnit("", time.UnixMilli(start), 'D')}
	} else {
		// We want view that might contain maxts to be included too, we need to add
		// extra date
		views = quantum.ViewsByTimeRange("",
			time.UnixMilli(start), time.UnixMilli(end).AddDate(0, 0, 1),
			quantum.TimeQuantum("D"))
	}
	ids := make([]string, 0, len(views))
	shards := make([]*v1.FieldViewInfo, 0, len(views))
	for i := range views {
		info, err := batch.FieldViewInfo(txn, resource, views[i])
		if err != nil {
			return nil, fmt.Errorf("reading view info   %w", err)
		}
		if len(info.Shards) == 0 {
			continue
		}
		ids = append(ids, views[i])
		shards = append(shards, info)
	}
	return &View{info: shards, views: ids}, nil
}

func (s *View) IsEmpty() bool {
	return len(s.views) == 0
}

func (s *View) Traverse(f func(shard *v1.Shard, view string) error) error {
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

func date(ts int64) time.Time {
	y, m, d := time.UnixMilli(ts).Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}
