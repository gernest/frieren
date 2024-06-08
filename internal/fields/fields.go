package fields

import "fmt"

type ID uint

const (
	MetricsValue = iota + (1 << 10)
	MetricsKind
	MetricsTimestamp
	MetricsSeries
	MetricsLabels
	MetricsExemplars
	MetricsExists
	MetricsShards
	sep
)

type Fragment struct {
	ID    ID
	Shard uint64
	View  string
}

func (v *Fragment) WithShard(shard uint64) *Fragment {
	return &Fragment{
		ID:    v.ID,
		Shard: shard,
		View:  v.View,
	}
}

func (v *Fragment) String() string {
	return fmt.Sprintf("%d%s_%d", v.ID, v.View, v.Shard)
}
