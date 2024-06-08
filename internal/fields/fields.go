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

type View struct {
	ID    ID
	Shard uint64
	View  string
}

func (v *View) WithShard(shard uint64) *View {
	return &View{
		ID:    v.ID,
		Shard: shard,
		View:  v.View,
	}
}

func (v *View) String() string {
	return fmt.Sprintf("%d%s_%d", v.ID, v.View, v.Shard)
}
