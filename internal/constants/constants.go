package constants

type ID uint

const (
	MetricsValue = iota + 1
	MetricsHistogram
	MetricsTimestamp
	MetricsSeries
	MetricsLabels
	MetricsExemplars
	MetricsShards
	MetricsFSTBitmap
	MetricsFST
	MetricsRow

	LogsLabels

	TracesLabels
)
