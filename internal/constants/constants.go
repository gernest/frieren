package constants

type ID uint

const (
	MetricsValue = iota + 1
	MetricsHistogram
	MetricsTimestamp
	MetricsSeries
	MetricsLabels
	MetricsExemplars
	MetricsRow
	MetricsFST

	LogsRow
	LogsStreamID
	LogsLabels
	LogsTimestamp
	LogsLine
	LogsMetadata
	LogsFST

	TracesLabels
	TraceFST
)
