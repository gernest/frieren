package constants

type ID uint

const (
	MetricsValue = iota + 1
	MetricsHistogram
	MetricsTimestamp
	MetricsSeries
	MetricsLabels
	MetricsExemplars
	// Used to generate unique row ID for metrics
	MetricsRow

	LogsLabels

	TracesLabels
)
