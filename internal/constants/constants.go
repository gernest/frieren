package constants

type ID uint

const (
	MetricsValue = iota + 1
	MetricsTimestamp
	MetricsLabels
	MetricsHistogram
	MetricsSeries
	MetricsExemplars
	MetricsRow
	MetricsFST

	LogsTimestamp
	LogsStreamID
	LogsLabels
	LogsLine
	LogsMetadata
	LogsFST
	LogsRow

	TracesResource
	TracesScope
	TracesSpan
	TracesStart
	TracesEnd
	TracesSpanStart
	TracesSpanEnd
	TracesDuration
	TracesSpanDuration
	TracesTags
	TracesRow
	TracesFST

	LastID
)

type Resource uint

const (
	METRICS Resource = iota
	LOGS
	TRACES
)

var resource = map[Resource]string{
	METRICS: "metrics",
	LOGS:    "logs",
	TRACES:  "traces",
}

func (r Resource) String() string {
	return resource[r]
}
