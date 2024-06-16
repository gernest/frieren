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

	TracesResource
	TracesScope
	TracesSpan
	TracesTracesID
	TracesSpanID
	TracesState
	TracesParent
	TracesName
	TracesKind
	TracesStart
	TracesEnd
	TracesDuration
	TracesEvents
	TracesLinks
	TracesStatusCode
	TracesStatusMessage
	TracesRow
	TracesFamily
	TracesFST
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
