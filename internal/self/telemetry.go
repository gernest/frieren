package self

import (
	"context"

	"github.com/gernest/frieren/internal/util"
	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	service = "frieren"
)

func Meter() metric.Meter {
	return otel.Meter(service)
}

func Counter(name string, options ...metric.Int64CounterOption) metric.Int64Counter {
	v, err := Meter().Int64Counter(name, options...)
	if err != nil {
		util.Exit("creating counter metric", "name", name)
	}
	return v
}

func Gauge(name string, options ...metric.Int64GaugeOption) metric.Int64Gauge {
	v, err := Meter().Int64Gauge(name, options...)
	if err != nil {
		util.Exit("creating gauge metric", "name", name)
	}
	return v
}

func Histogram(name string, options ...metric.Int64HistogramOption) metric.Int64Histogram {
	v, err := Meter().Int64Histogram(name, options...)
	if err != nil {
		util.Exit("creating histogram metric", "name", name)
	}
	return v
}

func FloatHistogram(name string, options ...metric.Float64HistogramOption) metric.Float64Histogram {
	v, err := Meter().Float64Histogram(name, options...)
	if err != nil {
		util.Exit("creating float histogram metric", "name", name)
	}
	return v
}

func Tracer() trace.Tracer {
	return otel.Tracer(service)
}

func Start(ctx context.Context, name string) (context.Context, trace.Span) {
	return Tracer().Start(ctx, name)
}

func Setup() {
	spanExporter, err := newSpanExporter()
	if err != nil {
		util.Exit("creating span exporter", "err", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(resource.Environment()),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithBatcher(spanExporter),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	metricsReader, err := newMetricReader()
	if err != nil {
		util.Exit("creating metrics exporter", "err", err)
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(
			metricsReader,
		),
		sdkmetric.WithResource(resource.Environment()),
	)

	otel.SetMeterProvider(provider)
}

func newMetricReader() (sdkmetric.Reader, error) {
	return autoexport.NewMetricReader(context.Background(),
		autoexport.WithFallbackMetricReader(func(_ context.Context) (sdkmetric.Reader, error) {
			return sdkmetric.NewManualReader(), nil
		}),
	)
}

func newSpanExporter() (sdktrace.SpanExporter, error) {
	return autoexport.NewSpanExporter(context.Background(), autoexport.WithFallbackSpanExporter(
		func(_ context.Context) (sdktrace.SpanExporter, error) {
			return noopSpanExporter{}, nil
		},
	))
}

type noopSpanExporter struct{}

var _ sdktrace.SpanExporter = noopSpanExporter{}

func (e noopSpanExporter) ExportSpans(_ context.Context, _ []sdktrace.ReadOnlySpan) error {
	return nil
}

func (e noopSpanExporter) Shutdown(_ context.Context) error {
	return nil
}
