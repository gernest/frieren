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

type M struct {
	Tracer trace.Tracer
	Meter  metric.Meter
}

type mKey struct{}

func Get(ctx context.Context) *M {
	if v := ctx.Value(mKey{}); v != nil {
		return v.(*M)
	}
	return nil
}

func (m *M) Start(ctx context.Context, name string) (context.Context, trace.Span) {
	return m.Tracer.Start(ctx, name)
}

func Start(ctx context.Context, name string) (context.Context, trace.Span) {
	return otel.Tracer(service).Start(ctx, name)
}

func Setup(ctx context.Context) context.Context {
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

	// use prometheus libraries and we want the stats to  be collected with the rest of otel metrics

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(
			metricsReader,
		),
		sdkmetric.WithResource(resource.Environment()),
	)

	otel.SetMeterProvider(provider)
	m := &M{
		Tracer: tp.Tracer(service),
		Meter:  provider.Meter(service),
	}
	return context.WithValue(ctx, mKey{}, m)
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
