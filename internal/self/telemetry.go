package self

import (
	"context"

	"github.com/gernest/frieren/internal/util"
	prometheusbridge "go.opentelemetry.io/contrib/bridges/prometheus"
	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
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

func Setup(ctx context.Context) func() {
	spanExporter, err := newSpanExporter(ctx)
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
	metricsReader, err := newMetricReader(ctx)
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

	return func() {
		spanExporter.Shutdown(ctx)
		metricsReader.Shutdown(ctx)
	}
}

func newMetricReader(ctx context.Context) (sdkmetric.Reader, error) {
	return autoexport.NewMetricReader(ctx,
		autoexport.WithFallbackMetricReader(func(ctx context.Context) (sdkmetric.Reader, error) {
			// By default we send to the local gRPC endpoint. We rely on grafana packages
			// which rely on prometheus, so we create producers for both prometheus and
			// otlp and mesh them togather.
			r, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithInsecure())
			if err != nil {
				return nil, err
			}
			return sdkmetric.NewPeriodicReader(r, sdkmetric.WithProducer(
				prometheusbridge.NewMetricProducer(),
			)), nil
		}),
	)
}

func newSpanExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	return autoexport.NewSpanExporter(ctx, autoexport.WithFallbackSpanExporter(
		func(ctx context.Context) (sdktrace.SpanExporter, error) {
			return otlptracegrpc.New(ctx, otlptracegrpc.WithInsecure())
		},
	))
}
