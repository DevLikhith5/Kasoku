package tracing

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

var tracer trace.Tracer

type ExporterType string

const (
	ExporterNone   ExporterType = "none"
	ExporterStdout ExporterType = "stdout"
	ExporterOTLP   ExporterType = "otlp"
)

type TracingConfig struct {
	ServiceName    string
	ExporterType   ExporterType
	OTLPEndpoint   string
	OTLPInsecure   bool
	SamplingRatio  float64
}

func InitWithConfig(cfg TracingConfig) (func(context.Context) error, error) {
	var exporter sdktrace.SpanExporter
	var err error

	switch cfg.ExporterType {
	case ExporterOTLP:
		endpoint := cfg.OTLPEndpoint
		if endpoint == "" {
			endpoint = "localhost:4317"
		}
		exporter, err = otlptracegrpc.New(context.Background(),
			otlptracegrpc.WithEndpoint(endpoint),
			otlptracegrpc.WithInsecure(),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
		}
		fmt.Printf("Tracing: Using OTLP exporter to %s\n", endpoint)

	case ExporterStdout:
		exporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, fmt.Errorf("failed to create stdout exporter: %w", err)
		}
		fmt.Println("Tracing: Using stdout exporter")

	default:
		exporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, fmt.Errorf("failed to create stdout exporter: %w", err)
		}
	}

	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion("1.0.0"),
			attribute.String("environment", os.Getenv("KASOKU_ENV")),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	var sampler sdktrace.Sampler
	if cfg.SamplingRatio >= 1.0 {
		sampler = sdktrace.AlwaysSample()
	} else if cfg.SamplingRatio > 0 {
		sampler = sdktrace.TraceIDRatioBased(cfg.SamplingRatio)
	} else {
		sampler = sdktrace.NeverSample()
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	otel.SetTracerProvider(tp)
	tracer = tp.Tracer(cfg.ServiceName)

	return tp.Shutdown, nil
}

func Init(serviceName string) (func(context.Context) error, error) {
	exporterType := ExporterType(os.Getenv("KASOKU_TRACING_EXPORTER"))
	
	cfg := TracingConfig{
		ServiceName:    serviceName,
		ExporterType:   exporterType,
		OTLPEndpoint:   os.Getenv("KASOKU_OTLP_ENDPOINT"),
		OTLPInsecure:   os.Getenv("KASOKU_OTLP_INSECURE") == "true",
		SamplingRatio:  1.0,
	}

	if exporterType == "" {
		exporterType = ExporterStdout
		if os.Getenv("KASOKU_TRACING") != "true" {
			exporterType = ExporterNone
		}
	}

	if exporterType == ExporterNone {
		tracer = otel.Tracer(serviceName)
		return func(ctx context.Context) error { return nil }, nil
	}

	return InitWithConfig(cfg)
}

func Tracer() trace.Tracer {
	if tracer == nil {
		tracer = otel.Tracer("kasoku")
	}
	return tracer
}

func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return Tracer().Start(ctx, name, trace.WithAttributes(attrs...))
}

func StartSpanWithParent(parent trace.SpanContext, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	ctx := trace.ContextWithSpanContext(context.Background(), parent)
	return Tracer().Start(ctx, name, trace.WithAttributes(attrs...))
}

func RecordError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetAttributes(attribute.Bool("error", true))
}

func AddEvent(span trace.Span, name string, attrs ...attribute.KeyValue) {
	span.AddEvent(name, trace.WithAttributes(attrs...))
}

func SetSpanAttributes(span trace.Span, attrs ...attribute.KeyValue) {
	span.SetAttributes(attrs...)
}

func GetTraceID(span trace.Span) string {
	return span.SpanContext().TraceID().String()
}

func GetSpanID(span trace.Span) string {
	return span.SpanContext().SpanID().String()
}

func WithTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, timeout)
}