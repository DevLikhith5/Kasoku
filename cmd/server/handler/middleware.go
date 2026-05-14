package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/DevLikhith5/kasoku/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type responseRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (rr *responseRecorder) WriteHeader(code int) {
	rr.statusCode = code
	rr.ResponseWriter.WriteHeader(code)
}

func WithLogging(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rr := &responseRecorder{ResponseWriter: w, statusCode: http.StatusOK}
			next.ServeHTTP(rr, r)
			duration := time.Since(start)
			// Only log slow requests or errors to avoid benchmark overhead
			if duration > 100*time.Millisecond || rr.statusCode >= 400 {
				logger.Info("http request",
					"method", r.Method,
					"path", r.URL.Path,
					"status", rr.statusCode,
					"duration_ms", duration.Milliseconds(),
				)
			}
		})
	}
}

func WithRecovery(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := recover(); err != nil {
					logger.Error("panic recovered", "error", err)
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)
					json.NewEncoder(w).Encode(map[string]interface{}{
						"success": false,
						"error":   "internal error",
					})
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

var propagator = propagation.NewCompositeTextMapPropagator(
	propagation.TraceContext{},
	propagation.Baggage{},
)

func WithTracing() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Extract trace context from incoming request headers
			ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))

			spanName := r.Method + " " + r.URL.Path
			ctx, span := tracing.StartSpan(ctx, spanName,
				attribute.String("http.method", r.Method),
				attribute.String("http.path", r.URL.Path),
				attribute.String("http.target", r.URL.String()),
			)
			defer span.End()

			// Inject trace context into response headers for client-side tracing
			propagator.Inject(ctx, propagation.HeaderCarrier(w.Header()))

			// Wrap response writer to capture status code
			rr := &responseRecorder{ResponseWriter: w, statusCode: http.StatusOK}
			next.ServeHTTP(rr, r.WithContext(ctx))

			span.SetAttributes(
				attribute.Int("http.status_code", rr.statusCode),
				attribute.Bool("http.success", rr.statusCode < 400),
			)
			if rr.statusCode >= 500 {
				span.SetAttributes(attribute.Bool("error", true))
			}
		})
	}
}

// HTTPClientWithTracing returns an HTTP client that propagates trace context
func HTTPClientWithTracing(client *http.Client) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	originalTransport := client.Transport
	if originalTransport == nil {
		originalTransport = http.DefaultTransport
	}
	client.Transport = &tracingRoundTripper{base: originalTransport}
	return client
}

type tracingRoundTripper struct {
	base http.RoundTripper
}

func (t *tracingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		propagator.Inject(ctx, propagation.HeaderCarrier(req.Header))
	}
	return t.base.RoundTrip(req)
}
