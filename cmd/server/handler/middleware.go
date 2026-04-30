package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
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
			logger.Info("http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rr.statusCode,
				"duration_ms", time.Since(start).Milliseconds(),
			)
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
