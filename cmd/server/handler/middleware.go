package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

const slowRequestThreshold = 100 * time.Millisecond

func WithLogging(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			next.ServeHTTP(w, r)
			duration := time.Since(start)
			if duration > slowRequestThreshold {
				logger.Warn("slow request",
					"method", r.Method,
					"path", r.URL.Path,
					"duration_ms", duration.Milliseconds(),
					"remote_addr", r.RemoteAddr,
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
