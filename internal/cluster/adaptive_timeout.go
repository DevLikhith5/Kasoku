package cluster

import (
	"math"
	"sort"
	"sync"
	"time"
)

// AdaptiveTimeout tracks per-node latencies and computes dynamic timeouts
// using a percentile-based approach with exponential decay.
type AdaptiveTimeout struct {
	mu sync.Mutex

	// per-node latency history (milliseconds)
	latencies map[string][]float64

	// global defaults
	minTimeout time.Duration
	maxTimeout time.Duration
	percentile float64 // e.g., 0.95 for p95
	multiplier float64 // safety multiplier, e.g., 2.0

	// ring buffer size per node
	windowSize int
}

// NewAdaptiveTimeout creates a tracker with sensible defaults.
func NewAdaptiveTimeout(opts ...TimeoutOption) *AdaptiveTimeout {
	at := &AdaptiveTimeout{
		latencies:  make(map[string][]float64),
		minTimeout: 50 * time.Millisecond,
		maxTimeout: 5 * time.Second,
		percentile: 0.95,
		multiplier: 2.0,
		windowSize: 100,
	}
	for _, opt := range opts {
		opt(at)
	}
	return at
}

// TimeoutOption functional option for AdaptiveTimeout
type TimeoutOption func(*AdaptiveTimeout)

// WithMinTimeout sets the minimum allowed timeout.
func WithMinTimeout(d time.Duration) TimeoutOption {
	return func(at *AdaptiveTimeout) { at.minTimeout = d }
}

// WithMaxTimeout sets the maximum allowed timeout.
func WithMaxTimeout(d time.Duration) TimeoutOption {
	return func(at *AdaptiveTimeout) { at.maxTimeout = d }
}

// WithPercentile sets the percentile to target (0.0-1.0).
func WithPercentile(p float64) TimeoutOption {
	return func(at *AdaptiveTimeout) { at.percentile = p }
}

// WithMultiplier sets the safety multiplier over the percentile.
func WithMultiplier(m float64) TimeoutOption {
	return func(at *AdaptiveTimeout) { at.multiplier = m }
}

// WithWindowSize sets the sliding window size per node.
func WithWindowSize(n int) TimeoutOption {
	return func(at *AdaptiveTimeout) { at.windowSize = n }
}

// Record records a latency measurement for a node.
func (at *AdaptiveTimeout) Record(nodeID string, latency time.Duration) {
	at.mu.Lock()
	defer at.mu.Unlock()

	samples := at.latencies[nodeID]
	if len(samples) >= at.windowSize {
		// Drop oldest (shift left)
		samples = append(samples[1:], float64(latency.Milliseconds()))
	} else {
		samples = append(samples, float64(latency.Milliseconds()))
	}
	at.latencies[nodeID] = samples
}

// Timeout returns the adaptive timeout duration for a node.
// If insufficient data, returns a conservative default.
func (at *AdaptiveTimeout) Timeout(nodeID string) time.Duration {
	at.mu.Lock()
	samples := at.latencies[nodeID]
	at.mu.Unlock()

	if len(samples) < 3 {
		// Not enough data — use a safe default
		return 500 * time.Millisecond
	}

	p := percentile(samples, at.percentile)
	timeout := max(
		// Clamp to bounds
		time.Duration(p*at.multiplier)*time.Millisecond, at.minTimeout)
	if timeout > at.maxTimeout {
		timeout = at.maxTimeout
	}
	return timeout
}

// TimeoutForReplicas returns the max adaptive timeout across all given nodes.
// Used when fanning out to multiple replicas — use the slowest expected node.
func (at *AdaptiveTimeout) TimeoutForReplicas(nodeIDs []string) time.Duration {
	maxTimeout := at.minTimeout
	for _, nid := range nodeIDs {
		t := at.Timeout(nid)
		if t > maxTimeout {
			maxTimeout = t
		}
	}
	return maxTimeout
}

// percentile computes the p-th percentile of a slice (0.0-1.0).
// Assumes input is in milliseconds.
func percentile(data []float64, p float64) float64 {
	if len(data) == 0 {
		return 0
	}
	sorted := make([]float64, len(data))
	copy(sorted, data)
	sort.Float64s(sorted)

	idx := int(math.Ceil(float64(len(sorted))*p)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
