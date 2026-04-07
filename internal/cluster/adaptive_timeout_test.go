package cluster

import (
	"testing"
	"time"
)

func TestAdaptiveTimeout_Defaults(t *testing.T) {
	at := NewAdaptiveTimeout()

	// No data yet — should return safe default
	timeout := at.Timeout("node-A")
	if timeout != 500*time.Millisecond {
		t.Errorf("expected 500ms default, got %v", timeout)
	}
}

func TestAdaptiveTimeout_RecordAndTimeout(t *testing.T) {
	at := NewAdaptiveTimeout(
		WithMinTimeout(10*time.Millisecond),
		WithMaxTimeout(2*time.Second),
		WithPercentile(0.95),
		WithMultiplier(2.0),
		WithWindowSize(10),
	)

	// Record consistent latencies ~100ms
	for i := 0; i < 10; i++ {
		at.Record("node-A", 100*time.Millisecond)
	}

	timeout := at.Timeout("node-A")
	// p95 of all 100ms = 100ms, * 2.0 = 200ms
	if timeout < 150*time.Millisecond || timeout > 250*time.Millisecond {
		t.Errorf("expected ~200ms timeout, got %v", timeout)
	}
}

func TestAdaptiveTimeout_SlowNode(t *testing.T) {
	at := NewAdaptiveTimeout(
		WithMinTimeout(10*time.Millisecond),
		WithMaxTimeout(5*time.Second),
		WithPercentile(0.95),
		WithMultiplier(2.0),
	)

	// Record slow latencies ~500ms
	for i := 0; i < 10; i++ {
		at.Record("slow-node", 500*time.Millisecond)
	}

	timeout := at.Timeout("slow-node")
	// p95 = 500ms, * 2.0 = 1000ms
	if timeout < 800*time.Millisecond || timeout > 1200*time.Millisecond {
		t.Errorf("expected ~1000ms timeout for slow node, got %v", timeout)
	}
}

func TestAdaptiveTimeout_FastNode(t *testing.T) {
	at := NewAdaptiveTimeout(
		WithMinTimeout(10*time.Millisecond),
		WithMaxTimeout(5*time.Second),
		WithPercentile(0.95),
		WithMultiplier(2.0),
	)

	// Record fast latencies ~5ms
	for i := 0; i < 10; i++ {
		at.Record("fast-node", 5*time.Millisecond)
	}

	timeout := at.Timeout("fast-node")
	// p95 = 5ms, * 2.0 = 10ms, but min is 10ms
	if timeout < 10*time.Millisecond || timeout > 20*time.Millisecond {
		t.Errorf("expected ~10ms timeout (clamped to min), got %v", timeout)
	}
}

func TestAdaptiveTimeout_MinClamp(t *testing.T) {
	at := NewAdaptiveTimeout(
		WithMinTimeout(50*time.Millisecond),
		WithMaxTimeout(5*time.Second),
		WithPercentile(0.95),
		WithMultiplier(1.0),
	)

	// Record very low latencies
	for i := 0; i < 10; i++ {
		at.Record("node", 1*time.Millisecond)
	}

	timeout := at.Timeout("node")
	// p95 = 1ms, * 1.0 = 1ms, but min is 50ms
	if timeout != 50*time.Millisecond {
		t.Errorf("expected min timeout 50ms, got %v", timeout)
	}
}

func TestAdaptiveTimeout_MaxClamp(t *testing.T) {
	at := NewAdaptiveTimeout(
		WithMinTimeout(50*time.Millisecond),
		WithMaxTimeout(1*time.Second),
		WithPercentile(0.95),
		WithMultiplier(2.0),
	)

	// Record very high latencies
	for i := 0; i < 10; i++ {
		at.Record("node", 2*time.Second)
	}

	timeout := at.Timeout("node")
	// p95 = 2000ms, * 2.0 = 4000ms, but max is 1000ms
	if timeout != 1*time.Second {
		t.Errorf("expected max timeout 1s, got %v", timeout)
	}
}

func TestAdaptiveTimeout_SlidingWindow(t *testing.T) {
	at := NewAdaptiveTimeout(
		WithMinTimeout(10*time.Millisecond),
		WithMaxTimeout(5*time.Second),
		WithPercentile(0.95),
		WithMultiplier(2.0),
		WithWindowSize(5),
	)

	// Record 5 old samples at 100ms
	for i := 0; i < 5; i++ {
		at.Record("node-A", 100*time.Millisecond)
	}

	// Record 5 new samples at 500ms — should push out old ones
	for i := 0; i < 5; i++ {
		at.Record("node-A", 500*time.Millisecond)
	}

	timeout := at.Timeout("node-A")
	// Now all 5 samples are 500ms, p95 = 500ms, * 2.0 = 1000ms
	if timeout < 800*time.Millisecond || timeout > 1200*time.Millisecond {
		t.Errorf("expected ~1000ms after sliding window, got %v", timeout)
	}
}

func TestAdaptiveTimeout_PerNodeIsolation(t *testing.T) {
	at := NewAdaptiveTimeout()

	at.Record("fast", 10*time.Millisecond)
	at.Record("fast", 10*time.Millisecond)
	at.Record("fast", 10*time.Millisecond)

	at.Record("slow", 500*time.Millisecond)
	at.Record("slow", 500*time.Millisecond)
	at.Record("slow", 500*time.Millisecond)

	fastTimeout := at.Timeout("fast")
	slowTimeout := at.Timeout("slow")

	if fastTimeout >= slowTimeout {
		t.Errorf("fast node timeout (%v) should be < slow node timeout (%v)", fastTimeout, slowTimeout)
	}
}

func TestAdaptiveTimeout_InsufficientData(t *testing.T) {
	at := NewAdaptiveTimeout()

	// Only 2 samples — below threshold of 3
	at.Record("node", 100*time.Millisecond)
	at.Record("node", 100*time.Millisecond)

	timeout := at.Timeout("node")
	if timeout != 500*time.Millisecond {
		t.Errorf("expected default timeout for insufficient data, got %v", timeout)
	}
}

func TestAdaptiveTimeout_TimeoutForReplicas(t *testing.T) {
	at := NewAdaptiveTimeout(
		WithMinTimeout(10*time.Millisecond),
		WithMaxTimeout(5*time.Second),
		WithPercentile(0.95),
		WithMultiplier(2.0),
	)

	// Setup: fast node ~10ms, slow node ~200ms
	for i := 0; i < 5; i++ {
		at.Record("fast", 10*time.Millisecond)
		at.Record("slow", 200*time.Millisecond)
	}

	nodes := []string{"fast", "slow"}
	timeout := at.TimeoutForReplicas(nodes)

	// Should return the max (slow node's timeout ~400ms)
	if timeout < 300*time.Millisecond {
		t.Errorf("expected max replica timeout >= 300ms, got %v", timeout)
	}
}

func TestAdaptiveTimeout_ConcurrentAccess(t *testing.T) {
	at := NewAdaptiveTimeout()

	done := make(chan bool)

	// Concurrent writes from multiple goroutines
	for i := 0; i < 10; i++ {
		go func(id int) {
			nodeID := "node"
			for j := 0; j < 100; j++ {
				at.Record(nodeID, time.Duration(id*10+j)*time.Millisecond)
			}
			done <- true
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				at.Timeout("node")
			}
			done <- true
		}()
	}

	// Wait for all
	for i := 0; i < 20; i++ {
		<-done
	}

	// Should not panic and return a valid timeout
	timeout := at.Timeout("node")
	if timeout == 0 {
		t.Error("timeout should not be zero after concurrent access")
	}
}

func TestPercentile(t *testing.T) {
	tests := []struct {
		name     string
		data     []float64
		p        float64
		expected float64
	}{
		{"p50 of 5 items", []float64{1, 2, 3, 4, 5}, 0.5, 3},
		{"p95 of 20 items", []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, 0.95, 19},
		{"p99 of 10 items", []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}, 0.99, 100},
		{"empty", []float64{}, 0.95, 0},
		{"single", []float64{42}, 0.95, 42},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := percentile(tt.data, tt.p)
			if got != tt.expected {
				t.Errorf("percentile(%v, %.2f) = %v, want %v", tt.data, tt.p, got, tt.expected)
			}
		})
	}
}

func BenchmarkAdaptiveTimeout_Record(b *testing.B) {
	at := NewAdaptiveTimeout(WithWindowSize(1000))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		at.Record("node-A", 100*time.Millisecond)
	}
}

func BenchmarkAdaptiveTimeout_Timeout(b *testing.B) {
	at := NewAdaptiveTimeout(WithWindowSize(1000))
	for i := 0; i < 1000; i++ {
		at.Record("node-A", 100*time.Millisecond)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		at.Timeout("node-A")
	}
}
