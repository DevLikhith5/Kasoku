package cluster

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFailureDetector_RecordHeartbeat(t *testing.T) {
	logger := slog.Default()
	fd := NewFailureDetector(8.0, time.Minute, 3, logger)

	fd.RecordHeartbeat("node-1")
	fd.RecordHeartbeat("node-1")
	fd.RecordHeartbeat("node-1")

	lastHB, exists := fd.GetLastHeartbeat("node-1")
	if !exists {
		t.Fatal("expected heartbeat to exist")
	}
	if lastHB.IsZero() {
		t.Error("expected non-zero last heartbeat")
	}
}

func TestFailureDetector_IsAvailable(t *testing.T) {
	logger := slog.Default()
	fd := NewFailureDetector(8.0, time.Minute, 2, logger)

	// Record heartbeats with 100ms intervals
	for i := 0; i < 10; i++ {
		fd.RecordHeartbeat("node-1")
		time.Sleep(100 * time.Millisecond)
	}

	// Should be available immediately after heartbeat
	if !fd.IsAvailable("node-1") {
		t.Error("expected node to be available")
	}

	// Unknown node should be unavailable
	if fd.IsAvailable("node-unknown") {
		t.Error("expected unknown node to be unavailable")
	}
}

func TestFailureDetector_PhiCalculation(t *testing.T) {
	logger := slog.Default()
	fd := NewFailureDetector(8.0, time.Minute, 3, logger)

	// Record regular heartbeats
	for i := 0; i < 10; i++ {
		fd.RecordHeartbeat("node-1")
		time.Sleep(50 * time.Millisecond)
	}

	phi := fd.phi("node-1")
	if phi < 0 {
		t.Errorf("expected non-negative phi, got %f", phi)
	}
}

func TestFailureDetector_RemoveNode(t *testing.T) {
	logger := slog.Default()
	fd := NewFailureDetector(8.0, time.Minute, 2, logger)

	fd.RecordHeartbeat("node-1")
	fd.RemoveNode("node-1")

	_, exists := fd.GetLastHeartbeat("node-1")
	if exists {
		t.Error("expected node to be removed")
	}
}

func TestFailureDetector_ConcurrentAccess(t *testing.T) {
	logger := slog.Default()
	fd := NewFailureDetector(8.0, time.Minute, 2, logger)

	var wg sync.WaitGroup

	// Concurrent heartbeats
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				fd.RecordHeartbeat("node-1")
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fd.IsAvailable("node-1")
			fd.GetLastHeartbeat("node-1")
		}()
	}

	wg.Wait()
}

func TestReadRepair_CheckAndRepair(t *testing.T) {
	logger := slog.Default()
	rr := NewReadRepair(logger)

	// Simulate inconsistent replicas
	values := map[string][]byte{
		"node-1": []byte("value-v2"),
		"node-2": []byte("value-v1"), // Stale
		"node-3": []byte("value-v2"),
	}

	repairs := make(map[string][]byte)
	writeFunc := func(ctx context.Context, nodeID string, key string, value []byte) error {
		repairs[nodeID] = value
		return nil
	}

	count := rr.CheckAndRepair(context.Background(), "test-key", values, writeFunc)

	// node-2 should be repaired
	if count != 1 {
		t.Errorf("expected 1 repair, got %d", count)
	}

	if string(repairs["node-2"]) != "value-v2" {
		t.Errorf("expected node-2 to be repaired to value-v2, got %s", string(repairs["node-2"]))
	}
}

func TestReadRepair_NoRepairNeeded(t *testing.T) {
	logger := slog.Default()
	rr := NewReadRepair(logger)

	// All replicas consistent
	values := map[string][]byte{
		"node-1": []byte("value"),
		"node-2": []byte("value"),
		"node-3": []byte("value"),
	}

	writeFunc := func(ctx context.Context, nodeID string, key string, value []byte) error {
		t.Error("write should not be called")
		return nil
	}

	count := rr.CheckAndRepair(context.Background(), "test-key", values, writeFunc)
	if count != 0 {
		t.Errorf("expected 0 repairs, got %d", count)
	}
}

func TestReadRepair_EmptyValues(t *testing.T) {
	logger := slog.Default()
	rr := NewReadRepair(logger)

	values := map[string][]byte{}

	writeFunc := func(ctx context.Context, nodeID string, key string, value []byte) error {
		return nil
	}

	count := rr.CheckAndRepair(context.Background(), "test-key", values, writeFunc)
	if count != 0 {
		t.Errorf("expected 0 repairs for empty values, got %d", count)
	}
}

func TestReadRepair_RepairCount(t *testing.T) {
	logger := slog.Default()
	rr := NewReadRepair(logger)

	// First repair
	values1 := map[string][]byte{
		"node-1": []byte("v2"),
		"node-2": []byte("v1"),
	}
	rr.CheckAndRepair(context.Background(), "key1", values1, func(ctx context.Context, nodeID string, key string, value []byte) error {
		return nil
	})

	// Second repair
	values2 := map[string][]byte{
		"node-3": []byte("v2"),
		"node-4": []byte("v1"),
	}
	rr.CheckAndRepair(context.Background(), "key2", values2, func(ctx context.Context, nodeID string, key string, value []byte) error {
		return nil
	})

	if rr.GetRepairCount() != 2 {
		t.Errorf("expected 2 total repairs, got %d", rr.GetRepairCount())
	}
}

func TestQuorumChecker_CheckWriteQuorum(t *testing.T) {
	qc := NewQuorumChecker(3, 2)

	if !qc.CheckWriteQuorum(2) {
		t.Error("expected quorum with 2 acks")
	}
	if !qc.CheckWriteQuorum(3) {
		t.Error("expected quorum with 3 acks")
	}
	if qc.CheckWriteQuorum(1) {
		t.Error("expected no quorum with 1 ack")
	}
}

func TestQuorumChecker_CheckReadQuorum(t *testing.T) {
	qc := NewQuorumChecker(3, 2)

	if !qc.CheckReadQuorum(2) {
		t.Error("expected read quorum with 2 responses")
	}
	if qc.CheckReadQuorum(1) {
		t.Error("expected no read quorum with 1 response")
	}
}

func TestQuorumChecker_IsQuorumPossible(t *testing.T) {
	qc := NewQuorumChecker(3, 2)

	if !qc.IsQuorumPossible(2) {
		t.Error("expected quorum possible with 2 nodes")
	}
	if !qc.IsQuorumPossible(3) {
		t.Error("expected quorum possible with 3 nodes")
	}
	if qc.IsQuorumPossible(1) {
		t.Error("expected quorum not possible with 1 node")
	}
}

func TestQuorumChecker_GetRequiredQuorum(t *testing.T) {
	qc := NewQuorumChecker(3, 2)

	if qc.GetRequiredQuorum() != 2 {
		t.Errorf("expected required quorum 2, got %d", qc.GetRequiredQuorum())
	}
}

func TestAntiEntropy_StartStop(t *testing.T) {
	logger := slog.Default()
	var syncCount atomic.Int32
	syncFunc := func(ctx context.Context, peerID string) error {
		syncCount.Add(1)
		return nil
	}

	ae := NewAntiEntropy("node-1", 100*time.Millisecond, syncFunc, logger)
	ae.Start([]string{"node-2", "node-3"})

	// Let it run for a bit
	time.Sleep(250 * time.Millisecond)
	ae.Stop()

	if syncCount.Load() == 0 {
		t.Error("expected at least one sync to occur")
	}
}

func TestAntiEntropy_SkipsSelf(t *testing.T) {
	logger := slog.Default()
	var syncCount atomic.Int32
	syncFunc := func(ctx context.Context, peerID string) error {
		syncCount.Add(1)
		return nil
	}

	ae := NewAntiEntropy("node-1", 100*time.Millisecond, syncFunc, logger)
	ae.Start([]string{"node-1", "node-2"}) // node-1 is self

	time.Sleep(150 * time.Millisecond)
	ae.Stop()

	// Should only sync with node-2, not node-1
	if syncCount.Load() != 1 {
		t.Errorf("expected 1 sync (skipping self), got %d", syncCount.Load())
	}
}

func TestAntiEntropy_HandlesSyncError(t *testing.T) {
	logger := slog.Default()
	syncFunc := func(ctx context.Context, peerID string) error {
		return context.DeadlineExceeded
	}

	ae := NewAntiEntropy("node-1", 100*time.Millisecond, syncFunc, logger)
	ae.Start([]string{"node-2"})

	time.Sleep(150 * time.Millisecond)
	ae.Stop()
	// Should not panic on error
}

func TestAntiEntropy_NilSyncFunc(t *testing.T) {
	logger := slog.Default()
	ae := NewAntiEntropy("node-1", time.Second, nil, logger)
	ae.Start([]string{"node-2"})
	ae.Stop()
	// Should not panic
}

func TestFailureDetector_InsufficientSamples(t *testing.T) {
	logger := slog.Default()
	fd := NewFailureDetector(8.0, time.Minute, 5, logger)

	// Record fewer than minSamples
	fd.RecordHeartbeat("node-1")
	fd.RecordHeartbeat("node-1")

	phi := fd.phi("node-1")
	if phi != 0.0 {
		t.Errorf("expected phi=0.0 with insufficient samples, got %f", phi)
	}
}

func TestFailureDetector_NoHeartbeats(t *testing.T) {
	logger := slog.Default()
	fd := NewFailureDetector(8.0, time.Minute, 2, logger)

	phi := fd.phi("node-unknown")
	if phi <= 8.0 {
		t.Errorf("expected phi > threshold for unknown node, got %f", phi)
	}
}

func TestFailureDetector_ZeroMean(t *testing.T) {
	logger := slog.Default()
	fd := NewFailureDetector(8.0, time.Minute, 2, logger)

	// Record heartbeats at exact same time
	fd.RecordHeartbeat("node-1")
	fd.RecordHeartbeat("node-1")

	// This edge case should not panic
	phi := fd.phi("node-1")
	if phi < 0 {
		t.Errorf("expected non-negative phi, got %f", phi)
	}
}
