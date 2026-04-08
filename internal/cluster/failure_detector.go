package cluster

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type FailureDetector struct {
	mu               sync.Mutex
	heartbeats       map[string]*heartbeatHistory
	threshold        float64
	sampleWindowSize time.Duration
	minSamples       int
	logger           *slog.Logger
}

type heartbeatHistory struct {
	timestamps []time.Time
	lastUpdate time.Time
}

func NewFailureDetector(threshold float64, windowSize time.Duration, minSamples int, logger *slog.Logger) *FailureDetector {
	if logger == nil {
		logger = slog.Default()
	}

	return &FailureDetector{
		heartbeats:       make(map[string]*heartbeatHistory),
		threshold:        threshold,
		sampleWindowSize: windowSize,
		minSamples:       minSamples,
		logger:           logger,
	}
}

func (fd *FailureDetector) RecordHeartbeat(nodeID string) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	now := time.Now()
	hist, exists := fd.heartbeats[nodeID]
	if !exists {
		hist = &heartbeatHistory{
			timestamps: make([]time.Time, 0, 100),
		}
		fd.heartbeats[nodeID] = hist
	}

	hist.timestamps = append(hist.timestamps, now)
	hist.lastUpdate = now

	// Keep only recent heartbeats
	cutoff := now.Add(-fd.sampleWindowSize)
	idx := 0
	for idx < len(hist.timestamps) && hist.timestamps[idx].Before(cutoff) {
		idx++
	}
	if idx > 0 {
		hist.timestamps = hist.timestamps[idx:]
	}
}

func (fd *FailureDetector) IsAvailable(nodeID string) bool {
	phi := fd.phi(nodeID)
	return phi < fd.threshold
}

func (fd *FailureDetector) phi(nodeID string) float64 {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	hist, exists := fd.heartbeats[nodeID]
	if !exists {
		return fd.threshold + 1.0 // Consider unavailable if no history
	}

	if len(hist.timestamps) < fd.minSamples {
		return 0.0 // Not enough samples
	}

	now := time.Now()
	timeSinceLast := now.Sub(hist.lastUpdate)

	// Calculate mean and variance of inter-arrival times
	if len(hist.timestamps) < 2 {
		return 0.0
	}

	var sum time.Duration
	for i := 1; i < len(hist.timestamps); i++ {
		sum += hist.timestamps[i].Sub(hist.timestamps[i-1])
	}
	mean := sum / time.Duration(len(hist.timestamps)-1)

	if mean <= 0 {
		return 0.0
	}

	// Phi = -log10(P_later(t_last_heartbeat))
	// Approximation using exponential distribution
	phi := float64(timeSinceLast) / float64(mean)

	return phi
}

func (fd *FailureDetector) GetLastHeartbeat(nodeID string) (time.Time, bool) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	hist, exists := fd.heartbeats[nodeID]
	if !exists {
		return time.Time{}, false
	}

	return hist.lastUpdate, true
}

func (fd *FailureDetector) RemoveNode(nodeID string) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	delete(fd.heartbeats, nodeID)
}

type ReadRepair struct {
	mu          sync.Mutex
	repairCount int
	logger      *slog.Logger
}

func NewReadRepair(logger *slog.Logger) *ReadRepair {
	if logger == nil {
		logger = slog.Default()
	}

	return &ReadRepair{
		logger: logger,
	}
}

func (rr *ReadRepair) CheckAndRepair(ctx context.Context, key string, values map[string][]byte, writeFunc func(ctx context.Context, nodeID string, key string, value []byte) error) int {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if len(values) == 0 {
		return 0
	}

	// Find the most common value (majority vote)
	valueCounts := make(map[string]int)
	valueToNodes := make(map[string][]string)

	for nodeID, value := range values {
		valStr := string(value)
		valueCounts[valStr]++
		valueToNodes[valStr] = append(valueToNodes[valStr], nodeID)
	}

	// Find the majority value
	var latestValue string
	var maxCount int
	for val, count := range valueCounts {
		if count > maxCount {
			maxCount = count
			latestValue = val
		}
	}

	if latestValue == "" {
		return 0
	}

	// Count inconsistencies and repair
	repairCount := 0
	for _, nodeID := range valueToNodes[latestValue] {
		_ = nodeID // These nodes have the correct value
	}

	for nodeID, value := range values {
		if string(value) != latestValue {
			// This replica is stale, repair it
			if err := writeFunc(ctx, nodeID, key, []byte(latestValue)); err != nil {
				rr.logger.Error("read repair failed", "node_id", nodeID, "key", key, "error", err)
				continue
			}
			repairCount++
			rr.logger.Debug("read repair completed", "node_id", nodeID, "key", key)
		}
	}

	rr.repairCount += repairCount
	return repairCount
}

func (rr *ReadRepair) GetRepairCount() int {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	return rr.repairCount
}

type QuorumChecker struct {
	replicationFactor int
	quorumSize        int
}

func NewQuorumChecker(replicationFactor, quorumSize int) *QuorumChecker {
	return &QuorumChecker{
		replicationFactor: replicationFactor,
		quorumSize:        quorumSize,
	}
}

func (qc *QuorumChecker) CheckWriteQuorum(successCount int) bool {
	return successCount >= qc.quorumSize
}

func (qc *QuorumChecker) CheckReadQuorum(successCount int) bool {
	// For strong consistency, read quorum should also meet the requirement
	return successCount >= qc.quorumSize
}

func (qc *QuorumChecker) IsQuorumPossible(availableNodes int) bool {
	return availableNodes >= qc.quorumSize
}

func (qc *QuorumChecker) GetRequiredQuorum() int {
	return qc.quorumSize
}

type AntiEntropy struct {
	stopOnce sync.Once // Bug 5 fix: prevents double-close panic on Stop()
	nodeID   string
	interval time.Duration
	stopCh   chan struct{}
	syncFunc func(ctx context.Context, peerID string) error
	logger   *slog.Logger
}

func NewAntiEntropy(nodeID string, interval time.Duration, syncFunc func(ctx context.Context, peerID string) error, logger *slog.Logger) *AntiEntropy {
	if logger == nil {
		logger = slog.Default()
	}

	return &AntiEntropy{
		nodeID:   nodeID,
		interval: interval,
		stopCh:   make(chan struct{}),
		syncFunc: syncFunc,
		logger:   logger,
	}
}

func (ae *AntiEntropy) Start(peers []string) {
	if ae.syncFunc == nil {
		ae.logger.Warn("no sync function provided, anti-entropy disabled")
		return
	}

	go func() {
		ticker := time.NewTicker(ae.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ae.run(peers)
			case <-ae.stopCh:
				ae.logger.Info("anti-entropy stopped")
				return
			}
		}
	}()

	ae.logger.Info("anti-entropy started", "interval", ae.interval)
}

// Stop stops the anti-entropy process.
// Bug 5 fix: uses sync.Once to prevent double-close panic.
func (ae *AntiEntropy) Stop() {
	ae.stopOnce.Do(func() {
		close(ae.stopCh)
	})
}

// run performs synchronization with peers.
// Bug 18 fix: mutex removed — it was held across all I/O operations (1+ seconds
// per peer) but doesn't actually protect any shared mutable state here.
func (ae *AntiEntropy) run(peers []string) {
	ctx, cancel := context.WithTimeout(context.Background(), ae.interval/2)
	defer cancel()

	for _, peerID := range peers {
		if peerID == ae.nodeID {
			continue
		}

		if err := ae.syncFunc(ctx, peerID); err != nil {
			ae.logger.Debug("anti-entropy sync failed", "peer", peerID, "error", err)
		}
	}
}
