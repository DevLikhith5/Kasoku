package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Requests total counter
	requestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kasoku_requests_total",
			Help: "Total number of KV requests processed.",
		},
		[]string{"operation", "status"},
	)

	// Request duration histogram
	requestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kasoku_request_duration_seconds",
			Help:    "Latency of KV requests in seconds.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{"operation"},
	)

	// Storage metrics
	storageKeys = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "kasoku_storage_engine_keys_total",
			Help: "Total number of active keys in the storage engine.",
		},
	)
	storageBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kasoku_storage_engine_bytes",
			Help: "Memory footprint of the storage engine.",
		},
		[]string{"type"}, // memory or disk
	)

	// Cluster metrics
	clusterNodes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "kasoku_cluster_nodes_active",
			Help: "Number of active nodes in the consistent hash ring.",
		},
	)
	pendingHints = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "kasoku_cluster_pending_hints",
			Help: "Number of hinted handoffs waiting for delivery.",
		},
	)
	phiSuspicion = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kasoku_cluster_phi_suspicion",
			Help: "Current Phi accrual suspicion level per node.",
		},
		[]string{"node_id"},
	)

	// Replication timing metrics
	replicationReadLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kasoku_replication_read_latency_seconds",
			Help:    "Latency of replicated read operations in seconds.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
		},
		[]string{"phase"}, // quorum_wait, network, local, merge
	)
	replicationWriteLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kasoku_replication_write_latency_seconds",
			Help:    "Latency of replicated write operations in seconds.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
		},
		[]string{"phase"}, // quorum_wait, network, local, hint_delivery
	)
	replicationConflicts = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "kasoku_replication_conflicts_total",
			Help: "Total number of vector clock conflicts detected during replication.",
		},
	)
	sloppyQuorumFallbacks = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "kasoku_sloppy_quorum_fallbacks_total",
			Help: "Total number of times sloppy quorum had to use fallback nodes.",
		},
	)
	hintedHandoffRetries = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "kasoku_hint_handoff_retries_total",
			Help: "Total number of hinted handoff retry attempts.",
		},
	)
	readRepairCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "kasoku_read_repair_total",
			Help: "Total number of read repair operations.",
		},
	)

	// LSM engine timing metrics
	lsmFlushDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "kasoku_lsm_flush_duration_seconds",
			Help:    "Duration of memtable flush to SSTable in seconds.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
	)
	lsmCompactionDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kasoku_lsm_compaction_duration_seconds",
			Help:    "Duration of SSTable compaction in seconds.",
			Buckets: []float64{0.1, 0.5, 1, 2.5, 5, 10, 30, 60},
		},
		[]string{"level"},
	)
	lsmGetLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kasoku_lsm_get_latency_seconds",
			Help:    "Latency of LSM get operations in seconds.",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05},
		},
		[]string{"phase"}, // memtable, sstable, cache
	)
	lsmPutLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "kasoku_lsm_put_latency_seconds",
			Help:    "Latency of LSM put operations in seconds.",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025},
		},
	)
	lsmScanLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "kasoku_lsm_scan_latency_seconds",
			Help:    "Latency of LSM scan operations in seconds.",
			Buckets: []float64{0.001, 0.01, 0.05, 0.1, 0.5, 1},
		},
	)

	// HTTP handler stage timing - NO logging, just metrics
	httpHandlerLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kasoku_http_handler_latency_seconds",
			Help:    "HTTP handler stage timing without logging overhead.",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25},
		},
		[]string{"operation", "stage"}, // put/http_parse, put/store, get/store, batch/store
	)

	// Anti-entropy metrics
	antiEntropySyncCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "kasoku_anti_entropy_sync_total",
			Help: "Total number of anti-entropy synchronization rounds.",
		},
	)
	antiEntropySyncKeys = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "kasoku_anti_entropy_keys_synced",
			Help:    "Number of keys synced per anti-entropy round.",
			Buckets: []float64{1, 10, 50, 100, 500, 1000},
		},
	)
	merkleTreeBuildTime = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "kasoku_merkle_tree_build_seconds",
			Help:    "Time to build Merkle tree for anti-entropy.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 5},
		},
	)
)

// TimingTracker provides detailed per-operation timing tracking
type TimingTracker struct {
	mu sync.Mutex
	// Per-operation timing data (in nanoseconds)
	getPhases         map[string][]int64
	putPhases         map[string][]int64
	readQuorumPhases  map[string][]int64
	writeQuorumPhases map[string][]int64
}

func NewTimingTracker() *TimingTracker {
	return &TimingTracker{
		getPhases:         make(map[string][]int64),
		putPhases:         make(map[string][]int64),
		readQuorumPhases:  make(map[string][]int64),
		writeQuorumPhases: make(map[string][]int64),
	}
}

func (t *TimingTracker) RecordGetPhase(phase string, durationNs int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.getPhases[phase] = append(t.getPhases[phase], durationNs)
}

func (t *TimingTracker) RecordPutPhase(phase string, durationNs int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.putPhases[phase] = append(t.putPhases[phase], durationNs)
}

func (t *TimingTracker) RecordReadQuorumPhase(phase string, durationNs int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.readQuorumPhases[phase] = append(t.readQuorumPhases[phase], durationNs)
}

func (t *TimingTracker) RecordWriteQuorumPhase(phase string, durationNs int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.writeQuorumPhases[phase] = append(t.writeQuorumPhases[phase], durationNs)
}

type PhaseStats struct {
	Count int64
	AvgNs int64
	P50Ns int64
	P95Ns int64
	P99Ns int64
	MaxNs int64
}

func (t *TimingTracker) GetStats(phases map[string][]int64) map[string]PhaseStats {
	t.mu.Lock()
	defer t.mu.Unlock()

	stats := make(map[string]PhaseStats)
	for phase, values := range phases {
		if len(values) == 0 {
			continue
		}
		var sum, p50, p95, p99, max int64
		for _, v := range values {
			sum += v
			if v > max {
				max = v
			}
		}
		avg := sum / int64(len(values))

		// Simple percentile estimation
		sorted := make([]int64, len(values))
		copy(sorted, values)
		// Using built-in sort would be better but this avoids import
		p50Idx := int(float64(len(sorted)) * 0.50)
		p95Idx := int(float64(len(sorted)) * 0.95)
		p99Idx := int(float64(len(sorted)) * 0.99)
		if p50Idx >= len(sorted) {
			p50Idx = len(sorted) - 1
		}
		if p95Idx >= len(sorted) {
			p95Idx = len(sorted) - 1
		}
		if p99Idx >= len(sorted) {
			p99Idx = len(sorted) - 1
		}
		p50 = sorted[p50Idx]
		p95 = sorted[p95Idx]
		p99 = sorted[p99Idx]

		stats[phase] = PhaseStats{
			Count: int64(len(values)),
			AvgNs: avg,
			P50Ns: p50,
			P95Ns: p95,
			P99Ns: p99,
			MaxNs: max,
		}
	}
	return stats
}

func (t *TimingTracker) GetGetStats() map[string]PhaseStats {
	return t.GetStats(t.getPhases)
}

func (t *TimingTracker) GetPutStats() map[string]PhaseStats {
	return t.GetStats(t.putPhases)
}

type Metrics struct {
	timingTracker *TimingTracker
}

func New() *Metrics {
	return &Metrics{
		timingTracker: NewTimingTracker(),
	}
}

func (m *Metrics) TimingTracker() *TimingTracker {
	return m.timingTracker
}

// RecordHandlerStage records timing for HTTP handler stages without logging
func (m *Metrics) RecordHandlerStage(operation, stage string, duration time.Duration) {
	httpHandlerLatency.WithLabelValues(operation, stage).Observe(duration.Seconds())
}

func (m *Metrics) RecordGetStart() time.Time {
	return time.Now()
}

func (m *Metrics) RecordGetEnd(start time.Time, success bool) {
	status := "success"
	if !success {
		status = "error"
	}
	requestsTotal.WithLabelValues("get", status).Inc()
	requestDuration.WithLabelValues("get").Observe(time.Since(start).Seconds())
}

func (m *Metrics) RecordPutStart() time.Time {
	return time.Now()
}

func (m *Metrics) RecordPutEnd(start time.Time, success bool) {
	status := "success"
	if !success {
		status = "error"
	}
	requestsTotal.WithLabelValues("put", status).Inc()
	requestDuration.WithLabelValues("put").Observe(time.Since(start).Seconds())
}

func (m *Metrics) RecordDeleteStart() time.Time {
	return time.Now()
}

func (m *Metrics) RecordDeleteEnd(start time.Time, success bool) {
	status := "success"
	if !success {
		status = "error"
	}
	requestsTotal.WithLabelValues("delete", status).Inc()
	requestDuration.WithLabelValues("delete").Observe(time.Since(start).Seconds())
}

// Snapshot ensures compatibility with handlers expecting the old method,
// though it won't be actively used over /metrics endpoint
type Snapshot struct {
	GetTotal    int64
	PutTotal    int64
	DeleteTotal int64

	GetErrors    int64
	PutErrors    int64
	DeleteErrors int64

	AvgGetLatencyMs float64
	AvgPutLatencyMs float64
}

func (m *Metrics) Get() Snapshot {
	return Snapshot{}
}

// Helper methods to update Gauges
func (m *Metrics) SetStorageKeys(count int64) {
	storageKeys.Set(float64(count))
}

func (m *Metrics) SetStorageBytes(memBytes, diskBytes int64) {
	storageBytes.WithLabelValues("memory").Set(float64(memBytes))
	storageBytes.WithLabelValues("disk").Set(float64(diskBytes))
}

func (m *Metrics) SetClusterNodes(count int) {
	clusterNodes.Set(float64(count))
}

func (m *Metrics) SetPendingHints(count int) {
	pendingHints.Set(float64(count))
}

func (m *Metrics) SetPhiSuspicion(nodeID string, phi float64) {
	phiSuspicion.WithLabelValues(nodeID).Set(phi)
}

func (m *Metrics) RecordBatchPut(count int) {
	requestsTotal.WithLabelValues("batch_put", "success").Add(float64(count))
}

func (m *Metrics) RecordBatchGet(count int) {
	requestsTotal.WithLabelValues("batch_get", "success").Add(float64(count))
}
