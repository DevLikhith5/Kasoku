package metrics

import (
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
)

type Metrics struct{}

func New() *Metrics {
	return &Metrics{}
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
