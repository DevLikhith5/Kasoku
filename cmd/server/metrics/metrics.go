package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

// Metrics tracks server operation statistics
type Metrics struct {
	getTotal    atomic.Int64
	putTotal    atomic.Int64
	deleteTotal atomic.Int64

	getErrors    atomic.Int64
	putErrors    atomic.Int64
	deleteErrors atomic.Int64

	getLatency    sync.Mutex
	getLatencySum int64
	getCount      int64

	putLatency    sync.Mutex
	putLatencySum int64
	putCount      int64
}

// New creates a new Metrics instance
func New() *Metrics {
	return &Metrics{}
}

// RecordGet records a GET operation (legacy, use RecordGetStart/RecordGetEnd)
func (m *Metrics) RecordGet(duration time.Duration, success bool) {
	m.getTotal.Add(1)
	if !success {
		m.getErrors.Add(1)
		return
	}

	m.getLatency.Lock()
	m.getLatencySum += int64(duration)
	m.getCount++
	m.getLatency.Unlock()
}

// RecordGetStart returns the start time for a GET operation
func (m *Metrics) RecordGetStart() time.Time {
	return time.Now()
}

// RecordGetEnd records the end of a GET operation
func (m *Metrics) RecordGetEnd(start time.Time, success bool) {
	m.RecordGet(time.Since(start), success)
}

// RecordPut records a PUT operation (legacy, use RecordPutStart/RecordPutEnd)
func (m *Metrics) RecordPut(duration time.Duration, success bool) {
	m.putTotal.Add(1)
	if !success {
		m.putErrors.Add(1)
		return
	}

	m.putLatency.Lock()
	m.putLatencySum += int64(duration)
	m.putCount++
	m.putLatency.Unlock()
}

// RecordPutStart returns the start time for a PUT operation
func (m *Metrics) RecordPutStart() time.Time {
	return time.Now()
}

// RecordPutEnd records the end of a PUT operation
func (m *Metrics) RecordPutEnd(start time.Time, success bool) {
	m.RecordPut(time.Since(start), success)
}

// RecordDelete records a DELETE operation (legacy, use RecordDeleteStart/RecordDeleteEnd)
func (m *Metrics) RecordDelete(duration time.Duration, success bool) {
	m.deleteTotal.Add(1)
	if !success {
		m.deleteErrors.Add(1)
	}
}

// RecordDeleteStart returns the start time for a DELETE operation
func (m *Metrics) RecordDeleteStart() time.Time {
	return time.Now()
}

// RecordDeleteEnd records the end of a DELETE operation
func (m *Metrics) RecordDeleteEnd(start time.Time, success bool) {
	m.RecordDelete(time.Since(start), success)
}

// Snapshot holds a point-in-time view of metrics
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

// Get returns a snapshot of current metrics
func (m *Metrics) Get() Snapshot {
	snapshot := Snapshot{
		GetTotal:    m.getTotal.Load(),
		PutTotal:    m.putTotal.Load(),
		DeleteTotal: m.deleteTotal.Load(),

		GetErrors:    m.getErrors.Load(),
		PutErrors:    m.putErrors.Load(),
		DeleteErrors: m.deleteErrors.Load(),
	}

	m.getLatency.Lock()
	if m.getCount > 0 {
		snapshot.AvgGetLatencyMs = float64(m.getLatencySum/m.getCount) / 1e6
	}
	m.getLatency.Unlock()

	m.putLatency.Lock()
	if m.putCount > 0 {
		snapshot.AvgPutLatencyMs = float64(m.putLatencySum/m.putCount) / 1e6
	}
	m.putLatency.Unlock()

	return snapshot
}
