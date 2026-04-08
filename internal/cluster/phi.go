package cluster

import (
	"math"
	"sync"
	"time"
)

const (
	// PhiThreshold is the phi value above which a node is declared dead.
	// phi=1 means ~10% chance of false positive
	// phi=2 means ~1% chance
	// phi=8 means ~0.000001% chance — essentially certain the node is dead
	PhiThreshold = 8.0

	// PhiSampleWindowSize is the number of recent heartbeat intervals to track
	PhiSampleWindowSize = 1000

	// PhiMinSamples is the minimum number of heartbeat samples needed before
	// the detector can make any judgment. Below this, nodes are assumed alive.
	PhiMinSamples = 10
)

// PhiDetector implements a phi accrual failure detector for a single node.
// Unlike fixed timeouts, it adapts to the observed heartbeat pattern:
//   - If a node usually heartbeats every 200ms, a 2s gap is very suspicious
//   - If a node usually heartbeats every 5s, a 6s gap is perfectly normal
//
// The phi value represents how many orders of magnitude the current silence
// exceeds the expected inter-arrival time.
type PhiDetector struct {
	mu          sync.Mutex
	intervals   []float64 // inter-arrival times in milliseconds
	lastArrival time.Time
}

// Heartbeat records that we received a heartbeat right now.
// Called each time we hear from the monitored node.
func (d *PhiDetector) Heartbeat() {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	if !d.lastArrival.IsZero() {
		interval := float64(now.Sub(d.lastArrival).Milliseconds())
		d.intervals = append(d.intervals, interval)
		if len(d.intervals) > PhiSampleWindowSize {
			d.intervals = d.intervals[1:] // sliding window
		}
	}
	d.lastArrival = now
}

// Phi returns the current suspicion level.
//
//	phi near 0 = healthy (heard from recently, relative to normal pattern)
//	phi > 8    = almost certainly dead
//
// The model uses exponential distribution:
//
//	phi = elapsed / mean / ln(10)
//	phi=1 means P(alive) ≈ 10%
//	phi=2 means P(alive) ≈ 1%
//	phi=8 means P(alive) ≈ 0.000001%
func (d *PhiDetector) Phi() float64 {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.intervals) < PhiMinSamples {
		return 0 // not enough data to judge
	}

	elapsed := float64(time.Since(d.lastArrival).Milliseconds())
	mean := d.mean()
	if mean <= 0 {
		return 0
	}

	return (elapsed / mean) / math.Log(10)
}

func (d *PhiDetector) IsAlive() bool {
	return d.Phi() < PhiThreshold
}

func (d *PhiDetector) mean() float64 {
	if len(d.intervals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range d.intervals {
		sum += v
	}
	return sum / float64(len(d.intervals))
}

// PhiDetectorMap tracks one phi detector per remote node.
// Thread-safe for concurrent access from gossip and health check goroutines.
type PhiDetectorMap struct {
	mu        sync.RWMutex
	detectors map[string]*PhiDetector
}

func NewPhiDetectorMap() *PhiDetectorMap {
	return &PhiDetectorMap{detectors: make(map[string]*PhiDetector)}
}

func (m *PhiDetectorMap) Heartbeat(nodeID string) {
	m.mu.Lock()
	if _, ok := m.detectors[nodeID]; !ok {
		m.detectors[nodeID] = &PhiDetector{}
	}
	d := m.detectors[nodeID]
	m.mu.Unlock()
	d.Heartbeat()
}

func (m *PhiDetectorMap) IsAlive(nodeID string) bool {
	m.mu.RLock()
	d, ok := m.detectors[nodeID]
	m.mu.RUnlock()
	if !ok {
		return true // no data yet — assume alive
	}
	return d.IsAlive()
}

func (m *PhiDetectorMap) Phi(nodeID string) float64 {
	m.mu.RLock()
	d, ok := m.detectors[nodeID]
	m.mu.RUnlock()
	if !ok {
		return 0
	}
	return d.Phi()
}

func (m *PhiDetectorMap) Remove(nodeID string) {
	m.mu.Lock()
	delete(m.detectors, nodeID)
	m.mu.Unlock()
}
