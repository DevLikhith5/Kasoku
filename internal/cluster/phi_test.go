package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPhiDetector_NoHeartbeats(t *testing.T) {
	d := &PhiDetector{}
	// No heartbeats — phi should be 0 (not enough samples)
	assert.Equal(t, 0.0, d.Phi())
	assert.True(t, d.IsAlive())
}

func TestPhiDetector_FewHeartbeats(t *testing.T) {
	d := &PhiDetector{}
	// Less than MinSamples — should always return 0
	for i := 0; i < PhiMinSamples-1; i++ {
		d.Heartbeat()
		time.Sleep(1 * time.Millisecond)
	}
	assert.Equal(t, 0.0, d.Phi())
	assert.True(t, d.IsAlive())
}

func TestPhiDetector_HealthyNode(t *testing.T) {
	d := &PhiDetector{}
	// Simulate rapid heartbeats
	for i := 0; i < 20; i++ {
		d.Heartbeat()
		time.Sleep(5 * time.Millisecond)
	}
	// Just heartbeated — should have very low phi
	phi := d.Phi()
	assert.Less(t, phi, PhiThreshold, "healthy node should have low phi")
	assert.True(t, d.IsAlive())
}

func TestPhiDetector_SlidingWindow(t *testing.T) {
	d := &PhiDetector{}
	// Fill more than the window size
	for i := 0; i < PhiSampleWindowSize+50; i++ {
		d.Heartbeat()
	}
	d.mu.Lock()
	assert.LessOrEqual(t, len(d.intervals), PhiSampleWindowSize,
		"should not exceed window size")
	d.mu.Unlock()
}

func TestPhiDetectorMap_NewNode(t *testing.T) {
	m := NewPhiDetectorMap()
	// Unknown node should be assumed alive
	assert.True(t, m.IsAlive("unknown-node"))
	assert.Equal(t, 0.0, m.Phi("unknown-node"))
}

func TestPhiDetectorMap_Heartbeat(t *testing.T) {
	m := NewPhiDetectorMap()
	// Record heartbeats
	for i := 0; i < 20; i++ {
		m.Heartbeat("node-1")
		time.Sleep(2 * time.Millisecond)
	}
	assert.True(t, m.IsAlive("node-1"))
}

func TestPhiDetectorMap_Remove(t *testing.T) {
	m := NewPhiDetectorMap()
	m.Heartbeat("node-1")
	m.Remove("node-1")
	// After removal, should be assumed alive (no data)
	assert.True(t, m.IsAlive("node-1"))
	assert.Equal(t, 0.0, m.Phi("node-1"))
}
