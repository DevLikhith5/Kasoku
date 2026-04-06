package cluster

import (
	"math/rand"
	"sync"
	"time"
)

// MemberState represents the state of a cluster member
type MemberState int

const (
	MemberStateAlive MemberState = iota
	MemberStateSuspect
	MemberStateDead
)

func (s MemberState) String() string {
	switch s {
	case MemberStateAlive:
		return "alive"
	case MemberStateSuspect:
		return "suspect"
	case MemberStateDead:
		return "dead"
	default:
		return "unknown"
	}
}

// Member represents a single cluster member
type Member struct {
	NodeID     string
	Address    string
	State      MemberState
	LastSeen   time.Time
	Incarnation int // incremented when member updates its own state
}

// MemberList maintains the gossip-based membership view of the cluster
type MemberList struct {
	mu       sync.RWMutex
	self     string
	members  map[string]*Member // nodeID -> Member
	seeded   bool
}

// NewMemberList creates a new membership list with the given node as self
func NewMemberList(selfNodeID string) *MemberList {
	ml := &MemberList{
		self:    selfNodeID,
		members: make(map[string]*Member),
	}
	// Add self as alive
	ml.members[selfNodeID] = &Member{
		NodeID:      selfNodeID,
		Address:     selfNodeID,
		State:       MemberStateAlive,
		LastSeen:    time.Now(),
		Incarnation: 1,
	}
	return ml
}

// AddMember adds or updates a member in the list
func (ml *MemberList) AddMember(nodeID string) {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	if _, exists := ml.members[nodeID]; exists {
		// Already known, just update last seen
		ml.members[nodeID].LastSeen = time.Now()
		return
	}

	ml.members[nodeID] = &Member{
		NodeID:      nodeID,
		Address:     nodeID,
		State:       MemberStateAlive,
		LastSeen:    time.Now(),
		Incarnation: 1,
	}
}

// Merge merges remote membership info and returns the combined view
func (ml *MemberList) Merge(remoteMembers []string) []string {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	known := make(map[string]bool)
	for nodeID := range ml.members {
		known[nodeID] = true
	}

	// Add any members we don't know about
	for _, nodeID := range remoteMembers {
		if !known[nodeID] {
			ml.members[nodeID] = &Member{
				NodeID:      nodeID,
				Address:     nodeID,
				State:       MemberStateAlive,
				LastSeen:    time.Now(),
				Incarnation: 1,
			}
			known[nodeID] = true
		}
	}

	// Return our full member list
	result := make([]string, 0, len(ml.members))
	for nodeID := range ml.members {
		result = append(result, nodeID)
	}
	return result
}

// Members returns all known member node IDs (excluding self if dead)
func (ml *MemberList) Members() []string {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	result := make([]string, 0, len(ml.members))
	for nodeID, m := range ml.members {
		if m.State != MemberStateDead {
			result = append(result, nodeID)
		}
	}
	return result
}

// RandomMember returns a random member other than self
func (ml *MemberList) RandomMember() string {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	alive := make([]string, 0, len(ml.members))
	for nodeID, m := range ml.members {
		if nodeID != ml.self && m.State != MemberStateDead {
			alive = append(alive, nodeID)
		}
	}
	if len(alive) == 0 {
		return ""
	}
	return alive[rand.Intn(len(alive))]
}

// MemberCount returns the number of known members
func (ml *MemberList) MemberCount() int {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	return len(ml.members)
}

// Tick is called periodically to check for dead members
func (ml *MemberList) Tick() {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	now := time.Now()
	deadTimeout := 30 * time.Second

	for nodeID, m := range ml.members {
		if nodeID == ml.self {
			continue
		}
		if m.State == MemberStateAlive && now.Sub(m.LastSeen) > deadTimeout {
			m.State = MemberStateSuspect
		} else if m.State == MemberStateSuspect && now.Sub(m.LastSeen) > deadTimeout*2 {
			m.State = MemberStateDead
		}
	}
}

// MarkAlive marks a member as alive
func (ml *MemberList) MarkAlive(nodeID string) {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	if m, exists := ml.members[nodeID]; exists {
		m.State = MemberStateAlive
		m.LastSeen = time.Now()
		m.Incarnation++
	} else {
		ml.members[nodeID] = &Member{
			NodeID:      nodeID,
			Address:     nodeID,
			State:       MemberStateAlive,
			LastSeen:    time.Now(),
			Incarnation: 1,
		}
	}
}

// MarkDead marks a member as dead
func (ml *MemberList) MarkDead(nodeID string) {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	if m, exists := ml.members[nodeID]; exists {
		m.State = MemberStateDead
		m.LastSeen = time.Now()
	}
}

// IsAlive checks if a member is considered alive
func (ml *MemberList) IsAlive(nodeID string) bool {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	m, exists := ml.members[nodeID]
	if !exists {
		return false
	}
	return m.State == MemberStateAlive
}
