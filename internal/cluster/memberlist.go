package cluster

import (
	"math/rand"
	"sync"
	"time"
)

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

type Member struct {
	NodeID      string
	Address     string
	State       MemberState
	LastSeen    time.Time
	Incarnation int // incremented when member updates its own state
}

type MemberList struct {
	mu      sync.RWMutex
	self    string
	members map[string]*Member // nodeID -> Member

}

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

// Merge merges remote membership info and returns the combined view.
// Bug 10 fix: if a remote peer reports a node as alive that we considered dead
// or suspect, we revive it — this allows recovered nodes to be re-discovered.
func (ml *MemberList) Merge(remoteMembers []string) []string {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	for _, nodeID := range remoteMembers {
		if existing, ok := ml.members[nodeID]; ok {
			// Node already known: revive it if dead/suspect and update last seen
			if existing.State != MemberStateAlive {
				existing.State = MemberStateAlive
				existing.Incarnation++
			}
			existing.LastSeen = time.Now()
		} else {
			// Brand new node — add it
			ml.members[nodeID] = &Member{
				NodeID:      nodeID,
				Address:     nodeID,
				State:       MemberStateAlive,
				LastSeen:    time.Now(),
				Incarnation: 1,
			}
		}
	}

	// Return our full member list
	result := make([]string, 0, len(ml.members))
	for nodeID := range ml.members {
		result = append(result, nodeID)
	}
	return result
}

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

func (ml *MemberList) GetRandomPeers(count int) []string {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	alive := make([]string, 0, len(ml.members))
	for nodeID, m := range ml.members {
		if nodeID != ml.self && m.State == MemberStateAlive {
			alive = append(alive, nodeID)
		}
	}
	if len(alive) == 0 {
		return nil
	}

	if len(alive) <= count {
		return alive
	}

	result := make([]string, count)
	for i := range result {
		idx := rand.Intn(len(alive))
		result[i] = alive[idx]
		alive = append(alive[:idx], alive[idx+1:]...)
	}
	return result
}

func (ml *MemberList) MemberCount() int {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	return len(ml.members)
}

func (ml *MemberList) AliveCount() int {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	count := 0
	for _, m := range ml.members {
		if m.State == MemberStateAlive {
			count++
		}
	}
	return count
}

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

func (ml *MemberList) MarkDead(nodeID string) {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	if m, exists := ml.members[nodeID]; exists {
		m.State = MemberStateDead
		m.LastSeen = time.Now()
	}
}

func (ml *MemberList) IsAlive(nodeID string) bool {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	m, exists := ml.members[nodeID]
	if !exists {
		return false
	}
	return m.State == MemberStateAlive
}

// AliveSet returns a snapshot map of all currently-alive node IDs.
// Use this instead of calling IsAlive in a loop to avoid per-call lock overhead.
func (ml *MemberList) AliveSet() map[string]bool {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	alive := make(map[string]bool, len(ml.members))
	for id, m := range ml.members {
		if m.State == MemberStateAlive {
			alive[id] = true
		}
	}
	return alive
}
