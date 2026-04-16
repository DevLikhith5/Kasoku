package cluster

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidQuorumConfig is returned when quorum configuration is invalid
	ErrInvalidQuorumConfig = errors.New("quorum size cannot exceed replication factor")
	// ErrQuorumImpossible is returned when there aren't enough healthy nodes to achieve quorum
	ErrQuorumImpossible = errors.New("quorum impossible: not enough healthy nodes")
	// ErrFencingTokenMismatch is returned when a fencing token check fails
	ErrFencingTokenMismatch = errors.New("fencing token mismatch - possible split brain")
)

// ClusterEpoch represents a logical clock for fencing.
// It increments whenever cluster membership changes significantly.
type ClusterEpoch struct {
	Epoch     uint64 // Monotonically increasing epoch
	LeaderID  string // Node that coordinated the epoch change
	Timestamp int64  // When the epoch started
}

// FencingToken is used to prevent split-brain scenarios.
// Each operation carries a token that must match the current epoch.
type FencingToken struct {
	Epoch    uint64 // The epoch this operation belongs to
	Sequence uint64 // Sequence number within the epoch
}

// EpochManager manages cluster epochs for fencing.
type EpochManager struct {
	currentEpoch ClusterEpoch
	sequence     uint64
	mu           chan struct{} // Use channel for simple mutex behavior
}

// NewEpochManager creates a new epoch manager.
func NewEpochManager(nodeID string) *EpochManager {
	return &EpochManager{
		currentEpoch: ClusterEpoch{
			Epoch:     1, // Start at epoch 1
			LeaderID:  nodeID,
			Timestamp: 0,
		},
		mu: make(chan struct{}, 1),
	}
}

// GetToken returns the current fencing token.
func (em *EpochManager) GetToken() FencingToken {
	em.mu <- struct{}{}
	defer func() { <-em.mu }()

	em.sequence++
	return FencingToken{
		Epoch:    em.currentEpoch.Epoch,
		Sequence: em.sequence,
	}
}

// ValidateToken checks if a token matches the current epoch.
func (em *EpochManager) ValidateToken(token FencingToken) error {
	em.mu <- struct{}{}
	defer func() { <-em.mu }()

	if token.Epoch != em.currentEpoch.Epoch {
		return fmt.Errorf("%w: expected epoch %d, got %d",
			ErrFencingTokenMismatch, em.currentEpoch.Epoch, token.Epoch)
	}
	return nil
}

// IncrementEpoch creates a new epoch (called on membership changes).
func (em *EpochManager) IncrementEpoch(leaderID string) ClusterEpoch {
	em.mu <- struct{}{}
	defer func() { <-em.mu }()

	em.currentEpoch.Epoch++
	em.currentEpoch.LeaderID = leaderID
	em.currentEpoch.Timestamp = 0 // Would use time.Now().Unix() in real implementation
	em.sequence = 0

	return em.currentEpoch
}

// GetCurrentEpoch returns the current epoch info.
func (em *EpochManager) GetCurrentEpoch() ClusterEpoch {
	em.mu <- struct{}{}
	defer func() { <-em.mu }()

	return em.currentEpoch
}

// QuorumValidator validates quorum configurations and checks feasibility.
type QuorumValidator struct {
	replicationFactor int
	writeQuorum       int
	readQuorum        int
}

// NewQuorumValidator creates a new quorum validator with validation.
func NewQuorumValidator(n, w, r int) (*QuorumValidator, error) {
	if n <= 0 {
		n = 3 // default
	}
	if w <= 0 {
		w = 2 // default
	}
	if r <= 0 {
		r = 2 // default
	}

	// Validate quorum constraints
	if w > n {
		return nil, fmt.Errorf("%w: write quorum %d > replication factor %d",
			ErrInvalidQuorumConfig, w, n)
	}
	if r > n {
		return nil, fmt.Errorf("%w: read quorum %d > replication factor %d",
			ErrInvalidQuorumConfig, r, n)
	}
	if w+r <= n {
		return nil, fmt.Errorf("write quorum (%d) + read quorum (%d) must exceed replication factor (%d) to ensure consistency",
			w, r, n)
	}

	return &QuorumValidator{
		replicationFactor: n,
		writeQuorum:       w,
		readQuorum:        r,
	}, nil
}

// CheckQuorumFeasible checks if quorum can be achieved with the given healthy nodes.
func (qv *QuorumValidator) CheckQuorumFeasible(healthyNodes int, operation string) error {
	required := qv.writeQuorum
	if operation == "read" {
		required = qv.readQuorum
	}

	if healthyNodes < required {
		return fmt.Errorf("%w: need %d nodes for %s, only %d healthy",
			ErrQuorumImpossible, required, operation, healthyNodes)
	}
	return nil
}

// GetReplicationFactor returns the replication factor.
func (qv *QuorumValidator) GetReplicationFactor() int {
	return qv.replicationFactor
}

// GetWriteQuorum returns the write quorum size.
func (qv *QuorumValidator) GetWriteQuorum() int {
	return qv.writeQuorum
}

// GetReadQuorum returns the read quorum size.
func (qv *QuorumValidator) GetReadQuorum() int {
	return qv.readQuorum
}

// MembershipConsensus tracks cluster membership with conflict resolution.
type MembershipConsensus struct {
	mu          chan struct{}
	members     map[string]*ConsensusMemberState
	incarnation map[string]int // incarnation numbers for conflict resolution
}

// ConsensusMemberState represents the state of a member in the cluster.
type ConsensusMemberState struct {
	NodeID      string
	Address     string
	IsAlive     bool
	Incarnation int
}

// NewMembershipConsensus creates a new membership consensus tracker.
func NewMembershipConsensus() *MembershipConsensus {
	return &MembershipConsensus{
		mu:          make(chan struct{}, 1),
		members:     make(map[string]*ConsensusMemberState),
		incarnation: make(map[string]int),
	}
}

// UpdateMember updates a member with conflict resolution using incarnation numbers.
// Returns true if the update was applied, false if it was rejected.
func (mc *MembershipConsensus) UpdateMember(nodeID string, isAlive bool, incarnation int) bool {
	mc.mu <- struct{}{}
	defer func() { <-mc.mu }()

	currentIncarnation, exists := mc.incarnation[nodeID]

	// If we've seen a higher incarnation, reject this update
	if exists && incarnation < currentIncarnation {
		return false // stale update, reject it
	}

	// Accept the update
	mc.incarnation[nodeID] = incarnation
	mc.members[nodeID] = &ConsensusMemberState{
		NodeID:      nodeID,
		IsAlive:     isAlive,
		Incarnation: incarnation,
	}

	return true
}

// GetMember returns a member's current state.
func (mc *MembershipConsensus) GetMember(nodeID string) (*ConsensusMemberState, bool) {
	mc.mu <- struct{}{}
	defer func() { <-mc.mu }()

	member, exists := mc.members[nodeID]
	return member, exists
}

// GetHealthyMembers returns the count of healthy (alive) members.
func (mc *MembershipConsensus) GetHealthyMembers() int {
	mc.mu <- struct{}{}
	defer func() { <-mc.mu }()

	count := 0
	for _, member := range mc.members {
		if member.IsAlive {
			count++
		}
	}
	return count
}

// GetAllMembers returns all known members.
func (mc *MembershipConsensus) GetAllMembers() map[string]*ConsensusMemberState {
	mc.mu <- struct{}{}
	defer func() { <-mc.mu }()

	// Return a copy to avoid external modification
	result := make(map[string]*ConsensusMemberState, len(mc.members))
	for k, v := range mc.members {
		result[k] = v
	}
	return result
}
