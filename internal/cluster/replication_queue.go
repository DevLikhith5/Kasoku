package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	rpc "github.com/DevLikhith5/kasoku/internal/rpc"
)

const (
	DefaultMaxQueueSize  = 50000
	DefaultFlushInterval = 20 * time.Millisecond
	DefaultMaxBatchSize  = 1000
)

var ErrQueueFull = errors.New("replication queue full")

type ReplicationQueue struct {
	mu      sync.Mutex
	pending []ReplicationEntry
	maxSize int

	flushInterval time.Duration
	maxBatchSize  int

	enqueued    atomic.Int64
	dequeued    atomic.Int64
	dropped     atomic.Int64
	flushErrors atomic.Int64
}

type ReplicationEntry struct {
	Key       string
	Value     []byte
	Tombstone bool
	Version   uint64
}

type PeerStats struct {
	Address  string
	Pending  atomic.Int64
	Success  atomic.Int64
	Failures atomic.Int64
	LastSync atomic.Int64
}

func NewReplicationQueue(maxSize int, flushInterval time.Duration, maxBatchSize int) *ReplicationQueue {
	if maxSize <= 0 {
		maxSize = DefaultMaxQueueSize
	}
	if flushInterval <= 0 {
		flushInterval = DefaultFlushInterval
	}
	if maxBatchSize <= 0 {
		maxBatchSize = DefaultMaxBatchSize
	}

	return &ReplicationQueue{
		pending:       make([]ReplicationEntry, 0, maxSize),
		maxSize:       maxSize,
		flushInterval: flushInterval,
		maxBatchSize:  maxBatchSize,
	}
}

func (q *ReplicationQueue) Enqueue(key string, value []byte, tombstone bool, version uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.pending) >= q.maxSize {
		q.dropped.Add(1)
		return ErrQueueFull
	}

	q.pending = append(q.pending, ReplicationEntry{
		Key:       key,
		Value:     value,
		Tombstone: tombstone,
		Version:   version,
	})
	q.enqueued.Add(1)

	return nil
}

func (q *ReplicationQueue) Flush() []ReplicationEntry {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.pending) == 0 {
		return nil
	}

	entries := q.pending
	q.pending = make([]ReplicationEntry, 0, q.maxSize)

	q.dequeued.Add(int64(len(entries)))
	return entries
}

func (q *ReplicationQueue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}

func (q *ReplicationQueue) Stats() (enqueued, dequeued, dropped, flushErrors int64) {
	return q.enqueued.Load(), q.dequeued.Load(), q.dropped.Load(), q.flushErrors.Load()
}

type BackgroundReplicator struct {
	cluster   *Cluster
	queue     *ReplicationQueue
	peerStats map[string]*PeerStats
	stopCh    chan struct{}
	wg        sync.WaitGroup
	running   atomic.Bool
	mu        sync.Mutex
	logger     *slog.Logger
}

func NewBackgroundReplicator(cluster *Cluster, queue *ReplicationQueue, logger *slog.Logger) *BackgroundReplicator {
	if logger == nil {
		logger = slog.Default()
	}
	return &BackgroundReplicator{
		cluster:   cluster,
		queue:     queue,
		peerStats: make(map[string]*PeerStats),
		stopCh:    make(chan struct{}),
		logger:    logger,
	}
}

func (r *BackgroundReplicator) Start(ctx context.Context) {
	if r.running.Load() {
		return
	}
	r.running.Store(true)

	r.wg.Add(1)
	go r.flushLoop(ctx)

	r.logger.Info("background replicator started",
		"flush_interval", r.queue.flushInterval,
		"max_batch", r.queue.maxBatchSize)
}

func (r *BackgroundReplicator) Stop() {
	if !r.running.Load() {
		return
	}
	r.running.Store(false)
	close(r.stopCh)
	r.wg.Wait()
	r.logger.Info("background replicator stopped")
}

func (r *BackgroundReplicator) Enqueue(key string, value []byte, tombstone bool, version uint64) error {
	return r.queue.Enqueue(key, value, tombstone, version)
}

func (r *BackgroundReplicator) flushLoop(ctx context.Context) {
	defer r.wg.Done()

	ticker := time.NewTicker(r.queue.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.flushToPeers(ctx)
		}
	}
}

func (r *BackgroundReplicator) flushToPeers(ctx context.Context) {
	entries := r.queue.Flush()
	if len(entries) == 0 {
		return
	}

	entriesByPeer := r.groupEntriesByReplica(entries)

	var wg sync.WaitGroup
	for addr, peerEntries := range entriesByPeer {
		wg.Add(1)
		go func(peerAddr string, entries []ReplicationEntry) {
			defer wg.Done()

			peerStats := r.getPeerStats(peerAddr)
			peerStats.Pending.Add(int64(len(entries)))

			rpcEntries := make([]rpc.BatchWriteEntry, len(entries))
			for i, e := range entries {
				rpcEntries[i] = rpc.BatchWriteEntry{
					Key:   e.Key,
					Value: e.Value,
				}
			}

			replCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			client, ok := r.cluster.getClient(peerAddr)
			if !ok {
				peerStats.Failures.Add(1)
				r.queue.flushErrors.Add(1)
				return
			}

			if _, err := client.BatchReplicatedPut(replCtx, rpcEntries); err != nil {
				peerStats.Failures.Add(1)
				r.queue.flushErrors.Add(1)
			} else {
				peerStats.Success.Add(1)
				peerStats.LastSync.Store(time.Now().UnixMilli())
			}
			peerStats.Pending.Add(-int64(len(entries)))
		}(addr, peerEntries)
	}

	wg.Wait()
}

func (r *BackgroundReplicator) groupEntriesByReplica(entries []ReplicationEntry) map[string][]ReplicationEntry {
	result := make(map[string][]ReplicationEntry)
	aliveSet := r.cluster.snapshotAliveSet()

	for _, entry := range entries {
		replicas := r.cluster.getReplicasForKey(entry.Key, aliveSet)
		for _, replica := range replicas {
			if replica != r.cluster.nodeID && replica != r.cluster.nodeAddr {
				result[replica] = append(result[replica], entry)
			}
		}
	}

	return result
}

func (r *BackgroundReplicator) getPeerStats(peerAddr string) *PeerStats {
	r.mu.Lock()
	defer r.mu.Unlock()

	stats, ok := r.peerStats[peerAddr]
	if !ok {
		stats = &PeerStats{Address: peerAddr}
		r.peerStats[peerAddr] = stats
	}
	return stats
}

func (r *BackgroundReplicator) GetStats() map[string]interface{} {
	enqueued, dequeued, dropped, flushErrors := r.queue.Stats()
	peerStatsMap := make(map[string]map[string]interface{})

	r.mu.Lock()
	for addr, stats := range r.peerStats {
		peerStatsMap[addr] = map[string]interface{}{
			"pending":   stats.Pending.Load(),
			"success":   stats.Success.Load(),
			"failures":  stats.Failures.Load(),
			"last_sync": stats.LastSync.Load(),
		}
	}
	r.mu.Unlock()

	return map[string]interface{}{
		"queue_enqueued": enqueued,
		"queue_dequeued": dequeued,
		"queue_dropped":  dropped,
		"flush_errors":   flushErrors,
		"queue_size":     r.queue.Size(),
		"peer_stats":     peerStatsMap,
	}
}

// GossipState represents the state exchanged between nodes
type GossipState struct {
	NodeID        string
	NodeAddr     string
	Version       uint64
	LastHeartbeat int64
	Membership    map[string]MemberInfo
}

type MemberInfo struct {
	Addr      string
	Heartbeat int64
	State     string
}

// GossipProtocol implements Dynamo-style gossip for membership and failure detection
type GossipProtocol struct {
	cluster        *Cluster
	stopCh         chan struct{}
	wg             sync.WaitGroup
	running        atomic.Bool
	state          *GossipState
	gossipInterval time.Duration
	fanout         int
	membership     *MemberList
	mu             sync.Mutex
	logger         *slog.Logger
}

func NewGossipProtocol(cluster *Cluster, membership *MemberList, logger *slog.Logger) *GossipProtocol {
	if logger == nil {
		logger = slog.Default()
	}
	return &GossipProtocol{
		cluster:        cluster,
		stopCh:         make(chan struct{}),
		state:          &GossipState{},
		membership:     membership,
		gossipInterval: 500 * time.Millisecond,
		fanout:         2,
		logger:         logger,
	}
}

func (g *GossipProtocol) Start(ctx context.Context) {
	if g.running.Load() {
		return
	}
	g.running.Store(true)

	g.state.NodeID = g.cluster.nodeID
	g.state.NodeAddr = g.cluster.nodeAddr
	g.state.Membership = make(map[string]MemberInfo)
	g.state.LastHeartbeat = time.Now().UnixMilli()

	g.wg.Add(1)
	go g.gossipLoop(ctx)

	g.logger.Info("gossip protocol started",
		"fanout", g.fanout,
		"interval", g.gossipInterval)
}

func (g *GossipProtocol) Stop() {
	if !g.running.Load() {
		return
	}
	g.running.Store(false)
	close(g.stopCh)
	g.wg.Wait()
}

func (g *GossipProtocol) gossipLoop(ctx context.Context) {
	defer g.wg.Done()

	ticker := time.NewTicker(g.gossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-g.stopCh:
			return
		case <-ticker.C:
			g.exchangeGossip(ctx)
		}
	}
}

func (g *GossipProtocol) exchangeGossip(ctx context.Context) {
	g.mu.Lock()
	g.state.LastHeartbeat = time.Now().UnixMilli()
	g.state.Version++
	localState := &GossipState{
		NodeID:        g.state.NodeID,
		NodeAddr:      g.state.NodeAddr,
		Version:       g.state.Version,
		LastHeartbeat: g.state.LastHeartbeat,
		Membership:    g.state.Membership,
	}
	g.mu.Unlock()

	peers := g.membership.GetRandomPeers(g.fanout)
	if len(peers) == 0 {
		return
	}

	for _, peer := range peers {
		go g.gossipWithPeer(ctx, peer, localState)
	}
}

func (g *GossipProtocol) gossipWithPeer(ctx context.Context, peer string, localState *GossipState) {
	client, ok := g.cluster.getClient(peer)
	if !ok {
		return
	}

	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	membership := make(map[string]string)
	for k, v := range localState.Membership {
		membership[k] = v.Addr
	}

	req := &rpc.GossipStateRequest{
		NodeID:        localState.NodeID,
		NodeAddr:      localState.NodeAddr,
		Version:       localState.Version,
		LastHeartbeat: localState.LastHeartbeat,
		Membership:    membership,
	}

	remoteState, err := client.ExchangeGossip(reqCtx, req)
	if err != nil {
		return
	}

	g.mergeStateResponse(remoteState)
}

func (g *GossipProtocol) mergeStateResponse(remote *rpc.GossipStateResponse) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for nodeID, addr := range remote.Membership {
		localInfo, exists := g.state.Membership[nodeID]
		if !exists || remote.LastHeartbeat > localInfo.Heartbeat {
			g.state.Membership[nodeID] = MemberInfo{
				Addr:      addr,
				Heartbeat: remote.LastHeartbeat,
				State:     "alive",
			}
		}
	}
}

func (g *GossipProtocol) GetMembership() map[string]MemberInfo {
	g.mu.Lock()
	defer g.mu.Unlock()

	result := make(map[string]MemberInfo)
	for k, v := range g.state.Membership {
		result[k] = v
	}
	return result
}

func (g *GossipProtocol) GetStats() map[string]interface{} {
	g.mu.Lock()
	defer g.mu.Unlock()

	return map[string]interface{}{
		"node_id":    g.state.NodeID,
		"version":    g.state.Version,
		"heartbeat":  g.state.LastHeartbeat,
		"membership": g.state.Membership,
	}
}

// MerkleAntiEntropy implements Merkle tree-based anti-entropy repair
type MerkleAntiEntropy struct {
	cluster      *Cluster
	replicator   *BackgroundReplicator
	lastSyncTime atomic.Int64
	syncInterval time.Duration
	stopCh       chan struct{}
	wg           sync.WaitGroup
	running      atomic.Bool
	mu           sync.Mutex
	logger       *slog.Logger
}

func NewMerkleAntiEntropy(cluster *Cluster, replicator *BackgroundReplicator, logger *slog.Logger) *MerkleAntiEntropy {
	if logger == nil {
		logger = slog.Default()
	}
	return &MerkleAntiEntropy{
		cluster:      cluster,
		replicator:   replicator,
		syncInterval: 10 * time.Minute,
		stopCh:       make(chan struct{}),
		logger:       logger,
	}
}

func (ae *MerkleAntiEntropy) Start(ctx context.Context) {
	if ae.running.Load() {
		return
	}
	ae.running.Store(true)
	ae.lastSyncTime.Store(time.Now().Unix())

	ae.wg.Add(1)
	go ae.repairLoop(ctx)

	ae.logger.Info("merkle anti-entropy repair started",
		"interval", ae.syncInterval)
}

func (ae *MerkleAntiEntropy) Stop() {
	if !ae.running.Load() {
		return
	}
	ae.running.Store(false)
	close(ae.stopCh)
	ae.wg.Wait()
}

func (ae *MerkleAntiEntropy) repairLoop(ctx context.Context) {
	defer ae.wg.Done()

	ticker := time.NewTicker(ae.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ae.stopCh:
			return
		case <-ticker.C:
			ae.runRepair(ctx)
		}
	}
}

func (ae *MerkleAntiEntropy) runRepair(ctx context.Context) {
	ae.lastSyncTime.Store(time.Now().Unix())

	peerClients := ae.cluster.GetPeerClients()
	for addr, client := range peerClients {
		ae.repairWithPeer(ctx, addr, client)
	}
}

func (ae *MerkleAntiEntropy) repairWithPeer(ctx context.Context, peerAddr string, client *rpc.Client) {
	localTree := ae.buildLocalMerkleTree()
	localRoot := localTree.RootHash()

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	remoteRoot, err := client.GetMerkleRoot(reqCtx)
	if err != nil {
		ae.logger.Debug("failed to get merkle root", "peer", peerAddr, "error", err)
		return
	}

	if string(localRoot) != string(remoteRoot) {
		ae.logger.Info("merkle mismatch, exchanging keys", "peer", peerAddr)
		ae.exchangeDifferences(ctx, peerAddr, client, localTree)
	}
}

func (ae *MerkleAntiEntropy) buildLocalMerkleTree() *MerkleTree {
	tree := NewMerkleTree(10)

	ae.mu.Lock()
	defer ae.mu.Unlock()

	keys, err := ae.cluster.store.Keys()
	if err != nil {
		ae.logger.Debug("failed to get keys for merkle tree", "error", err)
		return tree
	}

	for _, key := range keys {
		tree.Insert([]byte(key))
	}

	return tree
}

func (ae *MerkleAntiEntropy) exchangeDifferences(ctx context.Context, peerAddr string, client *rpc.Client, localTree *MerkleTree) {
	keys := localTree.GetAllKeys()

	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	diffKeys, err := client.GetKeyDifferences(reqCtx, keys)
	if err != nil {
		ae.logger.Debug("failed to get key differences", "peer", peerAddr, "error", err)
		return
	}

	for _, key := range diffKeys {
		entry, err := ae.cluster.store.Get(key)
		if err != nil {
			continue
		}

		putCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		client.ReplicatedPutBinary(putCtx, key, entry.Value)
		cancel()
	}
}

type MerkleTree struct {
	mu    sync.Mutex
	nodes map[uint64][]byte
	depth int
	hash  func([]byte) []byte
}

func NewMerkleTree(depth int) *MerkleTree {
	return &MerkleTree{
		nodes: make(map[uint64][]byte),
		depth: depth,
		hash:  simpleHash,
	}
}

func (mt *MerkleTree) Insert(key []byte) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	hash := mt.hash(key)
	bucket := uint64(0)
	for i, b := range hash {
		bucket += uint64(b) * uint64(i+1)
	}
	bucket %= 1024

	if mt.nodes[bucket] == nil {
		mt.nodes[bucket] = hash
	} else {
		mt.nodes[bucket] = mt.hash(append(mt.nodes[bucket], hash...))
	}
}

func (mt *MerkleTree) RootHash() []byte {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if len(mt.nodes) == 0 {
		return []byte{}
	}

	var root []byte
	for _, v := range mt.nodes {
		root = mt.hash(append(root, v...))
	}
	return root
}

func (mt *MerkleTree) GetAllKeys() []string {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	keys := make([]string, 0, len(mt.nodes)*10)
	for k := range mt.nodes {
		keys = append(keys, fmt.Sprintf("bucket_%d", k))
	}
	return keys
}

func simpleHash(data []byte) []byte {
	sum := uint64(0)
	for i, b := range data {
		sum += uint64(b) * uint64(i+1)
	}
	h := make([]byte, 8)
	for i := range h {
		h[i] = byte(sum >> (i * 8))
	}
	return h
}
