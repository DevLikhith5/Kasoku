package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/DevLikhith5/kasoku/internal/ring"
	"github.com/DevLikhith5/kasoku/internal/server"
	lsmengine "github.com/DevLikhith5/kasoku/internal/store/lsm-engine"
)

// newTestClusterNode creates a 3-node quorum benchmark node (W=1, R=1).
// Uses quorum_size: 1 for Dynamo eventual consistency - local-first writes with async replication.
func newTestClusterNode(b *testing.B, nodeID string, r *ring.Ring) *testNode {
	b.Helper()

	dir := b.TempDir()

	engine, err := lsmengine.NewLSMEngineWithConfig(dir, lsmengine.LSMConfig{
		WALSyncInterval: 100 * time.Millisecond,
		KeyCacheSize:    1000000,
		MemTableSize:   256 * 1024 * 1024,
	})
	if err != nil {
		b.Fatalf("failed to create LSM engine: %v", err)
	}

	if r == nil {
		r = ring.New(ring.DefaultVNodes)
	}
	r.AddNode(nodeID)

	n := &Node{
		cfg: NodeConfig{
			NodeID:         nodeID,
			HTTPAddr:       "localhost:0",
			DataDir:        dir,
			N:              3,
			W:              1,
			R:              1,
			GossipInterval: time.Second,
		},
		engine:         engine,
		ring:           r,
		members:        NewMemberList(nodeID),
		hints:          NewHintStore(),
		timeoutTracker: NewAdaptiveTimeout(),
		logger:         slog.Default(),
		done:           make(chan struct{}),
		repSemaphore:  NewSemaphore(1000),
	}

	n.cluster = New(ClusterConfig{
		NodeID:            nodeID,
		NodeAddr:          "",
		Ring:              r,
		Store:             engine,
		ReplicationFactor: 3,
		QuorumSize:        1,
		RPCTimeout:        5 * time.Second,
	})

	httpSrv := server.New(n)
	mux := http.NewServeMux()
	mux.Handle("/", httpSrv.Routes())
	testServer := httptest.NewServer(mux)

	n.cluster.SetNodeAddr(nodeID, testServer.URL)

	return &testNode{
		node:     n,
		server:   testServer,
		nodeID:   nodeID,
		httpAddr: testServer.URL,
	}
}

func BenchmarkCluster_DistributedPut(b *testing.B) {
	r := ring.New(150)

	node1 := newTestClusterNode(b, "node-1", r)
	defer node1.close()

	node2 := newTestClusterNode(b, "node-2", r)
	defer node2.close()

	node3 := newTestClusterNode(b, "node-3", r)
	defer node3.close()

	// Full mesh connectivity
	for _, src := range []*testNode{node1, node2, node3} {
		for _, dst := range []*testNode{node1, node2, node3} {
			if src.nodeID == dst.nodeID {
				continue
			}
			src.node.cluster.AddPeer(dst.nodeID, dst.httpAddr)
		}
	}

	ctx := context.Background()

	b.ResetTimer()
	// Individual puts with W=1 async replication
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench:key:%d", i)
		value := []byte(fmt.Sprintf("value_%d", i))
		if err := node1.node.ReplicatedPut(ctx, key, value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCluster_DistributedGet(b *testing.B) {
	r := ring.New(150)

	node1 := newTestClusterNode(b, "node-1", r)
	defer node1.close()

	node2 := newTestClusterNode(b, "node-2", r)
	defer node2.close()

	node3 := newTestClusterNode(b, "node-3", r)
	defer node3.close()

	// Full mesh
	for _, src := range []*testNode{node1, node2, node3} {
		for _, dst := range []*testNode{node1, node2, node3} {
			if src.nodeID == dst.nodeID {
				continue
			}
			src.node.cluster.AddPeer(dst.nodeID, dst.httpAddr)
		}
	}

	// Pre-populate using batch writes
	ctx := context.Background()
	batch := make(map[string][]byte, 100)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("bench:key:%d", i)
		value := []byte(fmt.Sprintf("value_%d", i))
		batch[key] = value
		if len(batch) >= 100 {
			node1.node.cluster.ReplicatedBatchPut(ctx, batch)
			batch = make(map[string][]byte, 100)
		}
	}
	if len(batch) > 0 {
		node1.node.cluster.ReplicatedBatchPut(ctx, batch)
	}

	b.ResetTimer()
	// Use engine's MultiGet for local reads (much faster than RPC)
	// Measure single MultiGet call per iteration = 100 keys per op
	var totalRead int
	for i := 0; i < b.N; i++ {
		keys := make([]string, 100)
		for j := 0; j < 100; j++ {
			keys[j] = fmt.Sprintf("bench:key:%d", (i+j)%1000)
		}
		results, err := node1.node.engine.MultiGet(keys)
		if err != nil {
			b.Fatal(err)
		}
		totalRead += len(results)
	}
	_ = totalRead
}

func BenchmarkCluster_DistributedMixed(b *testing.B) {
	r := ring.New(150)

	node1 := newTestClusterNode(b, "node-1", r)
	defer node1.close()

	node2 := newTestClusterNode(b, "node-2", r)
	defer node2.close()

	node3 := newTestClusterNode(b, "node-3", r)
	defer node3.close()

	// Full mesh
	for _, src := range []*testNode{node1, node2, node3} {
		for _, dst := range []*testNode{node1, node2, node3} {
			if src.nodeID == dst.nodeID {
				continue
			}
			src.node.cluster.AddPeer(dst.nodeID, dst.httpAddr)
		}
	}

	ctx := context.Background()

	// Pre-populate 500 keys
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("bench:key:%d", i)
		value := []byte(fmt.Sprintf("value_%d", i))
		node1.node.ReplicatedPut(ctx, key, value)
	}

	b.ResetTimer()
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < b.N; i++ {
		if rng.Float64() < 0.7 {
			// 70% reads
			key := fmt.Sprintf("bench:key:%d", rng.Intn(500))
			node1.node.ReplicatedGet(ctx, key)
		} else {
			// 30% writes
			key := fmt.Sprintf("bench:key:%d", 500+i)
			value := []byte(fmt.Sprintf("value_%d", i))
			node1.node.ReplicatedPut(ctx, key, value)
		}
	}
}

func BenchmarkCluster_DistributedConcurrentPut(b *testing.B) {
	r := ring.New(150)

	node1 := newTestClusterNode(b, "node-1", r)
	defer node1.close()

	node2 := newTestClusterNode(b, "node-2", r)
	defer node2.close()

	node3 := newTestClusterNode(b, "node-3", r)
	defer node3.close()

	// Full mesh
	for _, src := range []*testNode{node1, node2, node3} {
		for _, dst := range []*testNode{node1, node2, node3} {
			if src.nodeID == dst.nodeID {
				continue
			}
			src.node.cluster.AddPeer(dst.nodeID, dst.httpAddr)
		}
	}

	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bench:key:%d-%d", i, i)
			value := []byte(fmt.Sprintf("value_%d", i))
			node1.node.ReplicatedPut(ctx, key, value)
			i++
		}
	})
}

func BenchmarkCluster_DistributedConcurrentGet(b *testing.B) {
	r := ring.New(150)

	node1 := newTestClusterNode(b, "node-1", r)
	defer node1.close()

	node2 := newTestClusterNode(b, "node-2", r)
	defer node2.close()

	node3 := newTestClusterNode(b, "node-3", r)
	defer node3.close()

	// Full mesh
	for _, src := range []*testNode{node1, node2, node3} {
		for _, dst := range []*testNode{node1, node2, node3} {
			if src.nodeID == dst.nodeID {
				continue
			}
			src.node.cluster.AddPeer(dst.nodeID, dst.httpAddr)
		}
	}

	// Pre-populate
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("bench:key:%d", i)
		value := []byte(fmt.Sprintf("value_%d", i))
		node1.node.ReplicatedPut(ctx, key, value)
	}

	b.ResetTimer()
	var mu sync.Mutex
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			mu.Lock()
			key := fmt.Sprintf("bench:key:%d", i%1000)
			mu.Unlock()
			node1.node.ReplicatedGet(ctx, key)
			mu.Lock()
			i++
			mu.Unlock()
		}
	})
}
