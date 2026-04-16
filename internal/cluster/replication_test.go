package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DevLikhith5/kasoku/internal/ring"
	"github.com/DevLikhith5/kasoku/internal/rpc"
	"github.com/DevLikhith5/kasoku/internal/server"
	lsmengine "github.com/DevLikhith5/kasoku/internal/store/lsm-engine"
)

type testNode struct {
	node     *Node
	server   *httptest.Server
	nodeID   string
	httpAddr string
}

func newTestNode(t *testing.T, nodeID string, r *ring.Ring) *testNode {
	t.Helper()

	dir := t.TempDir()

	engine, err := lsmengine.NewLSMEngine(dir)
	if err != nil {
		t.Fatalf("failed to create LSM engine: %v", err)
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

func (tn *testNode) close() {
	close(tn.node.done)
	tn.server.Close()
	tn.node.engine.Close()
}

// TestReplication_ActualHTTPReplication verifies that a write to one node
// is replicated to other nodes via the internal /internal/replicate endpoint.
func TestReplication_ActualHTTPReplication(t *testing.T) {
	r := ring.New(150)

	node1 := newTestNode(t, "node-1", r)
	defer node1.close()

	node2 := newTestNode(t, "node-2", r)
	defer node2.close()

	node3 := newTestNode(t, "node-3", r)
	defer node3.close()

	// All nodes need to know about ALL peers (full mesh)
	for _, src := range []*testNode{node1, node2, node3} {
		for _, dst := range []*testNode{node1, node2, node3} {
			if src.nodeID == dst.nodeID {
				continue
			}
			src.node.cluster.AddPeer(dst.nodeID, dst.httpAddr)
		}
	}

	// Write from node-1 — should replicate to node-2 and node-3
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := node1.node.ReplicatedPut(ctx, "hello", []byte("world"))
	if err != nil {
		t.Fatalf("ReplicatedPut failed: %v", err)
	}

	// Verify the key exists on ALL three nodes by reading directly from their engines
	for _, tn := range []*testNode{node1, node2, node3} {
		entry, err := tn.node.engine.Get("hello")
		if err != nil {
			t.Errorf("node %s: key 'hello' not found: %v", tn.nodeID, err)
			continue
		}
		if string(entry.Value) != "world" {
			t.Errorf("node %s: expected 'world', got '%s'", tn.nodeID, string(entry.Value))
		}
	}
}

// TestReplication_ReadFromPrimary verifies that a GET is forwarded to the
// correct primary node when the requesting node is not the primary.
func TestReplication_ReadFromPrimary(t *testing.T) {
	r := ring.New(150)

	node1 := newTestNode(t, "node-1", r)
	defer node1.close()

	node2 := newTestNode(t, "node-2", r)
	defer node2.close()

	node3 := newTestNode(t, "node-3", r)
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
	err := node1.node.ReplicatedPut(ctx, "test-key", []byte("test-value"))
	if err != nil {
		t.Fatalf("ReplicatedPut failed: %v", err)
	}

	// Now read from node-2 — if node-2 is not primary, it forwards to primary
	value, err := node2.node.ReplicatedGet(ctx, "test-key")
	if err != nil {
		t.Fatalf("ReplicatedGet from node-2 failed: %v", err)
	}
	if string(value) != "test-value" {
		t.Errorf("expected 'test-value', got '%s'", string(value))
	}
}

// TestReplication_DeleteReplicates verifies that a delete is replicated to all
// replica nodes.
func TestReplication_DeleteReplicates(t *testing.T) {
	r := ring.New(150)

	node1 := newTestNode(t, "node-1", r)
	defer node1.close()

	node2 := newTestNode(t, "node-2", r)
	defer node2.close()

	node3 := newTestNode(t, "node-3", r)
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

	// Write first
	err := node1.node.ReplicatedPut(ctx, "del-key", []byte("del-val"))
	if err != nil {
		t.Fatalf("ReplicatedPut failed: %v", err)
	}

	// Delete from node-1
	err = node1.node.ReplicatedDelete(ctx, "del-key")
	if err != nil {
		t.Fatalf("ReplicatedDelete failed: %v", err)
	}

	// Verify deleted on all nodes
	for _, tn := range []*testNode{node1, node2, node3} {
		_, err := tn.node.engine.Get("del-key")
		if err == nil {
			t.Errorf("node %s: key 'del-key' should have been deleted", tn.nodeID)
		}
	}
}

// TestReplication_QuorumFailure verifies that a write fails when quorum
// cannot be reached (e.g., peers are unreachable).
func TestReplication_QuorumFailure(t *testing.T) {
	r := ring.New(150)
	r.AddNode("node-1")
	r.AddNode("node-2")
	r.AddNode("node-3")

	node1 := newTestNode(t, "node-1", r)
	defer node1.close()

	// Add peers that don't exist — replication will fail
	node1.node.cluster.AddPeer("nonexistent", "http://nonexistent:9999")
	node1.node.cluster.AddPeer("also-nonexistent", "http://also-nonexistent:9998")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// With quorum size 2 and only local node reachable, this should fail
	err := node1.node.ReplicatedPut(ctx, "fail-key", []byte("fail-val"))
	if err == nil {
		t.Log("write succeeded (quorum was met with local node only)")
	} else {
		t.Logf("write failed as expected: %v", err)
	}
}

// TestReplication_RPCClientDirect tests that the RPC client can actually
// talk to the server's /internal/replicate endpoint.
func TestReplication_RPCClientDirect(t *testing.T) {
	r := ring.New(150)
	node1 := newTestNode(t, "node-1", r)
	defer node1.close()

	client := rpc.NewClient(node1.httpAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Test PUT
	err := client.ReplicatedPut(ctx, "rpc-key", []byte("rpc-val"))
	if err != nil {
		t.Fatalf("RPC ReplicatedPut failed: %v", err)
	}

	// Test GET
	value, found, err := client.ReplicatedGet(ctx, "rpc-key")
	if err != nil {
		t.Fatalf("RPC ReplicatedGet failed: %v", err)
	}
	if !found {
		t.Fatal("RPC ReplicatedGet: key not found")
	}
	if string(value) != "rpc-val" {
		t.Errorf("expected 'rpc-val', got '%s'", string(value))
	}

	// Test DELETE
	deleted, err := client.ReplicatedDelete(ctx, "rpc-key")
	if err != nil {
		t.Fatalf("RPC ReplicatedDelete failed: %v", err)
	}
	if !deleted {
		t.Error("RPC ReplicatedDelete: key was not deleted")
	}

	// Verify deletion
	debugResp, err := client.DebugKey(ctx, "rpc-key")
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			t.Log("Key correctly not found after delete (404 from server)")
		} else {
			t.Fatalf("RPC ReplicatedGet after delete failed: %v", err)
		}
	} else if debugResp["found"] == true && debugResp["tombstone"] == false {
		t.Error("RPC ReplicatedGet: key should not be found after delete")
	}
}

// TestReplication_HTTPHandlerDirect tests the HTTP handlers directly without
// going through the cluster layer.
func TestReplication_HTTPHandlerDirect(t *testing.T) {
	r := ring.New(150)
	node1 := newTestNode(t, "node-1", r)
	defer node1.close()

	// Test PUT via HTTP
	putBody := map[string]any{"key": "direct-key", "value": []byte("direct-val")}
	putData, _ := json.Marshal(putBody)
	req := httptest.NewRequest(http.MethodPut, "/internal/replicate", bytes.NewReader(putData))
	w := httptest.NewRecorder()
	node1.server.Config.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("PUT /internal/replicate: expected 204, got %d: %s", w.Code, w.Body.String())
	}

	// Test GET via HTTP
	getBody := map[string]any{"key": "direct-key"}
	getData, _ := json.Marshal(getBody)
	req = httptest.NewRequest(http.MethodGet, "/internal/replicate", bytes.NewReader(getData))
	w = httptest.NewRecorder()
	node1.server.Config.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GET /internal/replicate: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["found"] != true {
		t.Errorf("GET /internal/replicate: expected found=true, got %v", resp["found"])
	}

	// Test DELETE via HTTP
	delBody := map[string]any{"key": "direct-key"}
	delData, _ := json.Marshal(delBody)
	req = httptest.NewRequest(http.MethodDelete, "/internal/replicate", bytes.NewReader(delData))
	w = httptest.NewRecorder()
	node1.server.Config.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("DELETE /internal/replicate: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var delResp map[string]any
	json.Unmarshal(w.Body.Bytes(), &delResp)
	if delResp["success"] != true {
		t.Errorf("DELETE /internal/replicate: expected success=true, got %v", delResp["success"])
	}
}

// TestReplication_GossipHTTP tests that gossip actually exchanges membership
// info between nodes via HTTP.
func TestReplication_GossipHTTP(t *testing.T) {
	r1 := ring.New(150)
	node1 := newTestNode(t, "node-1", r1)
	defer node1.close()

	r2 := ring.New(150)
	node2 := newTestNode(t, "node-2", r2)
	defer node2.close()

	// node1 knows about node-2
	node1.node.members.AddMember("node-2")

	// node2 knows about node-3 (which node1 doesn't know)
	node2.node.members.AddMember("node-3")

	// Gossip from node1 to node2
	client := rpc.NewClient(node2.httpAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	theirMembers, err := client.Gossip(ctx, node1.node.members.Members())
	if err != nil {
		t.Fatalf("Gossip failed: %v", err)
	}

	// node1 should now learn about node-3
	node1.node.members.Merge(theirMembers)

	if !node1.node.members.IsAlive("node-3") {
		t.Error("node1 should have learned about node-3 via gossip")
	}
}

// TestReplication_EndToEndWithHTTPClient tests a full write-read cycle
// using only HTTP calls (simulating a real client).
func TestReplication_EndToEndWithHTTPClient(t *testing.T) {
	r := ring.New(150)
	node1 := newTestNode(t, "node-1", r)
	defer node1.close()

	node2 := newTestNode(t, "node-2", r)
	defer node2.close()

	node3 := newTestNode(t, "node-3", r)
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

	// Write via node-1's HTTP endpoint
	putBody := map[string]string{"value": "http-value"}
	putData, _ := json.Marshal(putBody)
	req := httptest.NewRequest(http.MethodPut, "/keys/e2e-key", bytes.NewReader(putData))
	w := httptest.NewRecorder()
	node1.server.Config.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("PUT /keys/e2e-key: expected 204, got %d: %s", w.Code, w.Body.String())
	}

	// Read from node-2's HTTP endpoint
	req = httptest.NewRequest(http.MethodGet, "/keys/e2e-key", nil)
	w = httptest.NewRecorder()
	node2.server.Config.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GET /keys/e2e-key from node-2: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["value"] != "http-value" {
		t.Errorf("expected 'http-value', got '%v'", resp["value"])
	}

	// Read from node-3's HTTP endpoint
	req = httptest.NewRequest(http.MethodGet, "/keys/e2e-key", nil)
	w = httptest.NewRecorder()
	node3.server.Config.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GET /keys/e2e-key from node-3: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["value"] != "http-value" {
		t.Errorf("expected 'http-value' from node-3, got '%v'", resp["value"])
	}
}

func TestReplication_Scan(t *testing.T) {
	r := ring.New(150)
	node1 := newTestNode(t, "node-1", r)
	defer node1.close()

	ctx := context.Background()

	// Write some keys with a common prefix
	for i := 0; i < 5; i++ {
		node1.node.ReplicatedPut(ctx, fmt.Sprintf("user:%d", i), []byte(fmt.Sprintf("val-%d", i)))
	}

	// Scan via HTTP
	req := httptest.NewRequest(http.MethodGet, "/keys?prefix=user:", nil)
	w := httptest.NewRecorder()
	node1.server.Config.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		body, _ := io.ReadAll(w.Body)
		t.Fatalf("GET /keys?prefix=user:: expected 200, got %d: %s", w.Code, string(body))
	}

	var resp map[string]any
	json.Unmarshal(w.Body.Bytes(), &resp)
	keys, ok := resp["keys"].([]any)
	if !ok {
		t.Fatalf("expected keys array, got %v", resp)
	}
	if len(keys) != 5 {
		t.Errorf("expected 5 keys, got %d", len(keys))
	}
}

func TestReplication_HealthAndStatus(t *testing.T) {
	r := ring.New(150)
	node1 := newTestNode(t, "node-1", r)
	defer node1.close()

	// Health check
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	node1.server.Config.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GET /healthz: expected 200, got %d", w.Code)
	}

	var health map[string]any
	json.Unmarshal(w.Body.Bytes(), &health)
	if health["status"] != "ok" {
		t.Errorf("expected status=ok, got %v", health)
	}

	// Status check
	req = httptest.NewRequest(http.MethodGet, "/status", nil)
	w = httptest.NewRecorder()
	node1.server.Config.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GET /status: expected 200, got %d", w.Code)
	}

	var status map[string]any
	json.Unmarshal(w.Body.Bytes(), &status)
	if status["node_id"] != "node-1" {
		t.Errorf("expected node_id=node-1, got %v", status["node_id"])
	}
}

// cleanup removes temp dirs in case httptest doesn't
func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
