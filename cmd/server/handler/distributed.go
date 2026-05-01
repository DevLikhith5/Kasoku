package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"

	"github.com/DevLikhith5/kasoku/internal/rpc"
	storage "github.com/DevLikhith5/kasoku/internal/store"
)

type NodeInfo struct {
	NodeID string           `json:"node_id"`
	Addr   string           `json:"addr"`
	Stats  map[string]int64 `json:"stats,omitempty"`
	Alive  bool             `json:"alive"`
	Error  string           `json:"error,omitempty"`
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	key := extractKeyFromPath(r.URL.Path, "/api/v1/put/")
	if key == "" {
		s.writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	// If cluster mode is enabled, use coordinator pattern
	if s.cluster != nil && s.ring != nil && s.cluster.IsDistributed() {
		s.handleDistributedPut(w, r, key)
		return
	}

	// Single-node mode
	var body struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	value := []byte(body.Value)
	if len(value) == 0 {
		s.writeError(w, http.StatusBadRequest, "value is required")
		return
	}

	start := s.metrics.RecordPutStart()
	storeStart := time.Now()
	err := s.store.Put(key, value)
	storeEnd := time.Now()
	s.metrics.RecordPutEnd(start, err == nil)

	// Record timing to Prometheus (no logging overhead)
	s.metrics.RecordHandlerStage("put", "store", storeEnd.Sub(storeStart))

	if err != nil {
		s.logger.Error("put error", "key", key, "error", err)
		if isKeyTooLong(err) {
			s.writeError(w, http.StatusBadRequest, "key too long (max 1KB)")
		} else if isValueTooLarge(err) {
			s.writeError(w, http.StatusBadRequest, "value too large (max 1MB)")
		} else {
			s.writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    map[string]string{"status": "ok"},
	})
}

func (s *Server) handleDistributedPut(w http.ResponseWriter, r *http.Request, key string) {
	var body struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	value := []byte(body.Value)
	if len(value) == 0 {
		s.writeError(w, http.StatusBadRequest, "value is required")
		return
	}

	start := s.metrics.RecordPutStart()

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	replicationStart := time.Now()
	if err := s.cluster.ReplicatedPut(ctx, key, value); err != nil {
		replicationEnd := time.Now()
		s.metrics.RecordPutEnd(start, false)
		s.metrics.RecordHandlerStage("put", "replication", replicationEnd.Sub(replicationStart))
		s.logger.Error("quorum write failed", "key", key, "error", err)
		s.writeError(w, http.StatusServiceUnavailable, "quorum write failed: "+err.Error())
		return
	}

	replicationEnd := time.Now()
	s.metrics.RecordPutEnd(start, true)
	s.metrics.RecordHandlerStage("put", "replication", replicationEnd.Sub(replicationStart))

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    map[string]string{"status": "ok"},
	})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	key := extractKeyFromPath(r.URL.Path, "/api/v1/get/")
	if key == "" {
		s.writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	// If cluster mode is enabled, use coordinator pattern
	if s.cluster != nil && s.ring != nil && s.cluster.IsDistributed() {
		s.handleDistributedGet(w, r, key)
		return
	}

	// Single-node mode - measure latency breakdown
	requestStart := time.Now()

	// Step 1: Call store.Get
	storeStart := time.Now()
	entry, err := s.store.Get(key)
	storeLatency := time.Since(storeStart)

	// Step 2: Record metrics
	s.metrics.RecordGetEnd(requestStart, err == nil)

	if err != nil {
		if isKeyNotFound(err) {
			s.writeError(w, http.StatusNotFound, "key not found")
		} else {
			s.logger.Error("get error", "key", key, "error", err)
			s.writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}

	// Log slow reads for debugging
	totalLatency := time.Since(requestStart)
	if totalLatency > 10*time.Millisecond {
		s.logger.Warn("slow GET detected",
			"key", key,
			"total_ms", totalLatency.Milliseconds(),
			"store_ms", storeLatency.Milliseconds(),
		)
	}

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: GetResponse{
			Key:       entry.Key,
			Value:     entry.Value,
			Version:   entry.Version,
			Timestamp: entry.TimeStamp.UnixNano(),
		},
	})
}

func (s *Server) handleDistributedGet(w http.ResponseWriter, r *http.Request, key string) {
	totalStart := time.Now()

	start := s.metrics.RecordGetStart()

	storeStart := time.Now()
	entry, err := s.store.Get(key)
	storeEnd := time.Now()
	s.metrics.RecordHandlerStage("get", "store", storeEnd.Sub(storeStart))

	if err != nil {
		fmt.Printf("DEBUG: get err for key=%s: err=%v isKeyNotFound=%v\n", key, err, isKeyNotFound(err))
		s.metrics.RecordGetEnd(start, false)
		if isKeyNotFound(err) {
			// Key not found locally - try remote replicas
			remoteEntry, remoteErr := s.fetchFromReplicas(key)
			if remoteErr == nil && remoteEntry.Value != nil {
				// Found on remote - write to local and return
				_ = s.store.Put(key, remoteEntry.Value)
				s.metrics.RecordGetEnd(start, true)
				s.writeJSON(w, http.StatusOK, APIResponse{
					Success: true,
					Data: GetResponse{
						Key:       remoteEntry.Key,
						Value:     remoteEntry.Value,
						Version:   remoteEntry.Version,
						Timestamp: remoteEntry.TimeStamp.UnixNano(),
					},
				})
				go s.asyncReadRepair(key)
				return
			}
			// Not found anywhere - read repair in background
			go s.asyncReadRepair(key)
			s.writeError(w, http.StatusNotFound, "key not found")
		} else {
			s.logger.Error("get error", "key", key, "error", err,
				"timing.store_ms", storeEnd.Sub(storeStart).Milliseconds())
			s.writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}

	// Read repair: check other replicas for newer version
	// Dynamo-style: repair in background, don't block read
	go s.asyncReadRepairWithEntry(key, entry)

	totalLatency := time.Since(totalStart)
	if totalLatency > 10*time.Millisecond {
		s.logger.Warn("slow GET detected",
			"key", key,
			"total_ms", totalLatency.Milliseconds(),
			"store_ms", storeEnd.Sub(storeStart).Milliseconds(),
		)
	}

	s.metrics.RecordGetEnd(start, true)
	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: GetResponse{
			Key:       entry.Key,
			Value:     entry.Value,
			Version:   entry.Version,
			Timestamp: entry.TimeStamp.UnixNano(),
		},
	})
}

// fetchFromReplicas tries to get the key from other replicas in the replica set
func (s *Server) fetchFromReplicas(key string) (storage.Entry, error) {
	replicas := s.ring.GetNodes(key, 3)
	s.logger.Debug("fetchFromReplicas", "key", key, "replicas", replicas, "nodeID", s.nodeID)
	for _, replica := range replicas {
		if replica == s.nodeID || replica == s.addr {
			continue
		}
		s.logger.Debug("trying replica", "replica", replica)
		// getClient handles both nodeID and addr
		client, ok := s.cluster.GetClient(replica)
		if !ok {
			s.logger.Debug("no client for replica", "replica", replica)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		remoteValue, found, err := client.ReplicatedGet(ctx, key)
		if err != nil || !found {
			s.logger.Debug("replica get failed", "replica", replica, "err", err, "found", found)
			continue
		}
		return storage.Entry{
			Key:       key,
			Value:     remoteValue,
			TimeStamp: time.Now(),
		}, nil
	}
	return storage.Entry{}, storage.ErrKeyNotFound
}

// asyncReadRepair checks other replicas for key and repairs if needed
func (s *Server) asyncReadRepair(key string) {
	replicas := s.ring.GetNodes(key, 3)
	if len(replicas) < 2 {
		return
	}

	localVersion := uint64(0)
	entry, err := s.store.Get(key)
	if err == nil {
		localVersion = entry.Version
	}

	// Check other replicas
	for _, replica := range replicas {
		if replica == s.nodeID || replica == s.addr {
			continue
		}

		client, ok := s.cluster.GetClient(replica)
		if !ok {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		remoteValue, found, err := client.ReplicatedGet(ctx, key)
		if err != nil || !found {
			continue
		}

		// If remote has data and local doesn't, pull it
		if len(remoteValue) > 0 && localVersion == 0 {
			s.store.Put(key, remoteValue)
			return
		}
	}
}

// asyncReadRepairWithEntry uses local entry to repair stale replicas
func (s *Server) asyncReadRepairWithEntry(key string, localEntry storage.Entry) {
	replicas := s.ring.GetNodes(key, 3)
	if len(replicas) < 2 {
		return
	}

	for _, replica := range replicas {
		if replica == s.nodeID || replica == s.addr {
			continue
		}

		client, ok := s.cluster.GetClient(replica)
		if !ok {
			// Hint handoff: store locally for later delivery
			s.cluster.AddHint(replica, key, localEntry.Value)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		remoteValue, found, err := client.ReplicatedGet(ctx, key)
		if err != nil {
			s.cluster.AddHint(replica, key, localEntry.Value)
			continue
		}

		// If remote has older or no data, push our version
		if !found || len(remoteValue) == 0 {
			client.ReplicatedPut(ctx, key, localEntry.Value)
		}
	}
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	key := extractKeyFromPath(r.URL.Path, "/api/v1/delete/")
	if key == "" {
		s.writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	// If cluster mode is enabled, use coordinator pattern
	if s.cluster != nil && s.ring != nil && s.cluster.IsDistributed() {
		s.handleDistributedDelete(w, r, key)
		return
	}

	// Single-node mode
	start := s.metrics.RecordDeleteStart()
	err := s.store.Delete(key)
	s.metrics.RecordDeleteEnd(start, err == nil)

	if err != nil {
		if isKeyNotFound(err) {
			s.writeError(w, http.StatusNotFound, "key not found")
		} else {
			s.logger.Error("delete error", "key", key, "error", err)
			s.writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    map[string]string{"status": "deleted"},
	})
}

func (s *Server) handleDistributedDelete(w http.ResponseWriter, r *http.Request, key string) {
	// Find the replica set for this key
	replicas := s.ring.GetNodes(key, ReplicationFactor)
	if len(replicas) == 0 {
		s.writeError(w, http.StatusInternalServerError, "no replicas available")
		return
	}

	isPrimary := replicas[0] == s.nodeID || replicas[0] == s.addr

	if !isPrimary {
		// This node is not the coordinator — forward the request to the primary
		// Record metrics here so dashboard shows DELETE rate
		start := s.metrics.RecordDeleteStart()
		s.metrics.RecordDeleteEnd(start, true)
		s.proxyRequest(w, r, replicas[0])
		return
	}

	// This node IS the coordinator — handle the delete with replication
	start := s.metrics.RecordDeleteStart()

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if err := s.cluster.ReplicatedDelete(ctx, key); err != nil {
		s.metrics.RecordDeleteEnd(start, false)
		if isKeyNotFound(err) || err.Error() == "key not found" {
			s.writeError(w, http.StatusNotFound, "key not found")
			return
		}
		s.logger.Error("quorum delete failed", "key", key, "error", err)
		s.writeError(w, http.StatusServiceUnavailable, "quorum delete failed: "+err.Error())
		return
	}

	s.metrics.RecordDeleteEnd(start, true)
	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    map[string]string{"status": "deleted"},
	})
}

func (s *Server) proxyRequest(w http.ResponseWriter, r *http.Request, targetNodeID string) {
	targetAddr := s.getNodeAddress(targetNodeID)
	if targetAddr == "" {
		s.writeError(w, http.StatusInternalServerError, "cannot find target node: "+targetNodeID)
		return
	}

	targetURL, err := url.Parse(targetAddr)
	if err != nil {
		s.logger.Error("invalid target URL", "target", targetAddr, "error", err)
		s.writeError(w, http.StatusInternalServerError, "invalid target node address")
		return
	}

	proxy := s.getReverseProxy(targetURL)
	s.logger.Debug("proxying request", "from", s.nodeID, "to", targetNodeID, "url", r.URL.Path)
	proxy.ServeHTTP(w, r)
}

var proxyCache sync.Map

func (s *Server) getReverseProxy(target *url.URL) *httputil.ReverseProxy {
	if cached, ok := proxyCache.Load(target.String()); ok {
		return cached.(*httputil.ReverseProxy)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		s.logger.Error("proxy error", "target", target.String(), "error", err)
		s.writeError(w, http.StatusBadGateway, "failed to reach target node")
	}
	actual, _ := proxyCache.LoadOrStore(target.String(), proxy)
	return actual.(*httputil.ReverseProxy)
}

func (s *Server) getNodeAddress(nodeID string) string {
	// In a simple implementation, assume nodeID is the address
	// In production, you'd have a proper node registry
	if nodeID == s.nodeID {
		return fmt.Sprintf("http://%s", s.addr)
	}

	// Try to get from cluster's peer list
	if s.cluster != nil {
		// For now, assume nodeID format is "http://host:port"
		return nodeID
	}

	return ""
}

func (s *Server) handleInternalReplicatePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req rpc.ReplicatedWriteRequest
	if err := decodeInternal(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "decode error: "+err.Error())
		return
	}

	if req.Key == "" {
		s.writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	start := s.metrics.RecordPutStart()

	var err error
	if req.VectorClock != nil {
		vc := storage.VectorClock(req.VectorClock)
		err = s.store.PutWithVectorClock(req.Key, req.Value, vc)
	} else {
		err = s.store.Put(req.Key, req.Value)
	}
	s.metrics.RecordPutEnd(start, err == nil)

	if err != nil {
		s.logger.Error("replicated put error", "key", req.Key, "error", err)
		s.writeError(w, http.StatusInternalServerError, "replication failed: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, rpc.ReplicatedWriteResponse{
		Success: true,
	})
}

func (s *Server) handleInternalReplicateGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req rpc.ReplicatedReadRequest
	if err := decodeInternal(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "decode error: "+err.Error())
		return
	}

	if req.Key == "" {
		s.writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	start := s.metrics.RecordGetStart()
	entry, err := s.store.Get(req.Key)
	s.metrics.RecordGetEnd(start, err == nil)

	if err != nil {
		if isKeyNotFound(err) {
			s.writeJSON(w, http.StatusOK, rpc.ReplicatedReadResponse{
				Found: false,
			})
			return
		}
		s.logger.Error("replicated get error", "key", req.Key, "error", err)
		s.writeError(w, http.StatusInternalServerError, "replication failed: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, rpc.ReplicatedReadResponse{
		Found: true,
		Value: entry.Value,
	})
}

func (s *Server) handleInternalReplicateDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req rpc.ReplicatedDeleteRequest
	if err := decodeInternal(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "decode error: "+err.Error())
		return
	}

	if req.Key == "" {
		// Try to extract from URL path for DELETE requests
		req.Key = extractKeyFromPath(r.URL.Path, "/internal/replicate/delete/")
	}

	if req.Key == "" {
		s.writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	start := s.metrics.RecordDeleteStart()
	err := s.store.Delete(req.Key)
	s.metrics.RecordDeleteEnd(start, err == nil)

	if err != nil {
		if isKeyNotFound(err) {
			s.writeJSON(w, http.StatusOK, rpc.ReplicatedDeleteResponse{
				Success: true,
				Deleted: false,
			})
			return
		}
		s.logger.Error("replicated delete error", "key", req.Key, "error", err)
		s.writeError(w, http.StatusInternalServerError, "replication failed: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, rpc.ReplicatedDeleteResponse{
		Success: true,
		Deleted: true,
	})
}

// handleInternalReplicate dispatches internal replication requests by HTTP method.
// Used by rpc.Client for inter-node replication.
func (s *Server) handleInternalReplicate(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		s.handleInternalReplicatePut(w, r)
	case http.MethodGet:
		s.handleInternalReplicateGet(w, r)
	case http.MethodDelete:
		s.handleInternalReplicateDelete(w, r)
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if s.cluster == nil || s.ring == nil {
		s.writeError(w, http.StatusBadRequest, "cluster mode not enabled")
		return
	}

	ringNodes := s.ring.GetAllNodes()
	
	// Get internal storage stats if it's an LSMEngine
	var internalStats map[string]interface{}
	if lsm, ok := s.store.(interface{ InternalStats() map[string]interface{} }); ok {
		internalStats = lsm.InternalStats()
	}

	// Basic store stats
	stats := s.store.Stats()
	
	localInfo := NodeInfo{
		NodeID: s.nodeID,
		Addr:   s.addr,
		Alive:  true,
		Stats: map[string]int64{
			"key_count":  stats.KeyCount,
			"disk_bytes": stats.DiskBytes,
			"mem_bytes":  stats.MemBytes,
		},
	}

	// Return comprehensive cluster status
	status := map[string]interface{}{
		"node_id":            s.nodeID,
		"node_addr":          s.addr,
		"replication_factor": ReplicationFactor,
		"ring_distribution":  s.ring.Distribution(),
		"active_nodes":       len(ringNodes),
		"all_nodes":          ringNodes,
		"local_node":         localInfo,
		"storage_internal":   internalStats,
		"timestamp":          time.Now().Format(time.RFC3339),
	}

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    status,
	})
}

func (s *Server) handleAddPeer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if s.cluster == nil {
		s.writeError(w, http.StatusBadRequest, "cluster mode not enabled")
		return
	}

	var req struct {
		PeerAddr string `json:"peer_addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.PeerAddr == "" {
		s.writeError(w, http.StatusBadRequest, "peer_addr is required")
		return
	}

	s.cluster.AddPeer(req.PeerAddr, req.PeerAddr)

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    map[string]string{"status": "peer added"},
	})
}

func (s *Server) handleRemovePeer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if s.cluster == nil {
		s.writeError(w, http.StatusBadRequest, "cluster mode not enabled")
		return
	}

	var req struct {
		PeerAddr string `json:"peer_addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.PeerAddr == "" {
		s.writeError(w, http.StatusBadRequest, "peer_addr is required")
		return
	}

	s.cluster.RemovePeer(req.PeerAddr, req.PeerAddr)

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    map[string]string{"status": "peer removed"},
	})
}
