package handler

import (
	"fmt"
	"net/http"
	"os/exec"
	"strconv"
	"time"
)

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"status":  "healthy",
			"node_id": s.nodeID,
		},
	})
}

func (s *Server) handleLive(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    map[string]string{"status": "alive"},
	})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	stats := s.store.Stats()
	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: map[string]any{
			"status": "ready",
			"stats":  stats,
		},
	})
}

func (s *Server) handleBenchmark(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Parse benchmark parameters
	workers := 10
	durationSec := 5
	if wStr := r.URL.Query().Get("workers"); wStr != "" {
		if w, err := strconv.Atoi(wStr); err == nil && w > 0 && w <= 100 {
			workers = w
		}
	}
	if dStr := r.URL.Query().Get("duration"); dStr != "" {
		if d, err := strconv.Atoi(dStr); err == nil && d > 0 && d <= 30 {
			durationSec = d
		}
	}

	// Run quick benchmark - spawn workers that do actual operations
	type result struct {
		gets int64
		puts int64
	}

	resultCh := make(chan result, workers)
	barrier := make(chan struct{})

	// Spawn workers
	for i := 0; i < workers; i++ {
		go func() {
			<-barrier // Wait for signal to start
			var localGets, localPuts int64

			deadline := time.Now().Add(time.Duration(durationSec) * time.Second)
			for time.Now().Before(deadline) {
				// Generate keys for this worker
				key := fmt.Sprintf("bench_key_%d_%d", i, localPuts)
				value := []byte(fmt.Sprintf("value_%d", localPuts))

				// Write
				if err := s.store.Put(key, value); err == nil {
					localPuts++
				}

				// Read
				if _, err := s.store.Get(key); err == nil {
					localGets++
				}
			}
			resultCh <- result{gets: localGets, puts: localPuts}
		}()
	}

	// Start all workers simultaneously
	close(barrier)

	// Wait for completion
	var totalGets, totalPuts int64
	for i := 0; i < workers; i++ {
		res := <-resultCh
		totalGets += res.gets
		totalPuts += res.puts
	}

	// Calculate rates
	getRate := float64(totalGets) / float64(durationSec)
	putRate := float64(totalPuts) / float64(durationSec)

	// Detect mode
	mode := "single-node"
	nodeCount := 1
	replication := 1
	if s.cluster != nil && s.ring != nil {
		mode = "cluster"
		replication = ReplicationFactor
		if s.ring != nil {
			nodeCount = len(s.ring.GetAllNodes())
		}
	}

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"workers":        workers,
			"duration_sec":   durationSec,
			"writes":         totalPuts,
			"reads":          totalGets,
			"writes_per_sec": putRate,
			"reads_per_sec":  getRate,
			"mode":           mode,
			"replication":    replication,
			"nodes":          nodeCount,
			"note":           "Use /api/v1/benchmark/network for actual network benchmark.",
		},
	})
}

func (s *Server) handleNetworkBenchmark(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	workers := 60
	durationSec := 10
	if wStr := r.URL.Query().Get("workers"); wStr != "" {
		if w, err := strconv.Atoi(wStr); err == nil && w > 0 && w <= 200 {
			workers = w
		}
	}
	if dStr := r.URL.Query().Get("duration"); dStr != "" {
		if d, err := strconv.Atoi(dStr); err == nil && d > 0 && d <= 60 {
			durationSec = d
		}
	}

	// Detect cluster vs single
	nodes := []string{s.addr}
	replication := 1
	mode := "single"
	if s.cluster != nil && s.ring != nil {
		mode = "cluster"
		replication = ReplicationFactor
		if s.ring != nil {
			nodes = s.ring.GetAllNodes()
		}
	}

	// Build pressure-tool command
	nodeAddrs := ""
	for i, n := range nodes {
		if i > 0 {
			nodeAddrs += ","
		}
		nodeAddrs += n
	}

	// Run pressure-tool with proper flags
	args := []string{
		"-nodes=" + nodeAddrs,
		"-workers=" + strconv.Itoa(workers),
		"-write-duration=" + strconv.Itoa(durationSec) + "s",
		"-read-duration=" + strconv.Itoa(durationSec) + "s",
	}

	cmd := exec.Command("./pressure-tool", args...)
	output, err := cmd.CombinedOutput()

	var errMsg string
	var outputStr string
	if err != nil {
		errMsg = err.Error()
		outputStr = string(output)
	} else {
		outputStr = string(output)
	}

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: err == nil,
		Data: map[string]interface{}{
			"workers":         workers,
			"duration_sec":    durationSec,
			"mode":            mode,
			"nodes":           len(nodes),
			"replication":     replication,
			"pressure_output": outputStr,
			"error":           errMsg,
		},
	})
}

func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	prefix := r.URL.Query().Get("prefix")
	entries, err := s.store.Scan(prefix)
	if err != nil {
		s.logger.Error("scan error", "prefix", prefix, "error", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	keys := make([]string, len(entries))
	for i, e := range entries {
		keys[i] = e.Key
	}

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: ScanResponse{
			Keys: keys,
		},
	})
}

func (s *Server) handleKeys(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	keys, err := s.store.Keys()
	if err != nil {
		s.logger.Error("keys error", "error", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: ScanResponse{
			Keys: keys,
		},
	})
}

func (s *Server) handleNodeInfo(w http.ResponseWriter, r *http.Request) {
	stats := s.store.Stats()
	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"node_id": s.nodeID,
			"addr":    s.addr,
			"stats":   stats,
		},
	})
}

func (s *Server) handleRing(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var nodes []string
	if s.ring != nil {
		nodes = s.ring.GetAllNodes()
	} else {
		// Single-node mode: just this node
		nodes = []string{s.nodeID}
	}

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"nodes":       nodes,
			"count":       len(nodes),
			"replication": ReplicationFactor,
		},
	})
}

func (s *Server) handleFlush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Try to flush the storage engine
	var flushErr string
	if flusher, ok := s.store.(interface{ Flush() error }); ok {
		if err := flusher.Flush(); err != nil {
			flushErr = err.Error()
		}
	} else {
		flushErr = "storage engine does not support flush"
	}

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: flushErr == "",
		Data: map[string]interface{}{
			"flushed": flushErr == "",
			"error":   flushErr,
		},
	})
}
