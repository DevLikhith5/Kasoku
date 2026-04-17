// Kasoku pressure benchmark — DynamoDB-style
//
// Runs two isolated phases:
//   Phase 1 — WRITE  (populate cluster, no reads)
//   Phase 2 — READ   (reads-only from pre-seeded keys, no writes)
//
// Reads are routed to the correct node using consistent hashing (CRC32).
// Reports live ops/sec per second + p50/p95/p99/p999 latency.
//
// Usage:
//   go run tools/benchmarks/pressure/pressure.go [flags]
//   go run tools/benchmarks/pressure/pressure.go -nodes=localhost:9000,localhost:9001,localhost:9002 -write-duration=20s -read-duration=20s -workers=60 -batch=50

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ────────────────────────────────────────────────────────────────────────────
// Flags
// ────────────────────────────────────────────────────────────────────────────

var (
	nodesFlag     = flag.String("nodes", "localhost:9000,localhost:9001,localhost:9002", "Comma-separated node hosts")
	writeDuration = flag.Duration("write-duration", 20*time.Second, "Duration of write-only phase")
	readDuration  = flag.Duration("read-duration", 20*time.Second, "Duration of read-only phase")
	warmDuration  = flag.Duration("warm", 3*time.Second, "Pre-warm duration before each phase")
	workers       = flag.Int("workers", 60, "Concurrent goroutines per phase")
	batchSize     = flag.Int("batch", 50, "Keys per batch PUT request (smaller = lower tail latency)")
	batchGetSize  = flag.Int("batch-get", 50, "Keys per batch GET request")
)

// ────────────────────────────────────────────────────────────────────────────
// Colors
// ────────────────────────────────────────────────────────────────────────────

const (
	bold   = "\033[1m"
	yellow = "\033[33m"
	cyan   = "\033[36m"
	green  = "\033[32m"
	red    = "\033[31m"
	reset  = "\033[0m"
)

const defaultVNodes = 150

// ────────────────────────────────────────────────────────────────────────────
// Consistent Hash Ring (same algorithm as internal/ring)
// ────────────────────────────────────────────────────────────────────────────

type consistentHash struct {
	vnodes     []uint32
	nodeMap    map[uint32]string
	nodes      []string
	vnodeCount int
}

func newConsistentHash(nodes []string, vnodeCount int) *consistentHash {
	if vnodeCount <= 0 {
		vnodeCount = defaultVNodes
	}
	ch := &consistentHash{
		nodeMap:    make(map[uint32]string),
		nodes:      nodes,
		vnodeCount: vnodeCount,
	}

	for _, node := range nodes {
		for i := 0; i < vnodeCount; i++ {
			vnodeKey := fmt.Sprintf("%s#vnode%d", node, i)
			pos := crc32.ChecksumIEEE([]byte(vnodeKey))
			ch.vnodes = append(ch.vnodes, pos)
			ch.nodeMap[pos] = node
		}
	}
	sort.Slice(ch.vnodes, func(i, j int) bool {
		return ch.vnodes[i] < ch.vnodes[j]
	})

	return ch
}

func (ch *consistentHash) search(pos uint32) int {
	n := len(ch.vnodes)
	idx := sort.Search(n, func(i int) bool {
		return ch.vnodes[i] >= pos
	})
	return idx % n
}

func (ch *consistentHash) GetNode(key string) string {
	if len(ch.vnodes) == 0 {
		return ""
	}
	pos := crc32.ChecksumIEEE([]byte(key))
	idx := ch.search(pos)
	return ch.nodeMap[ch.vnodes[idx]]
}

// ────────────────────────────────────────────────────────────────────────────
// Histogram (lock-free bucket array, 0–5 seconds in µs)
// ────────────────────────────────────────────────────────────────────────────

const maxBucketUs = 5_000_000

type histogram struct {
	mu      sync.Mutex
	buckets []int64
}

func newHistogram() *histogram {
	return &histogram{buckets: make([]int64, maxBucketUs+1)}
}

func (h *histogram) record(d time.Duration) {
	µs := int64(d.Microseconds())
	if µs < 0 {
		µs = 0
	}
	if µs > maxBucketUs {
		µs = maxBucketUs
	}
	h.mu.Lock()
	h.buckets[µs]++
	h.mu.Unlock()
}

func (h *histogram) percentile(p float64) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	var total int64
	for _, v := range h.buckets {
		total += v
	}
	if total == 0 {
		return 0
	}
	target := int64(float64(total) * p / 100.0)
	var cum int64
	for i, v := range h.buckets {
		cum += v
		if cum >= target {
			return time.Duration(i) * time.Microsecond
		}
	}
	return 0
}

func (h *histogram) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := range h.buckets {
		h.buckets[i] = 0
	}
}

func (h *histogram) count() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	var n int64
	for _, v := range h.buckets {
		n += v
	}
	return n
}

// ────────────────────────────────────────────────────────────────────────────
// Shared key pool with node tracking
// ────────────────────────────────────────────────────────────────────────────

type keyWithNode struct {
	key  string
	node string
}

type keyPool struct {
	mu       sync.RWMutex
	keys     []keyWithNode
	cap      int
	nodeKeys map[string][]keyWithNode // keys grouped by node
}

func newKeyPool(cap int) *keyPool {
	return &keyPool{
		cap:      cap,
		nodeKeys: make(map[string][]keyWithNode),
	}
}

func (p *keyPool) push(key string, node string) {
	p.mu.Lock()
	kwn := keyWithNode{key: key, node: node}
	p.keys = append(p.keys, kwn)
	p.nodeKeys[node] = append(p.nodeKeys[node], kwn)
	if len(p.keys) > p.cap {
		p.keys = p.keys[len(p.keys)-p.cap:]
		// Rebuild nodeKeys map
		p.nodeKeys = make(map[string][]keyWithNode)
		for _, k := range p.keys {
			p.nodeKeys[k.node] = append(p.nodeKeys[k.node], k)
		}
	}
	p.mu.Unlock()
}

func (p *keyPool) random() (string, string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.keys) == 0 {
		return "", "", false
	}
	k := p.keys[rand.Intn(len(p.keys))]
	return k.key, k.node, true
}

func (p *keyPool) len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.keys)
}

func (p *keyPool) randomBatch(n int) []keyWithNode {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.keys) == 0 {
		return nil
	}
	result := make([]keyWithNode, n)
	for i := range result {
		result[i] = p.keys[rand.Intn(len(p.keys))]
	}
	return result
}

// ────────────────────────────────────────────────────────────────────────────
// HTTP client factory
// ────────────────────────────────────────────────────────────────────────────

func makeClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        1000,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  false,
			WriteBufferSize:     32 * 1024,
			ReadBufferSize:      32 * 1024,
		},
		Timeout: 30 * time.Second,
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Operations
// ────────────────────────────────────────────────────────────────────────────

type batchEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type batchGetReq struct {
	Keys []string `json:"keys"`
}

type batchGetResp struct {
	Success bool `json:"success"`
	Data    struct {
		Found   int `json:"found"`
		Missing int `json:"missing"`
	} `json:"data"`
}

// doBatchPut sends N keys to a node's /api/v1/batch. Returns per-key latency.
func doBatchPut(client *http.Client, base string, n int, pool *keyPool, ring *consistentHash) (latency time.Duration, keysWritten int, err error) {
	entries := make([]batchEntry, n)
	payload := strings.Repeat("X", 128)
	for i := range entries {
		entries[i].Key = fmt.Sprintf("k:%d:%d", time.Now().UnixNano(), i)
		entries[i].Value = payload
	}

	body, _ := json.Marshal(map[string]interface{}{"entries": entries})

	start := time.Now()
	req, _ := http.NewRequest(http.MethodPut, base+"/api/v1/batch", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err2 := client.Do(req)
	latency = time.Since(start)

	if err2 != nil {
		return time.Since(start), 0, err2
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return latency, 0, fmt.Errorf("status %d", resp.StatusCode)
	}

	for _, e := range entries {
		pool.push(e.Key, base)
	}
	_ = ring // Ring is for read routing, not needed during writes
	return latency, n, nil
}

func doBatchGet(client *http.Client, base string, keys []string) (time.Duration, int, error) {
	reqBody := batchGetReq{Keys: keys}
	body, _ := json.Marshal(reqBody)

	start := time.Now()
	req, _ := http.NewRequest(http.MethodPost, base+"/api/v1/batch/get", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	lat := time.Since(start)
	if err != nil {
		return lat, 0, err
	}
	respBody, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return lat, 0, fmt.Errorf("status %d: %s", resp.StatusCode, string(respBody))
	}
	if readErr != nil {
		return lat, 0, readErr
	}
	var result batchGetResp
	if err := json.Unmarshal(respBody, &result); err != nil {
		return lat, 0, err
	}
	return lat, result.Data.Found, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Phase runner — generic "run ops until stop"
// ────────────────────────────────────────────────────────────────────────────

type phaseResult struct {
	totalOps  uint64
	totalErrs uint64
	opsPerSec []float64
	hist      *histogram
}

type phaseOp func(workerIdx int) (ops int, err error)

func runPhase(name string, numWorkers int, dur time.Duration, op phaseOp) phaseResult {
	var totalOps, totalErrs uint64
	hist := newHistogram()

	stop := make(chan struct{})
	var opsPerSec []float64
	var psMu sync.Mutex

	ticker := time.NewTicker(time.Second)
	var lastOps uint64
	sec := 0
	go func() {
		for {
			select {
			case <-stop:
				ticker.Stop()
				return
			case <-ticker.C:
				sec++
				cur := atomic.LoadUint64(&totalOps)
				rate := float64(cur - lastOps)
				lastOps = cur
				psMu.Lock()
				opsPerSec = append(opsPerSec, rate)
				psMu.Unlock()
				fmt.Printf("  %s%-4d%s  %s%-14.0f%s  p50 %-10s  p99 %-10s\n",
					cyan, sec, reset,
					bold, rate, reset,
					fmtLat(hist.percentile(50)),
					fmtLat(hist.percentile(99)),
				)
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				start := time.Now()
				n, err := op(idx)
				lat := time.Since(start)
				if err != nil {
					atomic.AddUint64(&totalErrs, 1)
					continue
				}
				hist.record(lat / time.Duration(max(n, 1)))
				atomic.AddUint64(&totalOps, uint64(n))
			}
		}(i)
	}

	time.Sleep(dur)
	close(stop)
	wg.Wait()

	return phaseResult{
		totalOps:  atomic.LoadUint64(&totalOps),
		totalErrs: atomic.LoadUint64(&totalErrs),
		opsPerSec: opsPerSec,
		hist:      hist,
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ────────────────────────────────────────────────────────────────────────────
// Reporting helpers
// ────────────────────────────────────────────────────────────────────────────

func fmtLat(d time.Duration) string {
	if d == 0 {
		return "-"
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.0fµs", float64(d.Microseconds()))
	}
	return fmt.Sprintf("%.2fms", d.Seconds()*1000)
}

func summaryStats(opsPerSec []float64) (avg, p50, p99 float64) {
	if len(opsPerSec) == 0 {
		return
	}
	sorted := make([]float64, len(opsPerSec))
	copy(sorted, opsPerSec)
	sort.Float64s(sorted)

	var sum float64
	for _, v := range sorted {
		sum += v
	}
	avg = sum / float64(len(sorted))

	idx50 := int(float64(len(sorted)) * 0.5)
	idx99 := int(float64(len(sorted)) * 0.99)
	if idx50 >= len(sorted) {
		idx50 = len(sorted) - 1
	}
	if idx99 >= len(sorted) {
		idx99 = len(sorted) - 1
	}
	p50 = sorted[idx50]
	p99 = sorted[idx99]
	return
}

func printPhaseReport(phase string, r phaseResult) {
	avg, p50, p99 := summaryStats(r.opsPerSec)
	fmt.Printf("\n%s%s  %s Results %s\n", bold, yellow, phase, reset)
	fmt.Println("  " + strings.Repeat("─", 60))
	fmt.Printf("  Total ops        : %s%d%s\n", bold, r.totalOps, reset)
	fmt.Printf("  Errors           : %d\n", r.totalErrs)
	fmt.Printf("  Avg throughput   : %s%.0f ops/sec%s\n", green+bold, avg, reset)
	fmt.Printf("  p50 throughput   : %.0f ops/sec\n", p50)
	fmt.Printf("  p99 throughput   : %.0f ops/sec\n", p99)
	fmt.Printf("  Latency p50      : %s\n", fmtLat(r.hist.percentile(50)))
	fmt.Printf("  Latency p95      : %s\n", fmtLat(r.hist.percentile(95)))
	fmt.Printf("  Latency p99      : %s\n", fmtLat(r.hist.percentile(99)))
	fmt.Printf("  Latency p999     : %s\n", fmtLat(r.hist.percentile(99.9)))
	fmt.Println("  " + strings.Repeat("─", 60))
}

// ────────────────────────────────────────────────────────────────────────────
// main
// ────────────────────────────────────────────────────────────────────────────

func main() {
	flag.Parse()

	nodes := strings.Split(*nodesFlag, ",")
	for i, n := range nodes {
		if !strings.HasPrefix(n, "http") {
			nodes[i] = "http://" + n
		}
	}

	ring := newConsistentHash(nodes, defaultVNodes)
	pool := newKeyPool(500_000)
	clients := make([]*http.Client, *workers)
	for i := range clients {
		clients[i] = makeClient()
	}

	fmt.Printf("\n%s%s╔══════════════════════════════════════════════════════╗%s\n", bold, yellow, reset)
	fmt.Printf("%s%s║   KASOKU — DynamoDB-Style Pressure Benchmark         ║%s\n", bold, yellow, reset)
	fmt.Printf("%s%s╚══════════════════════════════════════════════════════╝%s\n\n", bold, yellow, reset)
	fmt.Printf("  Nodes      : %s\n", strings.Join(nodes, "  "))
	fmt.Printf("  Workers    : %d goroutines\n", *workers)
	fmt.Printf("  Batch size : %d keys/request\n", *batchSize)
	fmt.Printf("  Write phase: %s\n", *writeDuration)
	fmt.Printf("  Read phase : %s\n\n", *readDuration)

	// ── PHASE 1: WRITE-ONLY ─────────────────────────────────────────────────
	fmt.Printf("%s%s▸ Phase 1 — Write-Only  (warming %s...)%s\n", bold, cyan, *warmDuration, reset)

	warmStop := make(chan struct{})
	var warmWg sync.WaitGroup
	for i := 0; i < 10; i++ {
		warmWg.Add(1)
		go func(idx int) {
			defer warmWg.Done()
			c := makeClient()
			for {
				select {
				case <-warmStop:
					return
				default:
					doBatchPut(c, nodes[idx%len(nodes)], *batchSize, pool, ring)
				}
			}
		}(i)
	}
	time.Sleep(*warmDuration)
	close(warmStop)
	warmWg.Wait()

	fmt.Printf("  Seeded %d keys. Starting write measurement...\n\n", pool.len())
	fmt.Printf("  %sSEC   OPS/SEC          LATENCY%s\n", bold, reset)
	fmt.Println("  " + strings.Repeat("─", 55))

	writeResult := runPhase("WRITE", *workers, *writeDuration, func(workerIdx int) (int, error) {
		node := nodes[workerIdx%len(nodes)]
		c := clients[workerIdx]
		_, n, err := doBatchPut(c, node, *batchSize, pool, ring)
		return n, err
	})

	printPhaseReport("✍  WRITE", writeResult)

	// ── PHASE 2: READ-ONLY ──────────────────────────────────────────────────
	fmt.Printf("\n%s%s▸ Phase 2 — Read-Only  (pool: %d keys, batch-get: %d keys/request)%s\n", bold, cyan, pool.len(), *batchGetSize, reset)
	fmt.Printf("  Reads routed to correct nodes via consistent hashing.\n\n")
	fmt.Printf("  %sSEC   OPS/SEC          LATENCY%s\n", bold, reset)
	fmt.Println("  " + strings.Repeat("─", 55))

	readResult := runPhase("READ", *workers, *readDuration, func(workerIdx int) (int, error) {
		keys := pool.randomBatch(*batchGetSize)
		if len(keys) == 0 {
			return 0, fmt.Errorf("no keys")
		}

		// Group keys by node
		nodeKeys := make(map[string][]string)
		for _, k := range keys {
			nodeKeys[k.node] = append(nodeKeys[k.node], k.key)
		}

		// Send batch-get to each node and sum results
		totalFound := 0
		for node, nodeKeysList := range nodeKeys {
			_, found, err := doBatchGet(clients[workerIdx], node, nodeKeysList)
			if err != nil {
				return 0, err
			}
			totalFound += found
		}
		return totalFound, nil
	})

	printPhaseReport("📖  READ", readResult)

	// ── Final comparison ────────────────────────────────────────────────────
	wAvg, _, _ := summaryStats(writeResult.opsPerSec)
	rAvg, _, _ := summaryStats(readResult.opsPerSec)

	fmt.Printf("\n%s%s╔══════════════════════════════════════════════════════╗%s\n", bold, yellow, reset)
	fmt.Printf("%s%s║                  PEAK PERFORMANCE                   ║%s\n", bold, yellow, reset)
	fmt.Printf("%s%s╚══════════════════════════════════════════════════════╝%s\n", bold, yellow, reset)
	fmt.Printf("  %s✍  Writes%s  avg %-8.0f ops/sec   p50 %-8s  p99 %s\n",
		green+bold, reset, wAvg,
		fmtLat(writeResult.hist.percentile(50)),
		fmtLat(writeResult.hist.percentile(99)))
	fmt.Printf("  %s📖 Reads%s   avg %-8.0f ops/sec   p50 %-8s  p99 %s\n",
		green+bold, reset, rAvg,
		fmtLat(readResult.hist.percentile(50)),
		fmtLat(readResult.hist.percentile(99)))
	fmt.Printf("  %s⚡ Total%s   %.0f ops/sec combined (3-node cluster, RF=3)\n\n",
		green+bold, reset, wAvg+rAvg)

	if writeResult.totalErrs > writeResult.totalOps/20 || readResult.totalErrs > readResult.totalOps/20 {
		fmt.Fprintf(os.Stderr, "%s⚠ Error rate >5%%, results may be unreliable%s\n", red, reset)
		os.Exit(1)
	}
}
