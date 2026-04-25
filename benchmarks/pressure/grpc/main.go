// Kasoku gRPC Benchmark — Fixed with MultiGet reads and Cluster support
//
// Usage:
//   Single node:  go run benchmarks/pressure/grpc_pressure.go -nodes=localhost:9000
//   Cluster:      go run benchmarks/pressure/grpc_pressure.go -nodes=localhost:9000,localhost:9001,localhost:9002

package main

import (
	"context"
	"flag"
	"fmt"
	"hash/crc32"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DevLikhith5/kasoku/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	nodesFlag     = flag.String("nodes", "localhost:9000", "Comma-separated node hosts")
	writeDuration = flag.Duration("write-duration", 20*time.Second, "Duration of write phase")
	readDuration  = flag.Duration("read-duration", 20*time.Second, "Duration of read phase")
	warmDuration  = flag.Duration("warm", 3*time.Second, "Pre-warm duration")
	workers       = flag.Int("workers", 60, "Concurrent goroutines")
	batchSize     = flag.Int("batch", 50, "Keys per batch")
	batchGetSize  = flag.Int("batch-get", 50, "Keys per batch GET request")
	poolSize      = flag.Int("pool", 500000, "Key pool size")
	singleNode    = flag.Bool("single-node", false, "Route all traffic to first node only")
)

const (
	bold          = "\033[1m"
	yellow        = "\033[33m"
	cyan          = "\033[36m"
	green         = "\033[32m"
	red           = "\033[31m"
	reset         = "\033[0m"
	defaultVNodes = 150
	maxBucketUs   = 5_000_000
)

type consistentHash struct {
	nodes   []string
	vnodes  []uint32
	nodeMap map[uint32]string
}

func newConsistentHash(nodes []string, vnodeCount int) *consistentHash {
	if vnodeCount <= 0 {
		vnodeCount = defaultVNodes
	}
	ch := &consistentHash{
		nodes:   nodes,
		vnodes:  make([]uint32, 0, vnodeCount*len(nodes)),
		nodeMap: make(map[uint32]string),
	}
	for _, node := range nodes {
		for i := 0; i < vnodeCount; i++ {
			pos := crc32.ChecksumIEEE([]byte(fmt.Sprintf("%s#vnode%d", node, i)))
			ch.vnodes = append(ch.vnodes, pos)
			ch.nodeMap[pos] = node
		}
	}
	sort.Slice(ch.vnodes, func(i, j int) bool { return ch.vnodes[i] < ch.vnodes[j] })
	return ch
}

func (ch *consistentHash) getNode(key string) string {
	if len(ch.vnodes) == 0 {
		return ""
	}
	pos := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(ch.vnodes), func(i int) bool { return ch.vnodes[i] >= pos })
	return ch.nodeMap[ch.vnodes[idx%len(ch.vnodes)]]
}

type histogram struct {
	mu      sync.Mutex
	buckets []int64
}

func newHistogram() *histogram {
	return &histogram{buckets: make([]int64, maxBucketUs+1)}
}

func (h *histogram) record(d time.Duration) {
	us := int64(d.Microseconds())
	if us < 0 {
		us = 0
	} else if us > maxBucketUs {
		us = maxBucketUs
	}
	h.mu.Lock()
	h.buckets[us]++
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
	target := int64(float64(total) * p / 100)
	var cum int64
	for i, v := range h.buckets {
		cum += v
		if cum >= target {
			return time.Duration(i) * time.Microsecond
		}
	}
	return 0
}

type keyWithNode struct {
	key  string
	node string
}

type keyPool struct {
	mu   sync.RWMutex
	keys []keyWithNode
	cap  int
}

func newKeyPool(cap int) *keyPool {
	return &keyPool{cap: cap, keys: make([]keyWithNode, 0, cap)}
}

func (p *keyPool) push(key, node string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.keys) < p.cap {
		p.keys = append(p.keys, keyWithNode{key, node})
	} else {
		idx := rand.Intn(p.cap)
		p.keys[idx] = keyWithNode{key, node}
	}
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

type grpcClient struct {
	conn   *grpc.ClientConn
	client api.KasokuServiceClient
	addr   string
}

func newGrpcClient(addr string) (*grpcClient, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithInitialWindowSize(1<<30),
		grpc.WithInitialConnWindowSize(1<<30),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(1024*1024*1024),
			grpc.MaxCallRecvMsgSize(1024*1024*1024),
		),
	)
	if err != nil {
		return nil, err
	}
	return &grpcClient{conn: conn, client: api.NewKasokuServiceClient(conn), addr: addr}, nil
}

func (c *grpcClient) Close() error {
	return c.conn.Close()
}

type phaseResult struct {
	totalOps  int64
	totalErrs int64
	opsPerSec []float64
	hist      *histogram
}

func runWritePhase(workers int, duration time.Duration, hist *histogram, pool *keyPool, clients []*grpcClient, nodes []string, ring *consistentHash, singleNode bool) phaseResult {
	result := phaseResult{
		opsPerSec: make([]float64, 0, int(duration.Seconds())+2),
		hist:      hist,
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	doneTimer := time.After(duration)

	numClients := len(clients)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()
			client := clients[workerIdx%numClients]
			payload := []byte(strings.Repeat("X", 128))

			for {
				select {
				case <-ctx.Done():
					return
				default:
					entries := make([]*api.Entry, *batchSize)
					for j := 0; j < *batchSize; j++ {
						key := fmt.Sprintf("k:%d:%d", time.Now().UnixNano(), workerIdx*1000+j)
						entries[j] = &api.Entry{
							Key:   key,
							Value: payload,
						}
					}

					start := time.Now()
					_, err := client.client.BatchPut(ctx, &api.BatchPutRequest{Entries: entries})
					latency := time.Since(start)
					hist.record(latency / time.Duration(*batchSize))

					if err == nil {
						atomic.AddInt64(&result.totalOps, int64(*batchSize))
						node := nodes[workerIdx%len(nodes)]
						if singleNode {
							node = nodes[0]
						}
						for _, e := range entries {
							pool.push(e.Key, node)
						}
					} else {
						atomic.AddInt64(&result.totalErrs, 1)
					}
				}
			}
		}(i)
	}

	ticker := time.NewTicker(1 * time.Second)
	prevOps := int64(0)
	sec := 0

	for {
		select {
		case <-ticker.C:
			sec++
			currOps := atomic.LoadInt64(&result.totalOps)
			ops := float64(currOps - prevOps)
			result.opsPerSec = append(result.opsPerSec, ops)
			prevOps = currOps
			fmt.Printf("  %s%-4d%s  %s%-14.0f%s  p50 %-10s  p99 %-10s\n",
				cyan, sec, reset,
				bold, ops, reset,
				fmtLat(hist.percentile(50)),
				fmtLat(hist.percentile(99)),
			)
		case <-doneTimer:
			cancel()
			wg.Wait()
			ticker.Stop()
			return result
		}
	}
}

func runReadPhase(workers int, duration time.Duration, hist *histogram, pool *keyPool, clients []*grpcClient, nodes []string, ring *consistentHash, singleNode bool) phaseResult {
	result := phaseResult{
		opsPerSec: make([]float64, 0, int(duration.Seconds())+2),
		hist:      hist,
	}

	clientMap := make(map[string]*grpcClient)
	for _, c := range clients {
		clientMap[c.addr] = c
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	doneTimer := time.After(duration)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				default:
					keys := pool.randomBatch(*batchGetSize)
					if len(keys) == 0 {
						time.Sleep(time.Millisecond)
						continue
					}

					// Group keys by node using consistent hash
					nodeKeys := make(map[string][]string)
					for _, k := range keys {
						var node string
						if singleNode {
							node = nodes[0]
						} else {
							node = ring.getNode(k.key)
						}
						nodeKeys[node] = append(nodeKeys[node], k.key)
					}

					// Send MultiGet to each node
					totalFound := int64(0)
					start := time.Now()

					for node, keyList := range nodeKeys {
						client, ok := clientMap[node]
						if !ok {
							continue
						}

						resp, err := client.client.MultiGet(ctx, &api.MultiGetRequest{Keys: keyList})
						if err == nil {
							totalFound += int64(len(resp.Entries))
						}
					}

					latency := time.Since(start)
					hist.record(latency / time.Duration(len(keys)))

					if totalFound > 0 {
						atomic.AddInt64(&result.totalOps, totalFound)
					} else {
						atomic.AddInt64(&result.totalErrs, int64(len(keys)))
					}
				}
			}
		}(i)
	}

	ticker := time.NewTicker(1 * time.Second)
	prevOps := int64(0)
	sec := 0

	for {
		select {
		case <-ticker.C:
			sec++
			currOps := atomic.LoadInt64(&result.totalOps)
			ops := float64(currOps - prevOps)
			result.opsPerSec = append(result.opsPerSec, ops)
			prevOps = currOps
			fmt.Printf("  %s%-4d%s  %s%-14.0f%s  p50 %-10s  p99 %-10s\n",
				cyan, sec, reset,
				bold, ops, reset,
				fmtLat(hist.percentile(50)),
				fmtLat(hist.percentile(99)),
			)
		case <-doneTimer:
			cancel()
			wg.Wait()
			ticker.Stop()
			return result
		}
	}
}

func fmtLat(d time.Duration) string {
	us := d.Microseconds()
	if us < 1000 {
		return fmt.Sprintf("%dµs", us)
	}
	if us < 1000000 {
		return fmt.Sprintf("%.1fms", float64(us)/1000)
	}
	return fmt.Sprintf("%.1fs", float64(us)/1000000)
}

func summaryStats(values []float64) (avg, p50, p99 float64) {
	if len(values) == 0 {
		return
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	for _, v := range sorted {
		avg += v
	}
	avg /= float64(len(sorted))
	idx50 := len(sorted) / 2
	if idx50 >= len(sorted) {
		idx50 = len(sorted) - 1
	}
	idx99 := int(float64(len(sorted)) * 0.99)
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

func main() {
	flag.Parse()

	nodes := strings.Split(*nodesFlag, ",")
	for i, n := range nodes {
		if !strings.Contains(n, ":") {
			nodes[i] = "localhost:" + n
		}
	}

	ring := newConsistentHash(nodes, defaultVNodes)
	pool := newKeyPool(*poolSize)

	// Create client per worker per node for proper distribution
	clients := make([]*grpcClient, 0, *workers)
	clientNodes := make([]string, 0, *workers)

	for i := 0; i < *workers; i++ {
		node := nodes[i%len(nodes)]
		cl, err := newGrpcClient(node)
		if err != nil {
			fmt.Printf("[WARNING] Failed to connect to %s: %v\n", node, err)
			continue
		}
		clients = append(clients, cl)
		clientNodes = append(clientNodes, node)
	}

	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()

	if len(clients) == 0 {
		fmt.Printf("%s[ERROR]%s No clients connected\n", red, reset)
		return
	}

	fmt.Printf("  Connected to %d clients (%d workers, %d nodes)\n", len(clients), *workers, len(nodes))
	if len(nodes) > 1 && !*singleNode {
		fmt.Printf("  Cluster mode: consistent hashing enabled\n")
	} else {
		fmt.Printf("  Single node mode\n")
	}

	fmt.Printf("\n%s%s╔══════════════════════════════════════════════════════╗%s\n", bold, yellow, reset)
	fmt.Printf("%s%s║   KASOKU — gRPC Pressure Benchmark                  ║%s\n", bold, yellow, reset)
	fmt.Printf("%s%s╚══════════════════════════════════════════════════════╝%s\n\n", bold, yellow, reset)
	fmt.Printf("  Nodes      : %s\n", strings.Join(nodes, "  "))
	fmt.Printf("  Workers    : %d goroutines\n", *workers)
	fmt.Printf("  Batch size : %d keys/request\n", *batchSize)
	fmt.Printf("  Batch-get  : %d keys/request\n", *batchGetSize)
	fmt.Printf("  Write phase: %s\n", *writeDuration)
	fmt.Printf("  Read phase : %s\n\n", *readDuration)

	ctx := context.Background()

	fmt.Printf("%s%s▸ Phase 1 — Write-Only  (warming %s...)%s\n", bold, cyan, *warmDuration, reset)

	warmStop := make(chan struct{})
	var warmWg sync.WaitGroup

	for i := 0; i < *workers; i++ {
		warmWg.Add(1)
		go func(idx int) {
			defer warmWg.Done()
			client := clients[idx%len(clients)]
			payload := []byte(strings.Repeat("X", 128))

			for {
				select {
				case <-warmStop:
					return
				default:
					entries := make([]*api.Entry, *batchSize)
					for j := 0; j < *batchSize; j++ {
						key := fmt.Sprintf("warm:%d:%d", time.Now().UnixNano(), j)
						entries[j] = &api.Entry{
							Key:   key,
							Value: payload,
						}
					}
					resp, err := client.client.BatchPut(ctx, &api.BatchPutRequest{Entries: entries})
					if err == nil && resp.Count > 0 {
						node := nodes[idx%len(nodes)]
						for _, e := range entries {
							pool.push(e.Key, node)
						}
					}
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

	writeHist := newHistogram()
	writeResult := runWritePhase(*workers, *writeDuration, writeHist, pool, clients, nodes, ring, *singleNode)

	printPhaseReport("✍  WRITE", writeResult)

	fmt.Printf("\n%s%s▸ Phase 2 — Read-Only  (pool: %d keys, batch-get: %d)%s\n", bold, cyan, pool.len(), *batchGetSize, reset)
	if !*singleNode && len(nodes) > 1 {
		fmt.Printf("  Reads routed to correct nodes via consistent hashing.\n\n")
	} else {
		fmt.Printf("  Single node mode.\n\n")
	}
	fmt.Printf("  %sSEC   OPS/SEC          LATENCY%s\n", bold, reset)
	fmt.Println("  " + strings.Repeat("─", 55))

	readHist := newHistogram()
	readResult := runReadPhase(*workers, *readDuration, readHist, pool, clients, nodes, ring, *singleNode)

	printPhaseReport("📖  READ", readResult)

	wAvg, _, _ := summaryStats(writeResult.opsPerSec)
	rAvg, _, _ := summaryStats(readResult.opsPerSec)

	fmt.Printf("\n%s%s╔══════════════════════════════════════════════════════╗%s\n", bold, yellow, reset)
	fmt.Printf("%s%s║                  PEAK PERFORMANCE                   ║%s\n", bold, yellow, reset)
	fmt.Printf("%s%s╚══════════════════════════════════════════════════════╝%s\n", bold, yellow, reset)
	fmt.Printf("  %s✍  Writes%s  avg %-8.0f ops/sec   p50 %-8s  p99 %s\n",
		green+bold, reset, wAvg,
		fmtLat(writeHist.percentile(50)),
		fmtLat(writeHist.percentile(99)))
	fmt.Printf("  %s📖 Reads%s   avg %-8.0f ops/sec   p50 %-8s  p99 %s\n",
		green+bold, reset, rAvg,
		fmtLat(readHist.percentile(50)),
		fmtLat(readHist.percentile(99)))
	fmt.Printf("  %s⚡ Total%s   %.0f ops/sec combined\n\n",
		green+bold, reset, wAvg+rAvg)

	if writeResult.totalErrs > writeResult.totalOps/20 || readResult.totalErrs > readResult.totalOps/20 {
		fmt.Printf("  %s[WARNING] High error rate detected!%s\n", red, reset)
	}
}