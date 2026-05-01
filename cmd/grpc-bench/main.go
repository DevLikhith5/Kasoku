package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	grpcrpc "github.com/DevLikhith5/kasoku/internal/rpc/grpc"
)

var workload = flag.String("workload", "A", "YCSB workload: A (50/50), B (95/5), C (100 read), D (95/5 insert), E (scan), F (read-modify)")

func runBenchmark(name string, addrs []string, workers int, batchSize int, writeDur, readDur time.Duration) {
	runBenchmarkWithWorkload(name, addrs, workers, batchSize, writeDur, readDur, "A")
}

func runBenchmarkWithWorkload(name string, addrs []string, workers int, batchSize int, writeDur, readDur time.Duration, workload string) {
	pool := grpcrpc.NewPool()

	var writeOps atomic.Int64
	var readOps atomic.Int64
	var writeErrors atomic.Int64
	var readErrors atomic.Int64
	
	var writeLatencies []float64
	var readLatencies []float64
	var latencyMu sync.Mutex

	fmt.Printf("\n=== %s gRPC ===\n", name)

	fmt.Println("Warming up...")
	var warmupWg sync.WaitGroup
	for w := 0; w < workers; w++ {
		warmupWg.Add(1)
		addr := addrs[w%len(addrs)]
		go func() {
			defer warmupWg.Done()
			client, _ := pool.Get(addr)
			_ = client
		}()
	}
	warmupWg.Wait()
	time.Sleep(2 * time.Second)
	fmt.Println("Warmup complete, starting benchmark...")

	var wg sync.WaitGroup

	// === WRITE PHASE ===
	writeEndTime := time.Now().Add(writeDur)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		addr := addrs[w%len(addrs)]
		go func(workerID int, addr string) {
			defer wg.Done()

			client, err := pool.Get(addr)
			if err != nil {
				writeErrors.Add(1)
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			batchNum := 0

			for time.Now().Before(writeEndTime) {
				entries := make([]grpcrpc.BatchWriteEntry, batchSize)
				for j := 0; j < batchSize; j++ {
					entries[j] = grpcrpc.BatchWriteEntry{
						Key:   fmt.Sprintf("bench:%s:%d:%d", name, workerID, batchNum),
						Value: []byte("value"),
					}
				}
				batchNum++
				start := time.Now()
				_, err := client.BatchReplicatedPut(ctx, entries)
				latency := time.Since(start).Seconds() * 1000 // ms
				latencyMu.Lock()
				writeLatencies = append(writeLatencies, latency)
				latencyMu.Unlock()
				if err != nil {
					writeErrors.Add(1)
					continue
				}
				writeOps.Add(int64(batchSize))
			}
		}(w, addr)
	}

	writeStart := time.Now()
	wg.Wait()
	writeTime := time.Since(writeStart)

	fmt.Printf("Writes: %d in %.2fs = %.0f ops/sec\n", writeOps.Load(), writeTime.Seconds(), float64(writeOps.Load())/writeTime.Seconds())
	if writeErrors.Load() > 0 {
		fmt.Printf("Write errors: %d\n", writeErrors.Load())
	}

	time.Sleep(200 * time.Millisecond)

	// === READ PHASE ===
	readEndTime := time.Now().Add(readDur)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		addr := addrs[w%len(addrs)]
		go func(workerID int, addr string) {
			defer wg.Done()

			client, err := pool.Get(addr)
			if err != nil {
				readErrors.Add(1)
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			keyBase := fmt.Sprintf("bench:%s:0:", name)

			for time.Now().Before(readEndTime) {
				keys := make([]string, batchSize)
				for j := 0; j < batchSize; j++ {
					keys[j] = fmt.Sprintf("%s%d", keyBase, j%10)
				}
				start := time.Now()
				_, err := client.BatchReplicatedGet(ctx, keys)
				latency := time.Since(start).Seconds() * 1000 // ms
				latencyMu.Lock()
				readLatencies = append(readLatencies, latency)
				latencyMu.Unlock()
				if err != nil {
					readErrors.Add(1)
					continue
				}
				readOps.Add(int64(batchSize))
			}
		}(w, addr)
	}

	readStart := time.Now()
	wg.Wait()
	readTime := time.Since(readStart)

	fmt.Printf("Reads: %d in %.2fs = %.0f ops/sec\n", readOps.Load(), readTime.Seconds(), float64(readOps.Load())/readTime.Seconds())
	if readErrors.Load() > 0 {
		fmt.Printf("Read errors: %d\n", readErrors.Load())
	}

	// Print latency percentiles
	sort.Float64s(writeLatencies)
	sort.Float64s(readLatencies)
	fmt.Println("\n=== Latency Percentiles ===")
	if len(writeLatencies) > 0 {
		fmt.Printf("Write latency (ms): p50=%.2f, p95=%.2f, p99=%.2f, max=%.2f (samples=%d)\n", 
			percentile(writeLatencies, 50), 
			percentile(writeLatencies, 95), 
			percentile(writeLatencies, 99),
			percentile(writeLatencies, 100),
			len(writeLatencies))
	} else {
		fmt.Println("Write latency (ms): N/A (no writes in this workload)")
	}
	if len(readLatencies) > 0 {
		fmt.Printf("Read latency (ms):  p50=%.2f, p95=%.2f, p99=%.2f, max=%.2f (samples=%d)\n", 
			percentile(readLatencies, 50), 
			percentile(readLatencies, 95), 
			percentile(readLatencies, 99),
			percentile(readLatencies, 100),
			len(readLatencies))
	} else {
		fmt.Println("Read latency (ms):  N/A (no reads in this workload)")
	}

	totalTime := writeTime + readTime
	fmt.Printf("Total: %.0f ops/sec\n", float64(writeOps.Load()+readOps.Load())/totalTime.Seconds())

	pool.Close()
}

func percentile(sorted []float64, p int) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(len(sorted))*float64(p)/100)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func main() {
	flag.Parse()
	
	// YCSB-style workloads
	switch *workload {
	case "A":
		// 50% reads, 50% writes - balanced
		fmt.Println("=== YCSB Workload A (50% read, 50% write) ===")
		runBenchmark("WorkloadA", []string{"localhost:9100"}, 500, 500, 10*time.Second, 10*time.Second)
		
	case "B":
		// 95% reads, 5% writes - read heavy
		fmt.Println("=== YCSB Workload B (95% read, 5% write) ===")
		runBenchmark("WorkloadB", []string{"localhost:9100"}, 500, 500, 5*time.Second, 20*time.Second)
		
	case "C":
		// 100% reads - read only
		fmt.Println("=== YCSB Workload C (100% read) ===")
		runBenchmark("WorkloadC", []string{"localhost:9100"}, 500, 500, 0, 20*time.Second)
		
	case "D":
		// 95% reads, 5% inserts (newer keys)
		fmt.Println("=== YCSB Workload D (95% read, 5% insert) ===")
		runBenchmark("WorkloadD", []string{"localhost:9100"}, 500, 500, 5*time.Second, 20*time.Second)
		
	case "E":
		// Range scans
		fmt.Println("=== YCSB Workload E (scan) ===")
		runBenchmark("WorkloadE", []string{"localhost:9100"}, 300, 100, 5*time.Second, 20*time.Second)
		
	case "F":
		// Read-modify-write
		fmt.Println("=== YCSB Workload F (read-modify-write) ===")
		runBenchmark("WorkloadF", []string{"localhost:9100"}, 500, 500, 10*time.Second, 10*time.Second)
		
	default:
		// Default: balanced
		runBenchmark("DEFAULT", []string{"localhost:9100"}, 700, 700, 5*time.Second, 5*time.Second)
	}
}
