package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	grpcrpc "github.com/DevLikhith5/kasoku/internal/rpc/grpc"
)

func runBenchmark(name string, addrs []string, workers int, batchSize int, writeDur, readDur time.Duration) {
	pool := grpcrpc.NewPool()

	var writeOps atomic.Int64
	var readOps atomic.Int64
	var writeErrors atomic.Int64
	var readErrors atomic.Int64

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
				_, err := client.BatchReplicatedPut(ctx, entries)
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
				_, err := client.BatchReplicatedGet(ctx, keys)
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

	totalTime := writeTime + readTime
	fmt.Printf("Total: %.0f ops/sec\n", float64(writeOps.Load()+readOps.Load())/totalTime.Seconds())

	pool.Close()
}

func main() {
	// Cluster: 3 nodes on localhost:9002, 9003, 9004
	runBenchmark("3-NODE CLUSTER", []string{"localhost:9002", "localhost:9003", "localhost:9004"}, 30, 30, 5*time.Second, 5*time.Second)
}
