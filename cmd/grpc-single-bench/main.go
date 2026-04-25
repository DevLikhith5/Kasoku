package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	grpcrpc "github.com/DevLikhith5/kasoku/internal/rpc/grpc"
)

func main() {
	pool := grpcrpc.NewPool()

	var writeOps atomic.Int64
	var writeErrors atomic.Int64
	var readOps atomic.Int64
	var readErrors atomic.Int64

	workers := 30
	batchSize := 50
	duration := 10 * time.Second

	fmt.Printf("=== SINGLE NODE gRPC (30 workers, batch=%d) ===\n", batchSize)

	// Get a test client first
	testClient, err := pool.Get("localhost:9002")
	if err != nil {
		fmt.Printf("Failed to get client: %v\n", err)
		return
	}
	fmt.Printf("Client connected: %v\n", testClient != nil)

	// WRITE
	writeEnd := time.Now().Add(duration)
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			client, err := pool.Get("localhost:9002")
			if err != nil {
				writeErrors.Add(1)
				return
			}
			ctx := context.Background()
			batch := 0
			for time.Now().Before(writeEnd) {
				entries := make([]grpcrpc.BatchWriteEntry, batchSize)
				for i := 0; i < batchSize; i++ {
					entries[i] = grpcrpc.BatchWriteEntry{
						Key:   fmt.Sprintf("bench:%d:%d", id, batch),
						Value: []byte("val"),
					}
				}
				batch++
				_, err := client.BatchReplicatedPut(ctx, entries)
				if err != nil {
					writeErrors.Add(1)
					continue
				}
				writeOps.Add(int64(batchSize))
			}
		}(w)
	}

	wg.Wait()
	writeTime := time.Since(time.Now().Add(-duration))
	fmt.Printf("Writes: %d in %.2fs = %.0f ops/sec (errors: %d)\n", writeOps.Load(), writeTime.Seconds(), float64(writeOps.Load())/writeTime.Seconds(), writeErrors.Load())

	time.Sleep(200 * time.Millisecond)

	// READ
	readEnd := time.Now().Add(duration)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			client, err := pool.Get("localhost:9002")
			if err != nil {
				readErrors.Add(1)
				return
			}
			ctx := context.Background()
			batch := 0
			for time.Now().Before(readEnd) {
				keys := make([]string, batchSize)
				for i := 0; i < batchSize; i++ {
					keys[i] = fmt.Sprintf("bench:%d:%d", i%10, 0)
				}
				batch++
				_, err := client.BatchReplicatedGet(ctx, keys)
				if err != nil {
					readErrors.Add(1)
					continue
				}
				readOps.Add(int64(batchSize))
			}
		}(w)
	}

	wg.Wait()
	readTime := time.Since(time.Now().Add(-duration))
	fmt.Printf("Reads: %d in %.2fs = %.0f ops/sec (errors: %d)\n", readOps.Load(), readTime.Seconds(), float64(readOps.Load())/readTime.Seconds(), readErrors.Load())
	fmt.Printf("Total: %.0f ops/sec\n", float64(writeOps.Load()+readOps.Load())/(writeTime.Seconds()+readTime.Seconds()))

	pool.Close()
}