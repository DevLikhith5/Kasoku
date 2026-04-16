package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type BatchEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type BatchRequest struct {
	Entries []BatchEntry `json:"entries"`
}

func main() {
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        5000,
			MaxIdleConnsPerHost: 1000,
		},
		Timeout: 5 * time.Second,
	}

	fmt.Println("🚀 RUNNING KASOKU THROUGHPUT BENCHMARK")
	fmt.Println("Testing optimizations: Batched Replication, Async WAL, Binary RPC")
	
	// Benchmark Batches
	workers := 20
	batchesPerWorker := 50
	batchSize := 100
	
	var totalWrites uint64
	var successWrites uint64
	
	start := time.Now()
	var wg sync.WaitGroup
	
	fmt.Printf("Spawning %d workers to push %d keys total via /api/v1/batch...\n", workers, workers*batchesPerWorker*batchSize)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < batchesPerWorker; i++ {
				var entries []BatchEntry
				for b := 0; b < batchSize; b++ {
					key := fmt.Sprintf("bench-batch-%d-%d-%d", workerID, i, b)
					entries = append(entries, BatchEntry{
						Key:   key,
						Value: "payload-data",
					})
				}
				
				reqBody, _ := json.Marshal(BatchRequest{Entries: entries})
				url := "http://localhost:9000/api/v1/batch"
				req, _ := http.NewRequest("PUT", url, bytes.NewReader(reqBody))
				req.Header.Set("Content-Type", "application/json")
				
				resp, err := client.Do(req)
				if err == nil && resp.StatusCode == 200 {
					atomic.AddUint64(&successWrites, uint64(batchSize))
				}
				if resp != nil {
					resp.Body.Close()
				}
				atomic.AddUint64(&totalWrites, uint64(batchSize))
			}
		}(w)
	}

	wg.Wait()
	duration := time.Since(start)
	
	writes := atomic.LoadUint64(&successWrites)
	throughput := float64(writes) / duration.Seconds()
	
	fmt.Printf("\n🏁 BENCHMARK COMPLETE\n")
	fmt.Printf("Time Elapsed : %.2fs\n", duration.Seconds())
	fmt.Printf("Total Written: %d keys\n", writes)
	fmt.Printf("Throughput   : %.0f Ops/sec\n", throughput)
}
