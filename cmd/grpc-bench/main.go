package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	grpcrpc "github.com/DevLikhith5/kasoku/internal/rpc/grpc"
)

var (
	nodes   = flag.String("nodes", "localhost:9100,localhost:9101,localhost:9102", "gRPC addresses")
	workers = flag.Int("workers", 30, "Concurrent workers")
	batch   = flag.Int("batch", 1, "Keys per request (default 1)")
	seedN   = flag.Int("seed", 500000, "Keys to pre-load")
	readPct = flag.Int("reads", 80, "Percentage of reads")
	dur     = flag.Int("dur", 30, "Benchmark duration (seconds)")
)

type collector struct {
	mu   sync.Mutex
	lats []float64
	ops  int64
	errs int64
}

func (c *collector) add(t time.Duration, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lats = append(c.lats, float64(t.Microseconds()))
	if ok {
		c.ops++
	} else {
		c.errs++
	}
}

func (c *collector) report() (int64, int64, float64, float64, float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.lats) == 0 {
		return 0, 0, 0, 0, 0
	}
	sorted := make([]float64, len(c.lats))
	copy(sorted, c.lats)
	sort.Float64s(sorted)
	n := len(sorted)
	return c.ops, c.errs, sorted[n/2], sorted[n*95/100], sorted[n*99/100]
}

func main() {
	flag.Parse()
	addrs := strings.Split(*nodes, ",")
	W, B, N := *workers, *batch, *seedN
	pool := grpcrpc.NewPool()

	fmt.Printf(`
╔══════════════════════════════════════════════════════════════╗
║     KASOKU - gRPC Benchmark                                  ║
╠══════════════════════════════════════════════════════════════╣
║  Nodes: %d  |  Workers: %d  |  Batch: %d keys/request       ║
║  Pre-load: %d keys  |  Workload: %d%%R/%d%%W  |  Duration: %ds ║
╚══════════════════════════════════════════════════════════════╝
`, len(addrs), W, B, N, *readPct, 100-*readPct, *dur)

	fmt.Printf("\n▸ Phase 1 — Pre-loading %d keys (100-byte values)...\n", N)
	var preloaded int64
	var wg sync.WaitGroup

	loadStart := time.Now()
	for i := 0; i < W; i++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			c, _ := pool.Get(addrs[wid%len(addrs)])
			if c == nil { return }
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			for {
				start := atomic.AddInt64(&preloaded, int64(B))
				if start > int64(N) { break }
				entries := make([]grpcrpc.BatchWriteEntry, B)
				for j := 0; j < B && start+int64(j) <= int64(N); j++ {
					entries[j] = grpcrpc.BatchWriteEntry{
						Key:   fmt.Sprintf("prod:%d", start+int64(j)-1),
						Value: []byte(strings.Repeat("X", 100)),
					}
				}
				if len(entries) > 0 {
					c.BatchReplicatedPut(ctx, entries)
				}
			}
		}(i)
	}
	wg.Wait()
	totalPre := atomic.LoadInt64(&preloaded)
	fmt.Printf("  Loaded: %d keys in %.1fs (%.0f keys/sec)\n",
		totalPre, time.Since(loadStart).Seconds(),
		float64(totalPre)/time.Since(loadStart).Seconds())

	fmt.Printf("\n▸ Phase 2 — Mixed Workload (%d%%R/%d%%W, %ds)\n",
		*readPct, 100-*readPct, *dur)

	wCollector := &collector{}
	rCollector := &collector{}
	stopCh := make(chan struct{})
	var wg2 sync.WaitGroup

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		startT := time.Now()
		for {
			select {
			case <-ticker.C:
				elapsed := int(time.Since(startT).Seconds())
				wo, _, w50, w95, w99 := wCollector.report()
				ro, _, r50, r95, r99 := rCollector.report()
				fmt.Printf("  [%3ds] W: %d (p50=%.0fµs p95=%.0fµs p99=%.0fµs)  R: %d (p50=%.0fµs p95=%.0fµs p99=%.0fµs)\n",
					elapsed, wo, w50, w95, w99, ro, r50, r95, r99)
			case <-stopCh:
				return
			}
		}
	}()

	benchStart := time.Now()
	for i := 0; i < W; i++ {
		wg2.Add(1)
		go func(wid int) {
			defer wg2.Done()
			c, _ := pool.Get(addrs[wid%len(addrs)])
			if c == nil { return }
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*dur+10)*time.Second)
			defer cancel()

			rng := rand.New(rand.NewSource(int64(wid)*time.Now().UnixNano()))
			hotSize := int64(float64(totalPre) * 0.2)

			for time.Since(benchStart) < time.Duration(*dur)*time.Second {
				if rng.Intn(100) < *readPct {
					keys := make([]string, B)
					for j := 0; j < B; j++ {
						if rng.Float64() < 0.8 {
							keys[j] = fmt.Sprintf("prod:%d", rng.Int63n(hotSize))
						} else {
							keys[j] = fmt.Sprintf("prod:%d", rng.Int63n(totalPre))
						}
					}
					start := time.Now()
					_, err := c.BatchReplicatedGet(ctx, keys)
					rCollector.add(time.Since(start), err == nil)
				} else {
					entries := make([]grpcrpc.BatchWriteEntry, B)
					for j := 0; j < B; j++ {
						entries[j] = grpcrpc.BatchWriteEntry{
							Key:   fmt.Sprintf("new:%d:%d", wid, atomic.AddInt64(&preloaded, 1)),
							Value: []byte(strings.Repeat("Y", 100)),
						}
					}
					start := time.Now()
					_, err := c.BatchReplicatedPut(ctx, entries)
					wCollector.add(time.Since(start), err == nil)
				}
			}
		}(i)
	}

	wg2.Wait()
	close(stopCh)
	benchTime := time.Since(benchStart).Seconds()

	wOps, wErrs, w50, w95, w99 := wCollector.report()
	rOps, rErrs, r50, r95, r99 := rCollector.report()

	wOpsSec := float64(wOps*int64(B)) / benchTime
	rOpsSec := float64(rOps*int64(B)) / benchTime

	fmt.Printf(`
╔══════════════════════════════════════════════════════════════╗
║                    RESULTS                                   ║
╠══════════════════════════════════════════════════════════════╣
║  WRITES:  %8d batches = %.0f keys/sec                    ║
║           p50=%.1fms  p95=%.1fms  p99=%.1fms                  ║
║                                                              ║
║  READS:   %8d batches = %.0f keys/sec                    ║
║           p50=%.1fms  p95=%.1fms  p99=%.1fms                  ║
╠══════════════════════════════════════════════════════════════╣
║  COMBINED: %.0f keys/sec (RF=3, W=1, fire-and-forget)         ║
║  Dataset:  %.0fM keys (~%.0fMB)  |  Duration: %.1fs         ║
╚══════════════════════════════════════════════════════════════╝
`,
		wOps, wOpsSec, w50/1000, w95/1000, w99/1000,
		rOps, rOpsSec, r50/1000, r95/1000, r99/1000,
		wOpsSec+rOpsSec,
		float64(totalPre)/1e6, float64(totalPre)*110/1e6, benchTime)

	if wErrs > 0 || rErrs > 0 {
		fmt.Printf("  WARN: %d write errors, %d read errors\n", wErrs, rErrs)
	}

	pool.Close()
}