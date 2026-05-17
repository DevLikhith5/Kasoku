package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	grpcrpc "github.com/DevLikhith5/kasoku/internal/rpc/grpc"
	storage "github.com/DevLikhith5/kasoku/internal/store"
)

// ──────────────────────────────────────────────────────────────────────────────
// YCSB Workload Definitions (industry standard)
// ──────────────────────────────────────────────────────────────────────────────

type WorkloadProfile struct {
	Name        string
	ReadPct     int  // read %
	UpdatePct   int  // update % (write to existing key)
	InsertPct   int  // insert % (write new key)
	ScanPct     int  // scan/range-read %
	RMWPct      int  // read-modify-write %
	Distribution string // uniform | zipfian | latest
	Description string
}

var workloads = map[string]WorkloadProfile{
	"a": {Name: "A", ReadPct: 50, UpdatePct: 50, Distribution: "zipfian",
		Description: "Update Heavy — 50% Read / 50% Update"},
	"b": {Name: "B", ReadPct: 95, UpdatePct: 5, Distribution: "zipfian",
		Description: "Read Mostly — 95% Read / 5% Update"},
	"c": {Name: "C", ReadPct: 100, Distribution: "zipfian",
		Description: "Read Only — 100% Read"},
	"d": {Name: "D", ReadPct: 95, InsertPct: 5, Distribution: "latest",
		Description: "Read Latest — 95% Read / 5% Insert"},
	"e": {Name: "E", ScanPct: 95, InsertPct: 5, Distribution: "zipfian",
		Description: "Short Ranges — 95% Scan / 5% Insert"},
	"f": {Name: "F", ReadPct: 50, RMWPct: 50, Distribution: "zipfian",
		Description: "Read-Modify-Write — 50% Read / 50% RMW"},
}

// ──────────────────────────────────────────────────────────────────────────────
// Zipfian Distribution (standard YCSB scrambled zipfian)
// ──────────────────────────────────────────────────────────────────────────────

type ZipfianGenerator struct {
	items     int64
	base      int64
	theta     float64
	zeta2     float64
	zetaN     float64
	alpha     float64
	eta       float64
	countFZI  int64
}

func NewZipfian(min, max int64, theta float64) *ZipfianGenerator {
	items := max - min + 1
	z := &ZipfianGenerator{
		items: items,
		base:  min,
		theta: theta,
	}
	z.zeta2 = z.zetaStatic(2, theta)
	z.zetaN = z.zetaStatic(items, theta)
	z.alpha = 1.0 / (1.0 - theta)
	z.eta = (1.0 - math.Pow(2.0/float64(items), 1.0-theta)) / (1.0 - z.zeta2/z.zetaN)
	z.countFZI = items
	return z
}

func (z *ZipfianGenerator) zetaStatic(n int64, theta float64) float64 {
	sum := 0.0
	for i := int64(0); i < n; i++ {
		sum += 1.0 / math.Pow(float64(i+1), theta)
	}
	return sum
}

func (z *ZipfianGenerator) Next(rng *rand.Rand) int64 {
	u := rng.Float64()
	uz := u * z.zetaN

	if uz < 1.0 {
		return z.base
	}
	if uz < 1.0+math.Pow(0.5, z.theta) {
		return z.base + 1
	}

	ret := z.base + int64(float64(z.items)*math.Pow(z.eta*u-z.eta+1.0, z.alpha))
	if ret >= z.base+z.items {
		ret = z.base + z.items - 1
	}
	return ret
}

// ScrambledZipfian wraps Zipfian with FNV hash to spread hotspots across keyspace
type ScrambledZipfian struct {
	gen   *ZipfianGenerator
	min   int64
	max   int64
	count int64
}

func NewScrambledZipfian(min, max int64) *ScrambledZipfian {
	return &ScrambledZipfian{
		gen:   NewZipfian(0, max-min, 0.99),
		min:   min,
		max:   max,
		count: max - min + 1,
	}
}

func (s *ScrambledZipfian) Next(rng *rand.Rand) int64 {
	val := s.gen.Next(rng)
	// FNV hash to scramble — standard YCSB technique
	val = fnvHash64(val) % s.count
	return s.min + val
}

func fnvHash64(val int64) int64 {
	const (
		fnvOffset = uint64(14695981039346656037)
		fnvPrime  = uint64(1099511628211)
	)
	hash := fnvOffset
	for i := 0; i < 8; i++ {
		hash ^= uint64(val>>(i*8)) & 0xFF
		hash *= fnvPrime
	}
	return int64(hash & 0x7FFFFFFFFFFFFFFF)
}

// ──────────────────────────────────────────────────────────────────────────────
// Latency Collector (HDR Histogram style)
// ──────────────────────────────────────────────────────────────────────────────

type LatencyCollector struct {
	mu   sync.Mutex
	lats []float64
	ops  int64
	errs int64
}

func (c *LatencyCollector) Record(d time.Duration, ok bool) {
	c.mu.Lock()
	c.lats = append(c.lats, float64(d.Microseconds()))
	if ok {
		c.ops++
	} else {
		c.errs++
	}
	c.mu.Unlock()
}

type LatencyStats struct {
	Ops     int64   `json:"ops"`
	Errors  int64   `json:"errors"`
	AvgUs   float64 `json:"avg_us"`
	P50Us   float64 `json:"p50_us"`
	P95Us   float64 `json:"p95_us"`
	P99Us   float64 `json:"p99_us"`
	P999Us  float64 `json:"p999_us"`
	MaxUs   float64 `json:"max_us"`
	MinUs   float64 `json:"min_us"`
}

func (c *LatencyCollector) Stats() LatencyStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.lats) == 0 {
		return LatencyStats{Ops: c.ops, Errors: c.errs}
	}

	sorted := make([]float64, len(c.lats))
	copy(sorted, c.lats)
	sort.Float64s(sorted)
	n := len(sorted)

	sum := 0.0
	for _, v := range sorted {
		sum += v
	}

	pIdx := func(p float64) int {
		idx := int(float64(n) * p / 100.0)
		if idx >= n {
			idx = n - 1
		}
		return idx
	}

	return LatencyStats{
		Ops:    c.ops,
		Errors: c.errs,
		AvgUs:  sum / float64(n),
		P50Us:  sorted[pIdx(50)],
		P95Us:  sorted[pIdx(95)],
		P99Us:  sorted[pIdx(99)],
		P999Us: sorted[pIdx(99.9)],
		MaxUs:  sorted[n-1],
		MinUs:  sorted[0],
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Main
// ──────────────────────────────────────────────────────────────────────────────

var (
	fNodes       = flag.String("nodes", "localhost:9100,localhost:9101,localhost:9102", "gRPC node addresses")
	fWorkers     = flag.Int("workers", 50, "concurrent worker goroutines")
	fBatch       = flag.Int("batch", 1, "keys per gRPC request")
	fRecordCount = flag.Int("recordcount", 1000000, "number of records to load (YCSB recordcount)")
	fOpCount     = flag.Int("operationcount", 0, "total ops to run (0 = use -dur instead)")
	fDuration    = flag.Int("dur", 30, "benchmark duration seconds (ignored if -operationcount > 0)")
	fWorkload    = flag.String("workload", "b", "YCSB workload: a,b,c,d,e,f or all")
	fFieldLen    = flag.Int("fieldlength", 100, "value size in bytes (YCSB fieldlength)")
	fJSON        = flag.Bool("json", false, "output results as JSON")
)

func main() {
	flag.Parse()

	addrs := strings.Split(*fNodes, ",")
	pool := grpcrpc.NewPool()
	defer pool.Close()

	if *fWorkload == "all" {
		for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
			runWorkload(id, addrs, pool)
			fmt.Println()
		}
	} else {
		for _, id := range strings.Split(*fWorkload, ",") {
			runWorkload(strings.TrimSpace(id), addrs, pool)
			fmt.Println()
		}
	}
}

func runWorkload(id string, addrs []string, pool *grpcrpc.Pool) {
	wl, ok := workloads[id]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown workload: %s\n", id)
		return
	}

	W := *fWorkers
	B := *fBatch
	N := *fRecordCount
	valueSize := *fFieldLen
	value := []byte(strings.Repeat("X", valueSize))

	fmt.Printf("╔══════════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║  YCSB Workload %s — %s\n", wl.Name, wl.Description)
	fmt.Printf("╠══════════════════════════════════════════════════════════════════╣\n")
	fmt.Printf("║  Nodes: %-3d | Workers: %-4d | Batch: %-4d | Distribution: %-8s║\n",
		len(addrs), W, B, wl.Distribution)
	fmt.Printf("║  RecordCount: %-10d | FieldLength: %-4d bytes                  ║\n", N, valueSize)
	fmt.Printf("╚══════════════════════════════════════════════════════════════════╝\n")

	// ─── Phase 1: Load ──────────────────────────────────────────────────
	fmt.Printf("\n[LOAD] Loading %d records (%d-byte values)...\n", N, valueSize)
	var loaded int64
	var wg sync.WaitGroup
	loadStart := time.Now()

	for i := 0; i < W; i++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			c, err := pool.Get(addrs[wid%len(addrs)])
			if c == nil || err != nil {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()

			for {
				batchSize := 500
				start := int(atomic.AddInt64(&loaded, int64(batchSize)))
				base := start - batchSize
				if base >= N {
					break
				}
				actual := batchSize
				if start > N {
					actual = batchSize - (start - N)
				}

				entries := make([]storage.Entry, actual)
				for j := 0; j < actual; j++ {
					entries[j] = storage.Entry{
						Key:   fmt.Sprintf("usertable:user%010d", base+j),
						Value: value,
					}
				}
				c.BatchReplicatedPut(ctx, entries)
			}
		}(i)
	}
	wg.Wait()
	loadTime := time.Since(loadStart)
	loadOps := float64(N) / loadTime.Seconds()
	fmt.Printf("[LOAD] Loaded %d records in %.2fs — %.0f ops/sec\n", N, loadTime.Seconds(), loadOps)

	// ─── Phase 2: Run ───────────────────────────────────────────────────
	durSec := *fDuration
	opCount := *fOpCount

	if opCount > 0 {
		fmt.Printf("\n[RUN]  Running %d operations (workload %s)...\n", opCount, wl.Name)
	} else {
		fmt.Printf("\n[RUN]  Running for %ds (workload %s)...\n", durSec, wl.Name)
	}

	readCol := &LatencyCollector{}
	updateCol := &LatencyCollector{}
	insertCol := &LatencyCollector{}
	scanCol := &LatencyCollector{}
	rmwCol := &LatencyCollector{}

	var totalOps int64
	insertCounter := int64(N) // new inserts start after the loaded keys
	stopCh := make(chan struct{})

	// Progress reporter
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		startT := time.Now()
		for {
			select {
			case <-ticker.C:
				elapsed := time.Since(startT).Seconds()
				ops := atomic.LoadInt64(&totalOps)
				throughput := float64(ops) / elapsed
				rSt := readCol.Stats()
				wSt := updateCol.Stats()
				fmt.Printf("[%4.0fs] throughput: %.0f ops/sec | reads: %d (p99=%.0fµs) | writes: %d (p99=%.0fµs) | errs: %d\n",
					elapsed, throughput, rSt.Ops, rSt.P99Us, wSt.Ops+insertCol.Stats().Ops, wSt.P99Us, rSt.Errors+wSt.Errors)
			case <-stopCh:
				return
			}
		}
	}()

	benchStart := time.Now()
	var wg2 sync.WaitGroup

	for i := 0; i < W; i++ {
		wg2.Add(1)
		go func(wid int) {
			defer wg2.Done()
			c, err := pool.Get(addrs[wid%len(addrs)])
			if c == nil || err != nil {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(durSec+60)*time.Second)
			defer cancel()

			rng := rand.New(rand.NewSource(int64(wid) * time.Now().UnixNano()))
			keyN := int64(N)

			var keyGen func() int64
			switch wl.Distribution {
			case "zipfian":
				zipf := NewScrambledZipfian(0, keyN-1)
				keyGen = func() int64 { return zipf.Next(rng) }
			case "latest":
				// latest: bias toward most recently inserted
				keyGen = func() int64 {
					cur := atomic.LoadInt64(&insertCounter)
					// exponential backoff from latest
					delta := int64(math.Abs(rng.ExpFloat64() * float64(cur) * 0.01))
					idx := cur - delta
					if idx < 0 {
						idx = 0
					}
					return idx
				}
			default: // uniform
				keyGen = func() int64 { return rng.Int63n(keyN) }
			}

			for {
				// Check termination
				if opCount > 0 {
					cur := atomic.AddInt64(&totalOps, 1)
					if cur > int64(opCount) {
						return
					}
				} else {
					if time.Since(benchStart) >= time.Duration(durSec)*time.Second {
						return
					}
					atomic.AddInt64(&totalOps, 1)
				}

				roll := rng.Intn(100)
				switch {
				case roll < wl.ReadPct:
					// READ
					keys := make([]string, B)
					for j := 0; j < B; j++ {
						keys[j] = fmt.Sprintf("usertable:user%010d", keyGen())
					}
					start := time.Now()
					_, err := c.BatchReplicatedGet(ctx, keys)
					readCol.Record(time.Since(start), err == nil)

				case roll < wl.ReadPct+wl.UpdatePct:
					// UPDATE
					entries := make([]storage.Entry, B)
					for j := 0; j < B; j++ {
						entries[j] = storage.Entry{
							Key:   fmt.Sprintf("usertable:user%010d", keyGen()),
							Value: value,
						}
					}
					start := time.Now()
					_, err := c.BatchReplicatedPut(ctx, entries)
					updateCol.Record(time.Since(start), err == nil)

				case roll < wl.ReadPct+wl.UpdatePct+wl.InsertPct:
					// INSERT
					entries := make([]storage.Entry, B)
					for j := 0; j < B; j++ {
						newID := atomic.AddInt64(&insertCounter, 1)
						entries[j] = storage.Entry{
							Key:   fmt.Sprintf("usertable:user%010d", newID),
							Value: value,
						}
					}
					start := time.Now()
					_, err := c.BatchReplicatedPut(ctx, entries)
					insertCol.Record(time.Since(start), err == nil)

				case roll < wl.ReadPct+wl.UpdatePct+wl.InsertPct+wl.ScanPct:
					// SCAN (multi-key read simulating range scan)
					startKey := keyGen()
					keys := make([]string, B)
					for j := 0; j < B; j++ {
						keys[j] = fmt.Sprintf("usertable:user%010d", startKey+int64(j))
					}
					start := time.Now()
					_, err := c.BatchReplicatedGet(ctx, keys)
					scanCol.Record(time.Since(start), err == nil)

				default:
					// READ-MODIFY-WRITE
					keys := make([]string, B)
					for j := 0; j < B; j++ {
						keys[j] = fmt.Sprintf("usertable:user%010d", keyGen())
					}
					start := time.Now()
					results, err := c.BatchReplicatedGet(ctx, keys)
					if err != nil {
						rmwCol.Record(time.Since(start), false)
						continue
					}
					entries := make([]storage.Entry, 0, len(results))
					for k, v := range results {
						// modify: append marker to existing value
						newVal := make([]byte, len(v))
						copy(newVal, v)
						entries = append(entries, storage.Entry{Key: k, Value: newVal})
					}
					if len(entries) == 0 {
						entries = append(entries, storage.Entry{
							Key: keys[0], Value: value,
						})
					}
					_, err = c.BatchReplicatedPut(ctx, entries)
					rmwCol.Record(time.Since(start), err == nil)
				}
			}
		}(i)
	}

	wg2.Wait()
	close(stopCh)
	benchTime := time.Since(benchStart).Seconds()

	// ─── Results ────────────────────────────────────────────────────────
	rSt := readCol.Stats()
	uSt := updateCol.Stats()
	iSt := insertCol.Stats()
	sSt := scanCol.Stats()
	rmwSt := rmwCol.Stats()

	totalCompleted := rSt.Ops + uSt.Ops + iSt.Ops + sSt.Ops + rmwSt.Ops
	totalErrors := rSt.Errors + uSt.Errors + iSt.Errors + sSt.Errors + rmwSt.Errors
	throughput := float64(totalCompleted) / benchTime

	if *fJSON {
		result := map[string]interface{}{
			"workload":    wl.Name,
			"description": wl.Description,
			"config": map[string]interface{}{
				"nodes": len(addrs), "workers": W, "batch": B,
				"recordcount": N, "fieldlength": valueSize,
				"distribution": wl.Distribution,
			},
			"load": map[string]interface{}{
				"records": N, "time_sec": loadTime.Seconds(), "ops_sec": loadOps,
			},
			"run": map[string]interface{}{
				"time_sec": benchTime, "throughput": throughput,
				"total_ops": totalCompleted, "errors": totalErrors,
			},
		}
		if rSt.Ops > 0 {
			result["READ"] = rSt
		}
		if uSt.Ops > 0 {
			result["UPDATE"] = uSt
		}
		if iSt.Ops > 0 {
			result["INSERT"] = iSt
		}
		if sSt.Ops > 0 {
			result["SCAN"] = sSt
		}
		if rmwSt.Ops > 0 {
			result["READ-MODIFY-WRITE"] = rmwSt
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(result)
		return
	}

	fmt.Printf("\n")
	fmt.Printf("══════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("  YCSB Workload %s — %s\n", wl.Name, wl.Description)
	fmt.Printf("══════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("  [OVERALL] RunTime(ms)=%.0f  Throughput(ops/sec)=%.0f\n",
		benchTime*1000, throughput)
	fmt.Printf("  [OVERALL] Operations=%d  Errors=%d\n", totalCompleted, totalErrors)

	printOpStats := func(name string, st LatencyStats) {
		if st.Ops == 0 {
			return
		}
		fmt.Printf("  [%s] Operations=%d  Errors=%d\n", name, st.Ops, st.Errors)
		fmt.Printf("  [%s] AverageLatency(us)=%.1f\n", name, st.AvgUs)
		fmt.Printf("  [%s] MinLatency(us)=%.0f\n", name, st.MinUs)
		fmt.Printf("  [%s] MaxLatency(us)=%.0f\n", name, st.MaxUs)
		fmt.Printf("  [%s] p50=%.0fus  p95=%.0fus  p99=%.0fus  p999=%.0fus\n",
			name, st.P50Us, st.P95Us, st.P99Us, st.P999Us)
		opsPerSec := float64(st.Ops) / benchTime
		fmt.Printf("  [%s] Throughput(ops/sec)=%.0f\n", name, opsPerSec)
	}

	printOpStats("READ", rSt)
	printOpStats("UPDATE", uSt)
	printOpStats("INSERT", iSt)
	printOpStats("SCAN", sSt)
	printOpStats("READ-MODIFY-WRITE", rmwSt)

	fmt.Printf("══════════════════════════════════════════════════════════════════════\n")
}
