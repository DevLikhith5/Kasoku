package lsm

import (
	crand "crypto/rand"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	storage "github.com/DevLikhith5/kasoku/internal/store"
)

// =============================================================================
// PRODUCTION-GRADE BENCHMARKS
//
// These benchmarks follow industry-standard practices used by RocksDB, LevelDB,
// Cassandra, and YCSB (Yahoo! Cloud Serving Benchmark).
//
// Key differences from the existing benchmarks:
//
//   1. DATASET > MEMTABLE: We use a small MemTable (4MB) and pre-populate with
//      enough data (500K+ keys, ~50MB+) to force multiple SSTable flushes.
//      This ensures reads actually hit SSTables on disk, not just the MemTable.
//
//   2. FORCED FLUSH: After pre-populating, we call FlushMemTable() to push
//      all data to SSTables before measuring read performance.
//
//   3. SMALL KEY CACHE: We use a small key cache (1000 entries) so most reads
//      go through the full path: Bloom filter → index → block cache → disk.
//
//   4. ZIPFIAN DISTRIBUTION: Real workloads aren't uniform. 20% of keys get
//      80% of traffic. We use Zipfian to model this.
//
//   5. REALISTIC VALUE SIZES: 100B-4KB values instead of tiny "value-123" strings.
//
//   6. LATENCY PERCENTILES: We measure p50, p95, p99, not just throughput.
//
// =============================================================================

// prodConfig returns a config that forces data through SSTables.
// Small MemTable + small cache = realistic disk I/O.
func prodConfig() LSMConfig {
	return LSMConfig{
		MemTableSize:        4 * 1024 * 1024,  // 4MB — forces frequent flushes
		MaxMemtableBytes:    16 * 1024 * 1024,  // 16MB total memtable memory
		CompactionThreshold: 4,                 // compact after 4 SSTables per level
		BloomFPRate:         0.01,
		KeyCacheSize:        1000,              // tiny cache — force SSTable reads
		MaxImmutable:        4,
		NodeID:              "bench-node",
	}
}

// populateAndFlush writes N keys with the given value size, then flushes
// all MemTables to SSTables. Returns the engine ready for read benchmarks.
func populateAndFlush(b *testing.B, dir string, numKeys int, valueSize int) *LSMEngine {
	b.Helper()

	engine, err := NewLSMEngineWithConfig(dir, prodConfig())
	if err != nil {
		b.Fatal(err)
	}

	value := make([]byte, valueSize)
	crand.Read(value)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("k:%09d", i) // fixed-width keys for even distribution
		// Vary value slightly so compression isn't unrealistically good
		copy(value[:8], []byte(fmt.Sprintf("%08d", i)))
		if err := engine.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}

	// CRITICAL: Flush everything to SSTables so reads go to disk
	if err := engine.flushMemTable(); err != nil {
		b.Fatal(err)
	}

	// Clear the key cache so the first read is cold
	engine.cache = newKeyCache(prodConfig().KeyCacheSize)

	return engine
}

// zipfianKey returns a key following Zipfian distribution (power law).
// A few keys are very hot, most are cold — like real production traffic.
func zipfianKey(rng *rand.Rand, numKeys int) string {
	// Zipfian: P(k) ∝ 1/k^s where s=0.99 (YCSB default)
	// Approximation using inverse CDF
	u := rng.Float64()
	k := int(math.Floor(math.Pow(float64(numKeys), u)))
	if k >= numKeys {
		k = numKeys - 1
	}
	return fmt.Sprintf("k:%09d", k)
}

// =============================================================================
// BENCHMARK: Point Reads from SSTables (the real test)
// =============================================================================

// BenchmarkProd_Get_Cold_SSTable reads keys that are definitely in SSTables.
// This is the TRUE read performance — not MemTable speed.
func BenchmarkProd_Get_Cold_SSTable(b *testing.B) {
	const numKeys = 100_000  // ~100K keys
	const valueSize = 256    // 256B values → ~25MB on disk

	dir, err := os.MkdirTemp("", "prod-bench-cold-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine := populateAndFlush(b, dir, numKeys, valueSize)
	defer engine.Close()

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// Random key — cache will miss for most since cache is tiny (1000)
		key := fmt.Sprintf("k:%09d", rng.Intn(numKeys))
		_, err := engine.Get(key)
		if err != nil && err != storage.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

// BenchmarkProd_Get_Zipfian_SSTable uses realistic access pattern.
// Hot keys hit cache, cold keys hit SSTables — mixed like production.
func BenchmarkProd_Get_Zipfian_SSTable(b *testing.B) {
	const numKeys = 100_000
	const valueSize = 256

	dir, err := os.MkdirTemp("", "prod-bench-zipf-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine := populateAndFlush(b, dir, numKeys, valueSize)
	defer engine.Close()

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := zipfianKey(rng, numKeys)
		engine.Get(key)
	}
}

// =============================================================================
// BENCHMARK: Writes that trigger real flushes and compaction
// =============================================================================

// BenchmarkProd_Put_WithFlush writes enough data to trigger multiple
// MemTable rotations and SSTable flushes — measuring real write throughput.
func BenchmarkProd_Put_WithFlush(b *testing.B) {
	dir, err := os.MkdirTemp("", "prod-bench-put-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngineWithConfig(dir, prodConfig())
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	value := make([]byte, 256)
	crand.Read(value)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k:%09d", i)
		copy(value[:8], []byte(fmt.Sprintf("%08d", i)))
		if err := engine.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkProd_Put_1KB_WithFlush tests with 1KB values (more realistic).
func BenchmarkProd_Put_1KB_WithFlush(b *testing.B) {
	dir, err := os.MkdirTemp("", "prod-bench-put1k-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngineWithConfig(dir, prodConfig())
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	value := make([]byte, 1024)
	crand.Read(value)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k:%09d", i)
		if err := engine.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

// =============================================================================
// BENCHMARK: Mixed Read-Write (YCSB-style workloads)
// =============================================================================

// BenchmarkProd_YCSB_A simulates YCSB Workload A: 50% read, 50% write.
// Real-world analog: Session store, user activity tracking.
func BenchmarkProd_YCSB_A(b *testing.B) {
	const numKeys = 100_000
	const valueSize = 256

	dir, err := os.MkdirTemp("", "prod-bench-ycsba-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine := populateAndFlush(b, dir, numKeys, valueSize)
	defer engine.Close()

	rng := rand.New(rand.NewSource(42))
	value := make([]byte, valueSize)
	crand.Read(value)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if rng.Float64() < 0.5 {
			// 50% reads (Zipfian)
			key := zipfianKey(rng, numKeys)
			engine.Get(key)
		} else {
			// 50% writes (new keys, so they don't just overwrite in MemTable)
			key := fmt.Sprintf("k:%09d", numKeys+i)
			engine.Put(key, value)
		}
	}
}

// BenchmarkProd_YCSB_B simulates YCSB Workload B: 95% read, 5% write.
// Real-world analog: Photo tagging, social media timeline.
func BenchmarkProd_YCSB_B(b *testing.B) {
	const numKeys = 100_000
	const valueSize = 256

	dir, err := os.MkdirTemp("", "prod-bench-ycsbb-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine := populateAndFlush(b, dir, numKeys, valueSize)
	defer engine.Close()

	rng := rand.New(rand.NewSource(42))
	value := make([]byte, valueSize)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if rng.Float64() < 0.95 {
			key := zipfianKey(rng, numKeys)
			engine.Get(key)
		} else {
			key := fmt.Sprintf("k:%09d", numKeys+i)
			engine.Put(key, value)
		}
	}
}

// BenchmarkProd_YCSB_C simulates YCSB Workload C: 100% read.
// Real-world analog: User profile cache, config lookups.
func BenchmarkProd_YCSB_C(b *testing.B) {
	const numKeys = 100_000
	const valueSize = 256

	dir, err := os.MkdirTemp("", "prod-bench-ycsbc-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine := populateAndFlush(b, dir, numKeys, valueSize)
	defer engine.Close()

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := zipfianKey(rng, numKeys)
		engine.Get(key)
	}
}

// BenchmarkProd_YCSB_F simulates YCSB Workload F: Read-Modify-Write.
// Real-world analog: User counters, inventory updates.
func BenchmarkProd_YCSB_F(b *testing.B) {
	const numKeys = 100_000
	const valueSize = 256

	dir, err := os.MkdirTemp("", "prod-bench-ycsbf-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine := populateAndFlush(b, dir, numKeys, valueSize)
	defer engine.Close()

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := zipfianKey(rng, numKeys)
		// Read
		entry, err := engine.Get(key)
		if err != nil {
			continue
		}
		// Modify + Write back
		copy(entry.Value[:8], []byte(fmt.Sprintf("%08d", i)))
		engine.Put(key, entry.Value)
	}
}

// =============================================================================
// BENCHMARK: Concurrent with SSTables (realistic multi-goroutine)
// =============================================================================

// BenchmarkProd_Concurrent_YCSB_B measures parallel 95/5 read/write.
func BenchmarkProd_Concurrent_YCSB_B(b *testing.B) {
	const numKeys = 200_000
	const valueSize = 256

	dir, err := os.MkdirTemp("", "prod-bench-conc-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine := populateAndFlush(b, dir, numKeys, valueSize)
	defer engine.Close()

	value := make([]byte, valueSize)
	crand.Read(value)
	var writeCounter atomic.Int64

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			if rng.Float64() < 0.95 {
				key := zipfianKey(rng, numKeys)
				engine.Get(key)
			} else {
				wc := writeCounter.Add(1)
				key := fmt.Sprintf("w:%012d", wc)
				engine.Put(key, value)
			}
		}
	})
}

// =============================================================================
// BENCHMARK: Latency Percentiles (p50, p95, p99)
// =============================================================================

// BenchmarkProd_ReadLatency measures actual latency distribution.
// Run with: go test -bench=BenchmarkProd_ReadLatency -benchtime=5s -v
func BenchmarkProd_ReadLatency(b *testing.B) {
	const numKeys = 100_000
	const valueSize = 256
	const sampleSize = 50_000

	dir, err := os.MkdirTemp("", "prod-bench-lat-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine := populateAndFlush(b, dir, numKeys, valueSize)
	defer engine.Close()

	rng := rand.New(rand.NewSource(42))

	// Collect latency samples
	latencies := make([]time.Duration, 0, sampleSize)
	var mu sync.Mutex

	b.ResetTimer()
	for i := 0; i < b.N && i < sampleSize; i++ {
		key := fmt.Sprintf("k:%09d", rng.Intn(numKeys))
		start := time.Now()
		engine.Get(key)
		elapsed := time.Since(start)

		mu.Lock()
		latencies = append(latencies, elapsed)
		mu.Unlock()
	}
	b.StopTimer()

	if len(latencies) > 100 {
		// Sort for percentile calculation
		sortDurations(latencies)
		p50 := latencies[len(latencies)*50/100]
		p95 := latencies[len(latencies)*95/100]
		p99 := latencies[len(latencies)*99/100]
		b.ReportMetric(float64(p50.Microseconds()), "p50_µs")
		b.ReportMetric(float64(p95.Microseconds()), "p95_µs")
		b.ReportMetric(float64(p99.Microseconds()), "p99_µs")
	}
}

// BenchmarkProd_WriteLatency measures write latency with real flushes.
func BenchmarkProd_WriteLatency(b *testing.B) {
	const valueSize = 256
	const sampleSize = 50_000

	dir, err := os.MkdirTemp("", "prod-bench-wlat-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngineWithConfig(dir, prodConfig())
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	value := make([]byte, valueSize)
	crand.Read(value)

	latencies := make([]time.Duration, 0, sampleSize)

	b.ResetTimer()
	for i := 0; i < b.N && i < sampleSize; i++ {
		key := fmt.Sprintf("k:%09d", i)
		start := time.Now()
		engine.Put(key, value)
		elapsed := time.Since(start)
		latencies = append(latencies, elapsed)
	}
	b.StopTimer()

	if len(latencies) > 100 {
		sortDurations(latencies)
		p50 := latencies[len(latencies)*50/100]
		p95 := latencies[len(latencies)*95/100]
		p99 := latencies[len(latencies)*99/100]
		b.ReportMetric(float64(p50.Microseconds()), "p50_µs")
		b.ReportMetric(float64(p95.Microseconds()), "p95_µs")
		b.ReportMetric(float64(p99.Microseconds()), "p99_µs")
	}
}

// =============================================================================
// BENCHMARK: Large Dataset (stress test)
// =============================================================================

// BenchmarkProd_LargeDataset_500K populates 500K keys (125MB+), forces
// flush, then measures read throughput from SSTables across multiple levels.
func BenchmarkProd_LargeDataset_500K(b *testing.B) {
	const numKeys = 500_000
	const valueSize = 256

	dir, err := os.MkdirTemp("", "prod-bench-large-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine := populateAndFlush(b, dir, numKeys, valueSize)
	defer engine.Close()

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k:%09d", rng.Intn(numKeys))
		engine.Get(key)
	}
}

// =============================================================================
// Helpers
// =============================================================================

func sortDurations(d []time.Duration) {
	// Simple insertion sort — fine for 50K elements
	for i := 1; i < len(d); i++ {
		key := d[i]
		j := i - 1
		for j >= 0 && d[j] > key {
			d[j+1] = d[j]
			j--
		}
		d[j+1] = key
	}
}
