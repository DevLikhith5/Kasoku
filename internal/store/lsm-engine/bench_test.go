package lsmengine

import (
	crand "crypto/rand"
	"fmt"
	"math/rand"
	"os"
	"testing"

	storage "github.com/DevLikhith5/kasoku/internal/store"
)

// Benchmark_Put_Sequential benchmarks sequential writes
func Benchmark_Put_Sequential(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-put-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := engine.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark_Put_Sequential_1KB benchmarks writes with 1KB values
func Benchmark_Put_Sequential_1KB(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-put-1kb-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	value := make([]byte, 1024)
	crand.Read(value)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		if err := engine.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark_Put_Sequential_10KB benchmarks writes with 10KB values
func Benchmark_Put_Sequential_10KB(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-put-10kb-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	value := make([]byte, 10*1024)
	crand.Read(value)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		if err := engine.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark_Get_Sequential benchmarks sequential reads
func Benchmark_Get_Sequential(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-get-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		engine.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%10000)
		_, err := engine.Get(key)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark_Get_Random benchmarks random reads
func Benchmark_Get_Random(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-get-rand-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		engine.Put(key, value)
	}

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", rng.Intn(10000))
		_, err := engine.Get(key)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark_Get_Miss benchmarks reads for non-existent keys
func Benchmark_Get_Miss(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-get-miss-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		engine.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("missing-%d", i)
		engine.Get(key)
	}
}

// Benchmark_Delete benchmarks deletes
func Benchmark_Delete(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-delete-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	// Pre-populate
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		engine.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		if err := engine.Delete(key); err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark_MixedReadWrite benchmarks 70% reads, 30% writes
func Benchmark_MixedReadWrite(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-mixed-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	// Pre-populate
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		engine.Put(key, value)
	}

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if rng.Float64() < 0.7 {
			// 70% reads
			key := fmt.Sprintf("key-%d", rng.Intn(5000))
			engine.Get(key)
		} else {
			// 30% writes
			key := fmt.Sprintf("key-%d", 5000+i)
			value := []byte(fmt.Sprintf("value-%d", i))
			engine.Put(key, value)
		}
	}
}

// Benchmark_Scan benchmarks prefix scans
func Benchmark_Scan(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-scan-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	// Pre-populate with prefixes
	for i := 0; i < 1000; i++ {
		for j := 0; j < 10; j++ {
			key := fmt.Sprintf("user:%d:item:%d", i, j)
			value := []byte(fmt.Sprintf("value-%d-%d", i, j))
			engine.Put(key, value)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prefix := fmt.Sprintf("user:%d:", i%1000)
		_, err := engine.Scan(prefix)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark_Put_Concurrent benchmarks concurrent writes
func Benchmark_Put_Concurrent(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-put-concurrent-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d-%d", i, i)
			value := []byte(fmt.Sprintf("value-%d", i))
			if err := engine.Put(key, value); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// Benchmark_Get_Concurrent benchmarks concurrent reads
func Benchmark_Get_Concurrent(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-get-concurrent-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		engine.Put(key, value)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i%10000)
			engine.Get(key)
			i++
		}
	})
}

// Benchmark_MemTable_Put benchmarks memtable-only writes
func Benchmark_MemTable_Put(b *testing.B) {
	memtable := NewMemTable(64 * 1024 * 1024) // 64MB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		memtable.Put(storage.Entry{Key: key, Value: value})
	}
}

// Benchmark_MemTable_Get benchmarks memtable-only reads
func Benchmark_MemTable_Get(b *testing.B) {
	memtable := NewMemTable(64 * 1024 * 1024)

	// Pre-populate
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		memtable.Put(storage.Entry{Key: key, Value: value})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%100000)
		memtable.Get(key)
	}
}

// Benchmark_SSTable_Write benchmarks SSTable creation
func Benchmark_SSTable_Write(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-sstable-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	entries := make([]storage.Entry, 10000)
	for i := 0; i < 10000; i++ {
		entries[i] = storage.Entry{
			Key:   fmt.Sprintf("key-%d", i),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
	}

	for i := 0; b.Loop(); i++ {
		path := fmt.Sprintf("%s/test-%d.sst", dir, i)
		writer, err := NewSSTableWriter(path, 10000, 0.01)
		if err != nil {
			b.Fatal(err)
		}
		for _, entry := range entries {
			writer.WriteEntry(entry)
		}
		writer.Finalize()
		writer.Close()
	}
}

// Benchmark_Keys benchmarks key enumeration
func Benchmark_Keys(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-keys-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		engine.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := engine.Keys()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark_Stats benchmarks stats collection
func Benchmark_Stats(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-stats-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		engine.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.Stats()
	}
}
