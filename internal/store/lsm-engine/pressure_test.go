package lsmengine

import (
	"fmt"
	"os"
	"sync"
	"testing"

	storage "github.com/DevLikhith5/kasoku/internal/store"
)

func Benchmark_Pressure_ReadHotKeys(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-pressure-hot-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	const hotKeys = 100
	const totalKeys = 10000

	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := engine.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%hotKeys)
		_, err := engine.Get(key)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Pressure_ReadColdKeys(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-pressure-cold-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	const totalKeys = 10000
	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := engine.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%totalKeys)
		_, err := engine.Get(key)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Pressure_MixedReadWrite(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-pressure-mixed-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngineWithConfig(dir, LSMConfig{
		MemTableSize: 64 * 1024 * 1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	const prefill = 10000
	for i := 0; i < prefill; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		engine.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%10 < 8 {
			key := fmt.Sprintf("key-%d", i%prefill)
			_, err := engine.Get(key)
			if err != nil {
				b.Fatal(err)
			}
		} else {
			key := fmt.Sprintf("key-%d", prefill+i)
			value := []byte(fmt.Sprintf("value-%d", i))
			if err := engine.Put(key, value); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func Benchmark_Pressure_ConcurrentReads(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-pressure-concurrent-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	const totalKeys = 10000
	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		engine.Put(key, value)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i%totalKeys)
			_, err := engine.Get(key)
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

func Benchmark_Pressure_ConcurrentMixed(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-pressure-concmixed-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngineWithConfig(dir, LSMConfig{
		MemTableSize: 64 * 1024 * 1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	const prefill = 5000
	for i := 0; i < prefill; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		engine.Put(key, value)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%5 == 0 {
				key := fmt.Sprintf("key-%d", prefill+i)
				value := []byte(fmt.Sprintf("value-%d", i))
				engine.Put(key, value)
			} else {
				key := fmt.Sprintf("key-%d", i%prefill)
				engine.Get(key)
			}
			i++
		}
	})
}

func Benchmark_Pressure_ReadMiss(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-pressure-miss-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		engine.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("nonexistent-%d", i)
		_, err := engine.Get(key)
		if err != storage.ErrKeyNotFound {
			b.Fatalf("expected ErrKeyNotFound, got %v", err)
		}
	}
}

func Benchmark_Pressure_ScanPrefix(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-pressure-scan-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

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

func Benchmark_Pressure_WriteReadCycle(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-pressure-cycle-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngineWithConfig(dir, LSMConfig{
		MemTableSize: 256 * 1024,
	})
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
		_, err := engine.Get(key)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Pressure_OverwriteRead(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-pressure-overwrite-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	const hotKeys = 500
	for i := 0; i < hotKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("initial-%d", i))
		engine.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%hotKeys)
		if i%4 == 0 {
			value := []byte(fmt.Sprintf("updated-%d", i))
			engine.Put(key, value)
		} else {
			_, err := engine.Get(key)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func Benchmark_Pressure_LargeValues(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-pressure-large-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	const valueSize = 4096
	const numKeys = 1000
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("large-%d", i)
		copy(value, []byte(fmt.Sprintf("v-%d:", i)))
		if err := engine.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("large-%d", i%numKeys)
		_, err := engine.Get(key)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Pressure_ConcurrentReadStress(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-pressure-rstress-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine, err := NewLSMEngineWithConfig(dir, LSMConfig{
		MemTableSize: 64 * 1024 * 1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	const totalKeys = 50000
	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		engine.Put(key, value)
	}

	var mu sync.Mutex
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i%totalKeys)
			_, err := engine.Get(key)
			if err != nil && err != storage.ErrKeyNotFound {
				b.Fatal(err)
			}
			i++
			if i%100 == 0 {
				mu.Lock()
				wkey := fmt.Sprintf("write-%d", i)
				engine.Put(wkey, []byte("write-value"))
				mu.Unlock()
			}
		}
	})
}
