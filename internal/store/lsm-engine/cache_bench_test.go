package lsmengine

import (
	"fmt"
	"testing"

	storage "github.com/DevLikhith5/kasoku/internal/store"
)

func Benchmark_KeyCache_Hit(b *testing.B) {
	cache := newKeyCache(10000)

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		entry := storage.Entry{Key: key, Value: []byte("value")}
		cache.Put(key, entry, true)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%100)
		cache.Get(key)
	}
}

func Benchmark_KeyCache_Miss(b *testing.B) {
	cache := newKeyCache(10000)

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		entry := storage.Entry{Key: key, Value: []byte("value")}
		cache.Put(key, entry, true)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("missing-%d", i)
		cache.Get(key)
	}
}

func Benchmark_KeyCache_Put(b *testing.B) {
	cache := newKeyCache(10000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		entry := storage.Entry{Key: key, Value: []byte("value")}
		cache.Put(key, entry, true)
	}
}

func Benchmark_KeyCache_Invalidate(b *testing.B) {
	cache := newKeyCache(10000)

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		entry := storage.Entry{Key: key, Value: []byte("value")}
		cache.Put(key, entry, true)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%100)
		cache.Invalidate(key)
	}
}
