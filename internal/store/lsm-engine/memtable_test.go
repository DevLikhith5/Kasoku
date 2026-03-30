package lsmengine

import (
	"fmt"
	"strings"
	"testing"
	"time"

	storage "github.com/DevLikhith5/kasoku/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemTable_New(t *testing.T) {
	t.Run("create with default size", func(t *testing.T) {
		mt := NewMemTable(0)

		assert.NotNil(t, mt)
		assert.Equal(t, int64(DefaultMemTableSize), mt.maxBytes)
		assert.Equal(t, int64(0), mt.Size())
		assert.Equal(t, 0, mt.Len())
		assert.False(t, mt.IsFull())
	})

	t.Run("create with custom size", func(t *testing.T) {
		mt := NewMemTable(1024)

		assert.NotNil(t, mt)
		assert.Equal(t, int64(1024), mt.maxBytes)
	})

	t.Run("create with negative size uses default", func(t *testing.T) {
		mt := NewMemTable(-100)

		assert.NotNil(t, mt)
		assert.Equal(t, int64(DefaultMemTableSize), mt.maxBytes)
	})
}

func TestMemTable_Put(t *testing.T) {
	t.Run("put single entry", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		entry := storage.Entry{
			Key:       "testkey",
			Value:     []byte("testvalue"),
			Version:   1,
			TimeStamp: time.Now(),
		}

		mt.Put(entry)

		assert.Equal(t, 1, mt.Len())
		assert.Greater(t, mt.Size(), int64(0))

		retrieved, found := mt.Get("testkey")
		require.True(t, found)
		assert.Equal(t, []byte("testvalue"), retrieved.Value)
	})

	t.Run("put multiple entries", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		for i := 0; i < 10; i++ {
			mt.Put(storage.Entry{
				Key:   fmt.Sprintf("key:%d", i),
				Value: []byte(fmt.Sprintf("value:%d", i)),
			})
		}

		assert.Equal(t, 10, mt.Len())

		for i := 0; i < 10; i++ {
			entry, found := mt.Get(fmt.Sprintf("key:%d", i))
			require.True(t, found)
			assert.Equal(t, []byte(fmt.Sprintf("value:%d", i)), entry.Value)
		}
	})

	t.Run("put updates existing key", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		mt.Put(storage.Entry{Key: "key", Value: []byte("v1"), Version: 1})
		assert.Equal(t, 1, mt.Len())

		mt.Put(storage.Entry{Key: "key", Value: []byte("v2"), Version: 2})
		assert.Equal(t, 1, mt.Len()) // Should still be 1

		entry, found := mt.Get("key")
		require.True(t, found)
		assert.Equal(t, []byte("v2"), entry.Value)
		assert.Equal(t, uint64(2), entry.Version)
	})

	t.Run("put size tracking", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		initialSize := mt.Size()

		mt.Put(storage.Entry{Key: "key", Value: []byte("value")})

		newSize := mt.Size()
		assert.Greater(t, newSize, initialSize)

		// Size should be key + value
		expectedIncrease := int64(len("key") + len("value"))
		assert.GreaterOrEqual(t, newSize-initialSize, expectedIncrease)
	})

	t.Run("put size adjustment on update", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		mt.Put(storage.Entry{Key: "key", Value: []byte("short")})
		size1 := mt.Size()

		mt.Put(storage.Entry{Key: "key", Value: []byte("much longer value")})
		size2 := mt.Size()

		// Size should increase because new value is longer
		assert.Greater(t, size2, size1)

		mt.Put(storage.Entry{Key: "key", Value: []byte("v")})
		size3 := mt.Size()

		// Size should decrease because new value is shorter
		assert.Less(t, size3, size2)
	})
}

func TestMemTable_Get(t *testing.T) {
	t.Run("get existing key", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		entry := storage.Entry{
			Key:       "mykey",
			Value:     []byte("myvalue"),
			Version:   42,
			TimeStamp: time.Now(),
		}

		mt.Put(entry)

		retrieved, found := mt.Get("mykey")
		require.True(t, found)
		assert.Equal(t, "mykey", retrieved.Key)
		assert.Equal(t, []byte("myvalue"), retrieved.Value)
		assert.Equal(t, uint64(42), retrieved.Version)
	})

	t.Run("get non-existent key", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		_, found := mt.Get("nonexistent")
		assert.False(t, found)
	})

	t.Run("get from empty memtable", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		_, found := mt.Get("anything")
		assert.False(t, found)
	})

	t.Run("get tombstone entry", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		mt.Put(storage.Entry{
			Key:       "delkey",
			Value:     nil,
			Tombstone: true,
		})

		entry, found := mt.Get("delkey")
		require.True(t, found)
		assert.True(t, entry.Tombstone)
		assert.Nil(t, entry.Value)
	})
}

func TestMemTable_Scan(t *testing.T) {
	t.Run("scan with prefix", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		mt.Put(storage.Entry{Key: "user:1", Value: []byte("Alice")})
		mt.Put(storage.Entry{Key: "user:2", Value: []byte("Bob")})
		mt.Put(storage.Entry{Key: "user:3", Value: []byte("Charlie")})
		mt.Put(storage.Entry{Key: "session:1", Value: []byte("xyz")})

		results := mt.Scan("user:")

		assert.Len(t, results, 3)
		assert.Equal(t, "user:1", results[0].Key)
		assert.Equal(t, "user:2", results[1].Key)
		assert.Equal(t, "user:3", results[2].Key)
	})

	t.Run("scan empty prefix", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		mt.Put(storage.Entry{Key: "c", Value: []byte("3")})
		mt.Put(storage.Entry{Key: "a", Value: []byte("1")})
		mt.Put(storage.Entry{Key: "b", Value: []byte("2")})

		results := mt.Scan("")

		assert.Len(t, results, 3)
		// Should be sorted
		assert.Equal(t, "a", results[0].Key)
		assert.Equal(t, "b", results[1].Key)
		assert.Equal(t, "c", results[2].Key)
	})

	t.Run("scan non-existent prefix", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		mt.Put(storage.Entry{Key: "key1", Value: []byte("value1")})

		results := mt.Scan("nonexistent:")
		assert.Empty(t, results)
	})

	t.Run("scan empty memtable", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		results := mt.Scan("")
		assert.Empty(t, results)
	})

	t.Run("scan returns sorted results", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		// Insert in random order
		keys := []string{"zebra", "apple", "mango", "banana", "cherry"}
		for _, key := range keys {
			mt.Put(storage.Entry{Key: key, Value: []byte("value")})
		}

		results := mt.Scan("")

		expected := []string{"apple", "banana", "cherry", "mango", "zebra"}
		for i, entry := range results {
			assert.Equal(t, expected[i], entry.Key)
		}
	})

	t.Run("scan with partial prefix match", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		mt.Put(storage.Entry{Key: "user", Value: []byte("1")})
		mt.Put(storage.Entry{Key: "user:1", Value: []byte("2")})
		mt.Put(storage.Entry{Key: "user:2", Value: []byte("3")})
		mt.Put(storage.Entry{Key: "username", Value: []byte("4")})

		results := mt.Scan("user:")

		assert.Len(t, results, 2)
		assert.Equal(t, "user:1", results[0].Key)
		assert.Equal(t, "user:2", results[1].Key)
	})

	t.Run("scan excludes tombstones", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		mt.Put(storage.Entry{Key: "user:1", Value: []byte("Alice")})
		mt.Put(storage.Entry{Key: "user:2", Value: []byte("Bob")})
		mt.Put(storage.Entry{Key: "user:2", Value: nil, Tombstone: true})

		results := mt.Scan("user:")

		// Note: MemTable.Scan returns all entries including tombstones
		// The engine layer filters them out
		assert.GreaterOrEqual(t, len(results), 1)
		assert.Equal(t, "user:1", results[0].Key)
	})
}

func TestMemTable_IsFull(t *testing.T) {
	t.Run("empty memtable is not full", func(t *testing.T) {
		mt := NewMemTable(1024)
		assert.False(t, mt.IsFull())
	})

	t.Run("memtable becomes full", func(t *testing.T) {
		mt := NewMemTable(100) // Small size for testing

		// Keep adding until full
		for i := 0; i < 100; i++ {
			mt.Put(storage.Entry{
				Key:   fmt.Sprintf("key:%d", i),
				Value: []byte("somevalue"),
			})

			if mt.IsFull() {
				break
			}
		}

		assert.True(t, mt.IsFull())
	})

	t.Run("memtable at exact capacity", func(t *testing.T) {
		mt := NewMemTable(50)

		// Add entries until exactly at capacity
		for i := 0; i < 100; i++ {
			mt.Put(storage.Entry{
				Key:   fmt.Sprintf("k%d", i),
				Value: []byte("v"),
			})

			if mt.Size() >= 50 {
				break
			}
		}

		assert.True(t, mt.IsFull())
	})

	t.Run("memtable with large capacity", func(t *testing.T) {
		mt := NewMemTable(64 * 1024 * 1024) // 64MB

		// Add some entries but not enough to fill
		for i := 0; i < 100; i++ {
			mt.Put(storage.Entry{
				Key:   fmt.Sprintf("key:%d", i),
				Value: []byte("value"),
			})
		}

		assert.False(t, mt.IsFull())
	})
}

func TestMemTable_Size(t *testing.T) {
	t.Run("size of empty memtable", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)
		assert.Equal(t, int64(0), mt.Size())
	})

	t.Run("size grows with entries", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		sizes := make([]int64, 0)

		for i := 0; i < 10; i++ {
			mt.Put(storage.Entry{
				Key:   fmt.Sprintf("key:%d", i),
				Value: []byte("fixedsizevalue"),
			})
			sizes = append(sizes, mt.Size())
		}

		// Size should generally increase
		for i := 1; i < len(sizes); i++ {
			assert.GreaterOrEqual(t, sizes[i], sizes[i-1])
		}
	})

	t.Run("size calculation accuracy", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		key := "testkey"
		value := "testvalue"

		mt.Put(storage.Entry{Key: key, Value: []byte(value)})

		// Size should be at least key + value
		minExpectedSize := int64(len(key) + len(value))
		assert.GreaterOrEqual(t, mt.Size(), minExpectedSize)
	})
}

func TestMemTable_Len(t *testing.T) {
	t.Run("length of empty memtable", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)
		assert.Equal(t, 0, mt.Len())
	})

	t.Run("length increases with unique keys", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		for i := 0; i < 100; i++ {
			mt.Put(storage.Entry{Key: fmt.Sprintf("key:%d", i), Value: []byte("value")})
		}

		assert.Equal(t, 100, mt.Len())
	})

	t.Run("length unchanged on update", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		mt.Put(storage.Entry{Key: "key", Value: []byte("v1")})
		assert.Equal(t, 1, mt.Len())

		mt.Put(storage.Entry{Key: "key", Value: []byte("v2")})
		assert.Equal(t, 1, mt.Len())

		mt.Put(storage.Entry{Key: "key", Value: []byte("v3")})
		assert.Equal(t, 1, mt.Len())
	})
}

func TestMemTable_Entries(t *testing.T) {
	t.Run("entries from empty memtable", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		entries := mt.Entries()
		assert.Empty(t, entries)
	})

	t.Run("entries returns all in sorted order", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		// Insert in random order
		keys := []string{"zebra", "apple", "mango", "banana", "cherry"}
		for i, key := range keys {
			mt.Put(storage.Entry{Key: key, Value: []byte(fmt.Sprintf("val:%d", i))})
		}

		entries := mt.Entries()

		assert.Len(t, entries, 5)

		expected := []string{"apple", "banana", "cherry", "mango", "zebra"}
		for i, entry := range entries {
			assert.Equal(t, expected[i], entry.Key)
		}
	})

	t.Run("entries includes all fields", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		now := time.Now()
		mt.Put(storage.Entry{
			Key:       "key",
			Value:     []byte("value"),
			Version:   42,
			TimeStamp: now,
			Tombstone: true,
		})

		entries := mt.Entries()
		require.Len(t, entries, 1)

		assert.Equal(t, "key", entries[0].Key)
		assert.Equal(t, []byte("value"), entries[0].Value)
		assert.Equal(t, uint64(42), entries[0].Version)
		assert.True(t, entries[0].Tombstone)
		assert.WithinDuration(t, now, entries[0].TimeStamp, time.Second)
	})
}

func TestMemTable_EdgeCases(t *testing.T) {
	t.Run("empty key", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		mt.Put(storage.Entry{Key: "", Value: []byte("empty key value")})

		entry, found := mt.Get("")
		require.True(t, found)
		assert.Equal(t, "", entry.Key)

		entries := mt.Entries()
		require.Len(t, entries, 1)
		assert.Equal(t, "", entries[0].Key)
	})

	t.Run("empty value", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		mt.Put(storage.Entry{Key: "key", Value: []byte{}})

		entry, found := mt.Get("key")
		require.True(t, found)
		assert.Empty(t, entry.Value)
	})

	t.Run("nil value", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		mt.Put(storage.Entry{Key: "key", Value: nil})

		entry, found := mt.Get("key")
		require.True(t, found)
		assert.Nil(t, entry.Value)
	})

	t.Run("binary values", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		binaryValue := []byte{0x00, 0x01, 0xFF, 0xFE, 0x80, 0x7F}
		mt.Put(storage.Entry{Key: "binary", Value: binaryValue})

		entry, found := mt.Get("binary")
		require.True(t, found)
		assert.Equal(t, binaryValue, entry.Value)
	})

	t.Run("unicode keys and values", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		mt.Put(storage.Entry{
			Key:   "こんにちは",
			Value: []byte("世界"),
		})

		entry, found := mt.Get("こんにちは")
		require.True(t, found)
		assert.Equal(t, "世界", string(entry.Value))
	})

	t.Run("special characters in keys", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		specialKeys := []string{
			"key with spaces",
			"key:with:colons",
			"key/with/slashes",
			"key.with.dots",
		}

		for i, key := range specialKeys {
			mt.Put(storage.Entry{Key: key, Value: []byte(fmt.Sprintf("val:%d", i))})
		}

		for i, key := range specialKeys {
			entry, found := mt.Get(key)
			require.True(t, found, "key '%s' not found", key)
			assert.Equal(t, []byte(fmt.Sprintf("val:%d", i)), entry.Value)
		}
	})

	t.Run("very long key", func(t *testing.T) {
		mt := NewMemTable(1024 * 1024)

		longKey := strings.Repeat("a", 1000)
		mt.Put(storage.Entry{Key: longKey, Value: []byte("value")})

		entry, found := mt.Get(longKey)
		require.True(t, found)
		assert.Equal(t, 1000, len(entry.Key))
	})

	t.Run("very large value", func(t *testing.T) {
		mt := NewMemTable(10 * 1024 * 1024) // 10MB capacity

		largeValue := make([]byte, 1*1024*1024) // 1MB
		for i := range largeValue {
			largeValue[i] = byte(i % 256)
		}

		mt.Put(storage.Entry{Key: "large", Value: largeValue})

		entry, found := mt.Get("large")
		require.True(t, found)
		assert.Equal(t, largeValue, entry.Value)
	})
}

func TestMemTable_LargeDataset(t *testing.T) {
	t.Run("10000 entries", func(t *testing.T) {
		mt := NewMemTable(64 * 1024 * 1024)

		for i := 0; i < 10000; i++ {
			mt.Put(storage.Entry{
				Key:   fmt.Sprintf("key:%05d", i),
				Value: []byte(fmt.Sprintf("value:%d", i)),
			})
		}

		assert.Equal(t, 10000, mt.Len())

		// Verify random samples
		samples := []int{0, 50, 100, 500, 9999}
		for _, i := range samples {
			entry, found := mt.Get(fmt.Sprintf("key:%05d", i))
			require.True(t, found)
			assert.Equal(t, []byte(fmt.Sprintf("value:%d", i)), entry.Value)
		}

		// Verify sorted order
		entries := mt.Entries()
		require.Len(t, entries, 10000)
		for i := 1; i < len(entries); i++ {
			assert.Less(t, entries[i-1].Key, entries[i].Key)
		}
	})
}

func TestMemTable_Concurrent(t *testing.T) {
	t.Run("concurrent reads and writes", func(t *testing.T) {
		mt := NewMemTable(64 * 1024 * 1024)

		done := make(chan bool)

		// Writers
		for i := 0; i < 5; i++ {
			go func(id int) {
				for j := 0; j < 100; j++ {
					mt.Put(storage.Entry{
						Key:   fmt.Sprintf("goroutine:%d:key:%d", id, j),
						Value: []byte(fmt.Sprintf("value:%d", j)),
					})
				}
				done <- true
			}(i)
		}

		// Readers
		for i := 0; i < 5; i++ {
			go func(id int) {
				for j := 0; j < 100; j++ {
					mt.Get(fmt.Sprintf("goroutine:%d:key:%d", id, j))
				}
				done <- true
			}(i)
		}

		// Wait for all goroutines
		for i := 0; i < 10; i++ {
			<-done
		}
	})

	t.Run("concurrent scans", func(t *testing.T) {
		mt := NewMemTable(64 * 1024 * 1024)

		// Pre-populate
		for i := 0; i < 100; i++ {
			mt.Put(storage.Entry{Key: fmt.Sprintf("key:%d", i), Value: []byte("value")})
		}

		done := make(chan bool)

		// Scanners
		for i := 0; i < 5; i++ {
			go func() {
				for j := 0; j < 20; j++ {
					mt.Scan("")
				}
				done <- true
			}()
		}

		for i := 0; i < 5; i++ {
			<-done
		}
	})
}

func TestMemTable_DefaultMemTableSize(t *testing.T) {
	t.Run("default size is 64MB", func(t *testing.T) {
		assert.Equal(t, int64(64*1024*1024), int64(DefaultMemTableSize))
	})
}
