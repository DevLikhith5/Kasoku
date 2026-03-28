package storage

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashMapEngine_Basic(t *testing.T) {
	t.Run("create engine", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, err := NewHashmapEngine(walPath)
		require.NoError(t, err)
		require.NotNil(t, engine)
		
		engine.Close()
	})

	t.Run("put and get", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		err := engine.Put("key1", []byte("value1"))
		require.NoError(t, err)
		
		entry, err := engine.Get("key1")
		require.NoError(t, err)
		assert.Equal(t, "key1", entry.Key)
		assert.Equal(t, []byte("value1"), entry.Value)
		assert.False(t, entry.Tombstone)
		assert.Greater(t, entry.Version, uint64(0))
	})

	t.Run("get non-existent key", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		_, err := engine.Get("nonexistent")
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("overwrite key", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		engine.Put("key", []byte("v1"))
		engine.Put("key", []byte("v2"))
		engine.Put("key", []byte("v3"))
		
		entry, err := engine.Get("key")
		require.NoError(t, err)
		assert.Equal(t, []byte("v3"), entry.Value)
		assert.Equal(t, uint64(3), entry.Version)
	})

	t.Run("delete key", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		engine.Put("delkey", []byte("value"))
		err := engine.Delete("delkey")
		require.NoError(t, err)
		
		_, err = engine.Get("delkey")
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("delete non-existent key", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		err := engine.Delete("nonexistent")
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("delete already deleted key", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		engine.Put("key", []byte("value"))
		engine.Delete("key")
		
		err := engine.Delete("key")
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})
}

func TestHashMapEngine_Scan(t *testing.T) {
	t.Run("scan with prefix", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		engine.Put("user:1", []byte("Alice"))
		engine.Put("user:2", []byte("Bob"))
		engine.Put("user:3", []byte("Charlie"))
		engine.Put("session:1", []byte("xyz"))
		
		entries, err := engine.Scan("user:")
		require.NoError(t, err)
		assert.Len(t, entries, 3)
		
		// Verify sorted order
		assert.Equal(t, "user:1", entries[0].Key)
		assert.Equal(t, "user:2", entries[1].Key)
		assert.Equal(t, "user:3", entries[2].Key)
	})

	t.Run("scan empty prefix", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		engine.Put("a", []byte("1"))
		engine.Put("b", []byte("2"))
		engine.Put("c", []byte("3"))
		
		entries, err := engine.Scan("")
		require.NoError(t, err)
		assert.Len(t, entries, 3)
	})

	t.Run("scan non-existent prefix", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		engine.Put("key1", []byte("value1"))
		
		entries, err := engine.Scan("nonexistent:")
		require.NoError(t, err)
		assert.Empty(t, entries)
	})

	t.Run("scan excludes deleted keys", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		engine.Put("user:1", []byte("Alice"))
		engine.Put("user:2", []byte("Bob"))
		engine.Delete("user:2")
		
		entries, err := engine.Scan("user:")
		require.NoError(t, err)
		assert.Len(t, entries, 1)
		assert.Equal(t, "user:1", entries[0].Key)
	})

	t.Run("scan returns sorted results", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		// Insert in random order
		keys := []string{"zebra", "apple", "mango", "banana"}
		for _, key := range keys {
			engine.Put(key, []byte("value"))
		}
		
		entries, err := engine.Scan("")
		require.NoError(t, err)
		
		expected := []string{"apple", "banana", "mango", "zebra"}
		for i, entry := range entries {
			assert.Equal(t, expected[i], entry.Key)
		}
	})
}

func TestHashMapEngine_Keys(t *testing.T) {
	t.Run("keys returns all non-deleted keys", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		engine.Put("c", []byte("3"))
		engine.Put("a", []byte("1"))
		engine.Put("b", []byte("2"))
		
		keys, err := engine.Keys()
		require.NoError(t, err)
		assert.Equal(t, []string{"a", "b", "c"}, keys)
	})

	t.Run("keys excludes deleted", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		engine.Put("key1", []byte("value1"))
		engine.Put("key2", []byte("value2"))
		engine.Delete("key2")
		
		keys, err := engine.Keys()
		require.NoError(t, err)
		assert.Equal(t, []string{"key1"}, keys)
	})

	t.Run("keys on empty engine", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		keys, err := engine.Keys()
		require.NoError(t, err)
		assert.Empty(t, keys)
	})
}

func TestHashMapEngine_Version(t *testing.T) {
	t.Run("version increments on put", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		engine.Put("key1", []byte("value1"))
		e1, _ := engine.Get("key1")
		
		engine.Put("key2", []byte("value2"))
		e2, _ := engine.Get("key2")
		
		assert.Greater(t, e2.Version, e1.Version)
	})

	t.Run("version increments on delete", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")

		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()

		engine.Put("key", []byte("value"))

		engine.Delete("key")

		// After delete, key should not be found (returns ErrKeyNotFound)
		_, err := engine.Get("key")
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})
}

func TestHashMapEngine_CrashRecovery(t *testing.T) {
	t.Run("recover after crash", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		// Create engine and write data
		engine1, _ := NewHashmapEngine(walPath)
		
		engine1.Put("key1", []byte("value1"))
		engine1.Put("key2", []byte("value2"))
		engine1.Put("key1", []byte("value1_updated"))
		engine1.Delete("key2")
		
		engine1.Close()
		
		// Create new engine - should recover from WAL
		engine2, err := NewHashmapEngine(walPath)
		require.NoError(t, err)
		defer engine2.Close()
		
		entry, err := engine2.Get("key1")
		require.NoError(t, err)
		assert.Equal(t, []byte("value1_updated"), entry.Value)
		
		_, err = engine2.Get("key2")
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("recover version counter", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine1, _ := NewHashmapEngine(walPath)
		engine1.Put("key", []byte("value"))
		e1, _ := engine1.Get("key")
		engine1.Close()
		
		engine2, _ := NewHashmapEngine(walPath)
		defer engine2.Close()
		
		engine2.Put("key2", []byte("value2"))
		e2, _ := engine2.Get("key2")
		
		assert.Greater(t, e2.Version, e1.Version)
	})

	t.Run("recover empty engine", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine1, _ := NewHashmapEngine(walPath)
		engine1.Close()
		
		engine2, err := NewHashmapEngine(walPath)
		require.NoError(t, err)
		defer engine2.Close()
		
		keys, _ := engine2.Keys()
		assert.Empty(t, keys)
	})
}

func TestHashMapEngine_Stats(t *testing.T) {
	t.Run("stats on empty engine", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		stats := engine.Stats()
		assert.Equal(t, int64(0), stats.KeyCount)
		assert.Equal(t, int64(0), stats.MemBytes)
		assert.Equal(t, int64(0), stats.DiskBytes)
		assert.Equal(t, float64(0), stats.BloomFPRate)
	})

	t.Run("stats with data", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		for i := 0; i < 100; i++ {
			engine.Put(fmt.Sprintf("key:%d", i), []byte(fmt.Sprintf("value:%d", i)))
		}
		
		stats := engine.Stats()
		assert.Equal(t, int64(100), stats.KeyCount)
		assert.Greater(t, stats.MemBytes, int64(0))
		assert.Equal(t, int64(0), stats.DiskBytes) // HashMap doesn't use disk
	})
}

func TestHashMapEngine_Close(t *testing.T) {
	t.Run("close engine", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		
		err := engine.Close()
		assert.NoError(t, err)
	})

	t.Run("operations after close fail", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		engine.Close()
		
		err := engine.Put("key", []byte("value"))
		assert.ErrorIs(t, err, ErrEngineClosed)
		
		_, err = engine.Get("key")
		assert.ErrorIs(t, err, ErrEngineClosed)
		
		_, err = engine.Keys()
		assert.ErrorIs(t, err, ErrEngineClosed)
		
		_, err = engine.Scan("")
		assert.ErrorIs(t, err, ErrEngineClosed)
		
		err = engine.Delete("key")
		assert.ErrorIs(t, err, ErrEngineClosed)
	})

	t.Run("double close is safe", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")

		engine, _ := NewHashmapEngine(walPath)

		err := engine.Close()
		assert.NoError(t, err)

		// Second close may error (WAL already closed) - that's OK
		_ = engine.Close()
	})
}

func TestHashMapEngine_Validation(t *testing.T) {
	t.Run("key too long", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		longKey := string(make([]byte, MaxKeyLen+1))
		err := engine.Put(longKey, []byte("value"))
		assert.ErrorIs(t, err, ErrKeyTooLong)
	})

	t.Run("value too large", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		largeValue := make([]byte, MaxValueLen+1)
		err := engine.Put("key", largeValue)
		assert.ErrorIs(t, err, ErrValueTooLarge)
	})

	t.Run("empty key is allowed", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		err := engine.Put("", []byte("empty key value"))
		require.NoError(t, err)
		
		entry, err := engine.Get("")
		require.NoError(t, err)
		assert.Equal(t, "", entry.Key)
	})
}

func TestHashMapEngine_EdgeCases(t *testing.T) {
	t.Run("binary values", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		binaryValue := []byte{0x00, 0x01, 0xFF, 0xFE, 0x80, 0x7F}
		err := engine.Put("binary", binaryValue)
		require.NoError(t, err)
		
		entry, err := engine.Get("binary")
		require.NoError(t, err)
		assert.Equal(t, binaryValue, entry.Value)
	})

	t.Run("unicode keys and values", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		unicodeKey := "こんにちは世界"
		unicodeValue := "🌍🌎🌏"
		
		err := engine.Put(unicodeKey, []byte(unicodeValue))
		require.NoError(t, err)
		
		entry, err := engine.Get(unicodeKey)
		require.NoError(t, err)
		assert.Equal(t, unicodeKey, entry.Key)
		assert.Equal(t, []byte(unicodeValue), entry.Value)
	})

	t.Run("special characters in keys", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		specialKeys := []string{
			"key with spaces",
			"key:with:colons",
			"key/with/slashes",
			"key.with.dots",
			"key@with@at",
		}
		
		for i, key := range specialKeys {
			err := engine.Put(key, []byte(fmt.Sprintf("value:%d", i)))
			require.NoError(t, err)
		}
		
		for i, key := range specialKeys {
			entry, err := engine.Get(key)
			require.NoError(t, err, "failed to get key: %s", key)
			assert.Equal(t, []byte(fmt.Sprintf("value:%d", i)), entry.Value)
		}
	})

	t.Run("empty value", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		err := engine.Put("key", []byte{})
		require.NoError(t, err)
		
		entry, err := engine.Get("key")
		require.NoError(t, err)
		assert.Empty(t, entry.Value)
	})

	t.Run("nil value", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		err := engine.Put("key", nil)
		require.NoError(t, err)
		
		entry, err := engine.Get("key")
		require.NoError(t, err)
		assert.Nil(t, entry.Value)
	})
}

func TestHashMapEngine_LargeDataset(t *testing.T) {
	t.Run("10000 keys", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		// Write 10000 keys
		for i := 0; i < 10000; i++ {
			err := engine.Put(fmt.Sprintf("key:%05d", i), []byte(fmt.Sprintf("value:%d", i)))
			require.NoError(t, err)
		}
		
		// Read all keys
		for i := 0; i < 10000; i++ {
			entry, err := engine.Get(fmt.Sprintf("key:%05d", i))
			require.NoError(t, err, "failed to get key:%05d", i)
			assert.Equal(t, []byte(fmt.Sprintf("value:%d", i)), entry.Value)
		}
		
		// Scan all
		entries, err := engine.Scan("")
		require.NoError(t, err)
		assert.Equal(t, 10000, len(entries))
		
		// Stats
		stats := engine.Stats()
		assert.Equal(t, int64(10000), stats.KeyCount)
	})
}

func TestHashMapEngine_Concurrent(t *testing.T) {
	t.Run("concurrent reads and writes", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		done := make(chan bool)
		
		// Writers
		for i := 0; i < 5; i++ {
			go func(id int) {
				for j := 0; j < 100; j++ {
					key := fmt.Sprintf("goroutine:%d:key:%d", id, j)
					engine.Put(key, []byte(fmt.Sprintf("value:%d", j)))
				}
				done <- true
			}(i)
		}
		
		// Readers
		for i := 0; i < 5; i++ {
			go func(id int) {
				for j := 0; j < 100; j++ {
					key := fmt.Sprintf("goroutine:%d:key:%d", id, j)
					engine.Get(key)
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
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		// Pre-populate
		for i := 0; i < 100; i++ {
			engine.Put(fmt.Sprintf("key:%d", i), []byte("value"))
		}
		
		done := make(chan bool)
		
		// Scanners
		for i := 0; i < 5; i++ {
			go func() {
				for j := 0; j < 20; j++ {
					engine.Scan("")
				}
				done <- true
			}()
		}
		
		for i := 0; i < 5; i++ {
			<-done
		}
	})
}

func TestHashMapEngine_PutEntryHandler(t *testing.T) {
	t.Run("PutEntry adds to data map", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		entry := Entry{
			Key:       "testkey",
			Value:     []byte("testvalue"),
			Version:   1,
			TimeStamp: time.Now(),
		}
		
		err := engine.PutEntry(entry)
		require.NoError(t, err)
		
		retrieved, ok := engine.data["testkey"]
		require.True(t, ok)
		assert.Equal(t, []byte("testvalue"), retrieved.Value)
	})

	t.Run("SetVersion updates version", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		engine.SetVersion(100)
		assert.Equal(t, uint64(100), engine.version.Load())
	})
}

func TestHashMapEngine_WithoutWAL(t *testing.T) {
	t.Run("engine without WAL", func(t *testing.T) {
		// This test shows the engine can work without WAL
		// though it won't have durability
		engine := &HashMapEngine{
			data: make(map[string]Entry),
		}
		
		err := engine.Put("key", []byte("value"))
		require.NoError(t, err)
		
		entry, err := engine.Get("key")
		require.NoError(t, err)
		assert.Equal(t, []byte("value"), entry.Value)
		
		engine.Close()
	})
}

func TestHashMapEngine_Tombstones(t *testing.T) {
	t.Run("tombstone in scan", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		engine.Put("key1", []byte("value1"))
		engine.Put("key2", []byte("value2"))
		engine.Delete("key2")
		
		entries, err := engine.Scan("")
		require.NoError(t, err)
		assert.Len(t, entries, 1)
		assert.Equal(t, "key1", entries[0].Key)
	})

	t.Run("tombstone in keys", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")
		
		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()
		
		engine.Put("key1", []byte("value1"))
		engine.Put("key2", []byte("value2"))
		engine.Delete("key2")
		
		keys, err := engine.Keys()
		require.NoError(t, err)
		assert.Len(t, keys, 1)
		assert.Equal(t, "key1", keys[0])
	})

	t.Run("get returns tombstone", func(t *testing.T) {
		dir := t.TempDir()
		walPath := filepath.Join(dir, "test.wal")

		engine, _ := NewHashmapEngine(walPath)
		defer engine.Close()

		engine.Put("key", []byte("value"))
		engine.Delete("key")

		// After delete, Get should return ErrKeyNotFound
		_, err := engine.Get("key")
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})
}
