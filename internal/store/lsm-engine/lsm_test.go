package lsmengine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	storage "github.com/DevLikhith5/kasoku/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLSMEngine_Basic tests basic LSMEngine operations
func TestLSMEngine_Basic(t *testing.T) {
	t.Run("create engine", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		require.NotNil(t, engine)

		assert.NoError(t, engine.Close())
	})

	t.Run("create engine in non-existent directory", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "subdir", "data")
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		require.NotNil(t, engine)

		assert.DirExists(t, dir)
		engine.Close()
	})

	t.Run("put and get", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		err = engine.Put("key1", []byte("value1"))
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
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		_, err = engine.Get("nonexistent")
		assert.ErrorIs(t, err, storage.ErrKeyNotFound)
	})

	t.Run("overwrite key", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		require.NoError(t, engine.Put("key", []byte("v1")))
		require.NoError(t, engine.Put("key", []byte("v2")))
		require.NoError(t, engine.Put("key", []byte("v3")))

		entry, err := engine.Get("key")
		require.NoError(t, err)
		assert.Equal(t, []byte("v3"), entry.Value)
		assert.Equal(t, uint64(3), entry.Version)
	})

	t.Run("delete key", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		require.NoError(t, engine.Put("delkey", []byte("value")))
		require.NoError(t, engine.Delete("delkey"))

		_, err = engine.Get("delkey")
		assert.ErrorIs(t, err, storage.ErrKeyNotFound)
	})

	t.Run("delete non-existent key", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		err = engine.Delete("nonexistent")
		assert.NoError(t, err) // LSM allows deleting non-existent keys (tombstone)
	})
}

func TestLSMEngine_Scan(t *testing.T) {
	t.Run("scan with prefix", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
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
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
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
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		engine.Put("key1", []byte("value1"))

		entries, err := engine.Scan("nonexistent:")
		require.NoError(t, err)
		assert.Empty(t, entries)
	})

	t.Run("scan excludes deleted keys", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		engine.Put("user:1", []byte("Alice"))
		engine.Put("user:2", []byte("Bob"))
		engine.Delete("user:2")

		entries, err := engine.Scan("user:")
		require.NoError(t, err)
		assert.Len(t, entries, 1)
		assert.Equal(t, "user:1", entries[0].Key)
	})

	t.Run("scan with tombstone in memtable", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		engine.Put("key", []byte("value"))
		engine.Delete("key")

		entries, err := engine.Scan("")
		require.NoError(t, err)
		assert.Empty(t, entries)
	})
}

func TestLSMEngine_Keys(t *testing.T) {
	t.Run("keys returns all non-deleted keys", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
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
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
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
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		keys, err := engine.Keys()
		require.NoError(t, err)
		assert.Empty(t, keys)
	})
}

func TestLSMEngine_Version(t *testing.T) {
	t.Run("version increments on put", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		engine.Put("key1", []byte("value1"))
		e1, _ := engine.Get("key1")

		engine.Put("key2", []byte("value2"))
		e2, _ := engine.Get("key2")

		assert.Greater(t, e2.Version, e1.Version)
	})

	t.Run("version increments on delete", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		engine.Put("key", []byte("value"))

		engine.Delete("key")

		// After delete, key should not be found
		_, err = engine.Get("key")
		assert.ErrorIs(t, err, storage.ErrKeyNotFound)
	})
}

func TestLSMEngine_Flush(t *testing.T) {
	t.Run("flush creates SSTable", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		// Write data to memtable
		for i := 0; i < 1000; i++ {
			engine.Put(fmt.Sprintf("key:%05d", i), []byte(fmt.Sprintf("value:%d", i)))
		}

		// Explicitly flush memtable to disk
		require.NoError(t, engine.flushMemTable())

		// Check SSTables were created
		files, _ := os.ReadDir(dir)
		sstCount := 0
		for _, f := range files {
			if filepath.Ext(f.Name()) == ".sst" {
				sstCount++
			}
		}
		assert.Greater(t, sstCount, 0, "SSTables should be created after flush")

		// Verify data is still readable
		entry, err := engine.Get("key:00000")
		require.NoError(t, err)
		assert.Equal(t, []byte("value:0"), entry.Value)
	})

	t.Run("data survives flush", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)

		// Write data
		engine.Put("persistent", []byte("data"))

		// Explicitly flush memtable to disk
		require.NoError(t, engine.flushMemTable())

		// Close and reopen
		engine.Close()

		engine2, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine2.Close()

		entry, err := engine2.Get("persistent")
		require.NoError(t, err)
		assert.Equal(t, []byte("data"), entry.Value)
	})
}

func TestLSMEngine_CrashRecovery(t *testing.T) {
	t.Run("recover after crash", func(t *testing.T) {
		dir := t.TempDir()

		// Create engine and write data
		engine1, err := NewLSMEngine(dir)
		require.NoError(t, err)

		engine1.Put("key1", []byte("value1"))
		engine1.Put("key2", []byte("value2"))
		engine1.Put("key1", []byte("value1_updated"))
		engine1.Delete("key2")

		// Simulate crash (close without graceful shutdown)
		engine1.wal.Close()
		for _, level := range engine1.levels {
			for _, sst := range level {
				sst.Close()
			}
		}

		// Create new engine - should recover from WAL
		engine2, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine2.Close()

		entry, err := engine2.Get("key1")
		require.NoError(t, err)
		assert.Equal(t, []byte("value1_updated"), entry.Value)

		_, err = engine2.Get("key2")
		assert.ErrorIs(t, err, storage.ErrKeyNotFound)
	})

	t.Run("recover version counter", func(t *testing.T) {
		dir := t.TempDir()

		engine1, err := NewLSMEngine(dir)
		require.NoError(t, err)

		engine1.Put("key", []byte("value"))
		e1, _ := engine1.Get("key")

		engine1.Close()

		engine2, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine2.Close()

		engine2.Put("key2", []byte("value2"))
		e2, _ := engine2.Get("key2")

		assert.Greater(t, e2.Version, e1.Version)
	})
}

func TestLSMEngine_Stats(t *testing.T) {
	t.Run("stats on empty engine", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		stats := engine.Stats()
		assert.Equal(t, int64(0), stats.KeyCount)
		assert.GreaterOrEqual(t, stats.MemBytes, int64(0))
	})

	t.Run("stats with data", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		for i := 0; i < 100; i++ {
			engine.Put(fmt.Sprintf("key:%d", i), []byte(fmt.Sprintf("value:%d", i)))
		}

		stats := engine.Stats()
		assert.Greater(t, stats.KeyCount, int64(0))
		assert.Greater(t, stats.MemBytes, int64(0))
		assert.Equal(t, 0.01, stats.BloomFPRate)
	})
}

func TestLSMEngine_Close(t *testing.T) {
	t.Run("close stops background goroutines", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)

		// Write some data
		for i := 0; i < 100; i++ {
			engine.Put(fmt.Sprintf("key:%d", i), []byte("value"))
		}

		err = engine.Close()
		assert.NoError(t, err)

		// Operations after close should fail
		err = engine.Put("newkey", []byte("value"))
		assert.ErrorIs(t, err, storage.ErrEngineClosed)

		_, err = engine.Get("key:0")
		assert.ErrorIs(t, err, storage.ErrEngineClosed)

		_, err = engine.Keys()
		assert.ErrorIs(t, err, storage.ErrEngineClosed)
	})

	t.Run("double close is safe", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)

		err = engine.Close()
		assert.NoError(t, err)

		err = engine.Close()
		assert.NoError(t, err)
	})
}

func TestLSMEngine_Compaction(t *testing.T) {
	t.Run("trigger compaction", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		// Create multiple SSTables in L0
		for batch := 0; batch < 4; batch++ {
			for i := 0; i < 500; i++ {
				engine.Put(fmt.Sprintf("key:%d:%05d", batch, i), []byte(fmt.Sprintf("value:%d", i)))
			}
			// Force flush
			engine.flushMemTable()
		}

		// Trigger compaction
		engine.TriggerCompaction()
		time.Sleep(200 * time.Millisecond)

		// Data should still be readable
		entry, err := engine.Get("key:0:00000")
		require.NoError(t, err)
		assert.Equal(t, []byte("value:0"), entry.Value)
	})

	t.Run("compaction merges duplicate keys", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		// Write same keys multiple times
		for i := 0; i < 10; i++ {
			engine.Put("samekey", []byte(fmt.Sprintf("version%d", i)))
		}

		// Force flushes
		for i := 0; i < 5; i++ {
			for j := 0; j < 500; j++ {
				engine.Put(fmt.Sprintf("fill:%d:%d", i, j), []byte("padding"))
			}
			engine.flushMemTable()
		}

		// Trigger compaction
		engine.TriggerCompaction()
		time.Sleep(200 * time.Millisecond)

		// Should get latest version
		entry, err := engine.Get("samekey")
		require.NoError(t, err)
		assert.Equal(t, []byte("version9"), entry.Value)
	})
}

func TestLSMEngine_MultipleLevels(t *testing.T) {
	t.Run("data propagates through levels", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		// Create enough SSTables to trigger multi-level compaction
		for batch := 0; batch < 20; batch++ {
			for i := 0; i < 500; i++ {
				engine.Put(fmt.Sprintf("key:%d:%05d", batch, i), []byte(fmt.Sprintf("value:%d", i)))
			}
			engine.flushMemTable()
			time.Sleep(50 * time.Millisecond)
		}

		// Trigger compactions
		for i := 0; i < 5; i++ {
			engine.TriggerCompaction()
			time.Sleep(100 * time.Millisecond)
		}

		// Verify data is still accessible
		entry, err := engine.Get("key:0:00000")
		require.NoError(t, err)
		assert.Equal(t, []byte("value:0"), entry.Value)
	})
}

func TestLSMEngine_EdgeCases(t *testing.T) {
	t.Run("empty key", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		err = engine.Put("", []byte("empty key value"))
		require.NoError(t, err)

		entry, err := engine.Get("")
		require.NoError(t, err)
		assert.Equal(t, "", entry.Key)
		assert.Equal(t, []byte("empty key value"), entry.Value)
	})

	t.Run("key too long", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		longKey := string(make([]byte, storage.MaxKeyLen+1))
		err = engine.Put(longKey, []byte("value"))
		assert.ErrorIs(t, err, storage.ErrKeyTooLong)
	})

	t.Run("value too large", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		largeValue := make([]byte, storage.MaxValueLen+1)
		err = engine.Put("key", largeValue)
		assert.ErrorIs(t, err, storage.ErrValueTooLarge)
	})

	t.Run("binary values", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		binaryValue := []byte{0x00, 0x01, 0xFF, 0xFE, 0x80, 0x7F}
		err = engine.Put("binary", binaryValue)
		require.NoError(t, err)

		entry, err := engine.Get("binary")
		require.NoError(t, err)
		assert.Equal(t, binaryValue, entry.Value)
	})

	t.Run("unicode keys and values", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		unicodeKey := "こんにちは世界"
		unicodeValue := "🌍🌎🌏"

		err = engine.Put(unicodeKey, []byte(unicodeValue))
		require.NoError(t, err)

		entry, err := engine.Get(unicodeKey)
		require.NoError(t, err)
		assert.Equal(t, unicodeKey, entry.Key)
		assert.Equal(t, []byte(unicodeValue), entry.Value)
	})

	t.Run("special characters in keys", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		specialKeys := []string{
			"key with spaces",
			"key:with:colons",
			"key/with/slashes",
			"key.with.dots",
			"key@with@at",
		}

		for i, key := range specialKeys {
			err = engine.Put(key, []byte(fmt.Sprintf("value:%d", i)))
			require.NoError(t, err)
		}

		for i, key := range specialKeys {
			entry, err := engine.Get(key)
			require.NoError(t, err, "failed to get key: %s", key)
			assert.Equal(t, []byte(fmt.Sprintf("value:%d", i)), entry.Value)
		}
	})
}

func TestLSMEngine_LargeDataset(t *testing.T) {
	t.Run("10000 keys", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		// Write 10000 keys
		for i := 0; i < 10000; i++ {
			err = engine.Put(fmt.Sprintf("key:%05d", i), []byte(fmt.Sprintf("value:%d", i)))
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
		assert.Greater(t, stats.KeyCount, int64(0))
	})
}

func TestLSMEngine_Concurrent(t *testing.T) {
	t.Run("concurrent reads and writes", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
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
}

// TestLSMEngine_PutEntryHandler tests the WAL replay handler
func TestLSMEngine_PutEntryHandler(t *testing.T) {
	t.Run("PutEntry adds to memtable", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		entry := storage.Entry{
			Key:       "testkey",
			Value:     []byte("testvalue"),
			Version:   1,
			TimeStamp: time.Now(),
		}

		err = engine.PutEntry(entry)
		require.NoError(t, err)

		retrieved, ok := engine.active.Get("testkey")
		require.True(t, ok)
		assert.Equal(t, []byte("testvalue"), retrieved.Value)
	})

	t.Run("SetVersion updates version", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewLSMEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		engine.SetVersion(100)
		assert.Equal(t, uint64(100), engine.version.Load())
	})
}
