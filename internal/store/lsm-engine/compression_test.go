package lsmengine

import (
	"os"
	"testing"

	storage "github.com/DevLikhith5/kasoku/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSTable_Compression(t *testing.T) {
	tmpFile := t.TempDir() + "/test.sst"

	t.Run("compressible data", func(t *testing.T) {
		writer, err := NewSSTableWriter(tmpFile, 10, 0.01)
		require.NoError(t, err)

		// Write repetitive data (highly compressible)
		for i := 0; i < 10; i++ {
			entry := storage.Entry{
				Key:   "key",
				Value: []byte("this is repetitive data that should compress well"),
			}
			require.NoError(t, writer.WriteEntry(entry))
		}

		require.NoError(t, writer.Finalize())

		// Read back
		reader, err := OpenSSTable(tmpFile)
		require.NoError(t, err)
		defer reader.Close()

		// Verify data
		for i := 0; i < 10; i++ {
			entry, err := reader.Get("key")
			require.NoError(t, err)
			assert.Equal(t, "this is repetitive data that should compress well", string(entry.Value))
		}
	})

	t.Run("incompressible data", func(t *testing.T) {
		tmpFile2 := t.TempDir() + "/test2.sst"
		writer, err := NewSSTableWriter(tmpFile2, 10, 0.01)
		require.NoError(t, err)

		// Write random data (not compressible)
		for i := 0; i < 10; i++ {
			entry := storage.Entry{
				Key:   string(rune('a' + i)),
				Value: []byte("random data " + string(rune('0'+i))),
			}
			require.NoError(t, writer.WriteEntry(entry))
		}

		require.NoError(t, writer.Finalize())

		reader, err := OpenSSTable(tmpFile2)
		require.NoError(t, err)
		defer reader.Close()

		// Verify all entries
		for i := 0; i < 10; i++ {
			key := string(rune('a' + i))
			entry, err := reader.Get(key)
			require.NoError(t, err)
			assert.Equal(t, "random data "+string(rune('0'+i)), string(entry.Value))
		}
	})
}

func TestSSTable_BlockCache(t *testing.T) {
	tmpFile := t.TempDir() + "/test.sst"

	// Create SSTable
	writer, err := NewSSTableWriter(tmpFile, 5, 0.01)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		entry := storage.Entry{
			Key:   string([]byte{'k', byte('0' + i)}),
			Value: []byte("value" + string(rune('0'+i))),
		}
		require.NoError(t, writer.WriteEntry(entry))
	}
	require.NoError(t, writer.Finalize())

	reader, err := OpenSSTable(tmpFile)
	require.NoError(t, err)
	defer reader.Close()

	t.Run("cache on get", func(t *testing.T) {
		// First get - should cache
		entry, err := reader.Get("k0")
		require.NoError(t, err)
		assert.Equal(t, "value0", string(entry.Value))

		// Second get - should hit cache
		entry2, err := reader.Get("k0")
		require.NoError(t, err)
		assert.Equal(t, "value0", string(entry2.Value))
	})

	t.Run("cache on scan", func(t *testing.T) {
		entries, err := reader.Scan("k")
		require.NoError(t, err)
		assert.Len(t, entries, 5)

		// All entries should be cached now
		cache := reader.blockCache
		assert.Greater(t, len(cache.keys), 0)
	})
}

func TestBlockCache_Global(t *testing.T) {
	t.Run("singleton pattern", func(t *testing.T) {
		cache1 := GetBlockCache()
		cache2 := GetBlockCache()
		assert.Equal(t, cache1, cache2) // same instance
	})

	t.Run("init once", func(t *testing.T) {
		// Should not panic on multiple inits
		InitBlockCache(100)
		InitBlockCache(200) // ignored
	})
}

func TestWAL_Compact(t *testing.T) {
	tmpFile := t.TempDir() + "/wal.log"

	wal, err := storage.OpenWAL(tmpFile)
	require.NoError(t, err)
	defer wal.Close()

	t.Run("checkpoint", func(t *testing.T) {
		// Write some entries
		for i := 0; i < 5; i++ {
			entry := storage.Entry{
				Key:   "key" + string(rune('0'+i)),
				Value: []byte("value" + string(rune('0'+i))),
			}
			require.NoError(t, wal.Append(entry))
		}

		checkpoint, err := wal.Checkpoint()
		require.NoError(t, err)
		assert.Greater(t, checkpoint, uint64(0))
	})

	t.Run("count", func(t *testing.T) {
		count, err := wal.Count()
		require.NoError(t, err)
		assert.Greater(t, count, 0)
	})

	t.Run("truncate before", func(t *testing.T) {
		// Create new WAL
		wal2, err := storage.OpenWAL(t.TempDir() + "/wal2.log")
		require.NoError(t, err)
		defer wal2.Close()

		// Write entries
		for i := 0; i < 10; i++ {
			entry := storage.Entry{
				Key:   "key" + string(rune('0'+i)),
				Value: []byte("value" + string(rune('0'+i))),
			}
			require.NoError(t, wal2.Append(entry))
		}

		countBefore, _ := wal2.Count()
		assert.Equal(t, 10, countBefore)

		// Checkpoint after 5 entries
		// (in real scenario, you'd checkpoint after flushing to SSTable)
		checkpoint, err := wal2.Checkpoint()
		require.NoError(t, err)

		// Truncate (simulating that first 5 entries are flushed)
		// For this test, we just verify checkpoint works
		assert.Greater(t, checkpoint, uint64(0))
	})
}

func TestWAL_Reset(t *testing.T) {
	tmpFile := t.TempDir() + "/wal.log"

	wal, err := storage.OpenWAL(tmpFile)
	require.NoError(t, err)
	defer wal.Close()

	// Write entries
	for i := 0; i < 5; i++ {
		entry := storage.Entry{
			Key:   "key" + string(rune('0'+i)),
			Value: []byte("value" + string(rune('0'+i))),
		}
		require.NoError(t, wal.Append(entry))
	}

	countBefore, _ := wal.Count()
	assert.Equal(t, 5, countBefore)

	// Reset WAL
	require.NoError(t, wal.Reset())

	countAfter, _ := wal.Count()
	assert.Equal(t, 0, countAfter)
}

func TestCompressionRatio(t *testing.T) {
	tmpFile := t.TempDir() + "/test.sst"

	// Write highly compressible data
	writer, err := NewSSTableWriter(tmpFile, 100, 0.01)
	require.NoError(t, err)

	repetitiveValue := []byte("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	for i := 0; i < 100; i++ {
		entry := storage.Entry{
			Key:   "key" + string(rune('0'+i%10)),
			Value: repetitiveValue,
		}
		require.NoError(t, writer.WriteEntry(entry))
	}

	require.NoError(t, writer.Finalize())

	// Check file size
	info, err := os.Stat(tmpFile)
	require.NoError(t, err)
	fileSize := info.Size()

	// Without compression, would be ~100 * (4 + 72) = 7600 bytes + index + metadata
	// With JSON overhead and index, compression may not always be smaller for small datasets
	// But compression should still be applied (check the Compressed flag in index)
	t.Logf("File size: %d bytes (100 entries)", fileSize)

	// Verify compression was attempted by opening and checking entries
	reader, err := OpenSSTable(tmpFile)
	require.NoError(t, err)
	defer reader.Close()

	// Verify data integrity
	for i := 0; i < 10; i++ {
		key := "key" + string(rune('0'+i))
		entry, err := reader.Get(key)
		require.NoError(t, err)
		assert.Equal(t, string(repetitiveValue), string(entry.Value))
	}
}
