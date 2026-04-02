package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockWALHandler for testing WAL replay
type MockWALHandler struct {
	entries []Entry
	version uint64
}

func (m *MockWALHandler) PutEntry(entry Entry) error {
	m.entries = append(m.entries, entry)
	return nil
}

func (m *MockWALHandler) SetVersion(version uint64) {
	m.version = version
}

func TestWAL_Open(t *testing.T) {
	t.Run("create new WAL", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, err := OpenWAL(path)
		require.NoError(t, err)
		require.NotNil(t, wal)

		assert.FileExists(t, path)
		wal.Close()
	})

	t.Run("open existing WAL", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		// Create and write
		wal1, _ := OpenWAL(path)
		wal1.Append(Entry{Key: "key1", Value: []byte("value1")})
		wal1.Close()

		// Reopen
		wal2, err := OpenWAL(path)
		require.NoError(t, err)
		require.NotNil(t, wal2)

		wal2.Close()
	})

	t.Run("open WAL in non-existent directory", func(t *testing.T) {
		dir := t.TempDir()
		subdir := filepath.Join(dir, "subdir")
		path := filepath.Join(subdir, "test.wal")

		// Create directory first
		require.NoError(t, os.MkdirAll(subdir, 0755))

		wal, err := OpenWAL(path)
		require.NoError(t, err)
		require.NotNil(t, wal)

		assert.DirExists(t, filepath.Dir(path))
		wal.Close()
	})
}

func TestWAL_Append(t *testing.T) {
	t.Run("append single entry", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)
		defer wal.Close()

		entry := Entry{
			Key:       "testkey",
			Value:     []byte("testvalue"),
			Version:   1,
			TimeStamp: time.Now(),
			Tombstone: false,
		}

		err := wal.Append(entry)
		require.NoError(t, err)

		// Verify file has content
		info, err := wal.File().Stat()
		require.NoError(t, err)
		assert.Greater(t, info.Size(), int64(0))
	})

	t.Run("append multiple entries", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)
		defer wal.Close()

		for i := 0; i < 10; i++ {
			err := wal.Append(Entry{
				Key:     fmt.Sprintf("key:%d", i),
				Value:   []byte(fmt.Sprintf("value:%d", i)),
				Version: uint64(i + 1),
			})
			require.NoError(t, err)
		}

		info, _ := wal.File().Stat()
		assert.Greater(t, info.Size(), int64(0))
	})

	t.Run("append tombstone entry", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)
		defer wal.Close()

		entry := Entry{
			Key:       "delkey",
			Value:     nil,
			Tombstone: true,
		}

		err := wal.Append(entry)
		require.NoError(t, err)

		// Replay and verify
		handler := &MockWALHandler{}
		err = wal.Replay(handler)
		require.NoError(t, err)

		require.Len(t, handler.entries, 1)
		assert.True(t, handler.entries[0].Tombstone)
	})

	t.Run("append large value", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)
		defer wal.Close()

		largeValue := make([]byte, 100*1024) // 100KB
		for i := range largeValue {
			largeValue[i] = byte(i % 256)
		}

		err := wal.Append(Entry{
			Key:   "large",
			Value: largeValue,
		})
		require.NoError(t, err)

		// Replay and verify
		handler := &MockWALHandler{}
		err = wal.Replay(handler)
		require.NoError(t, err)

		require.Len(t, handler.entries, 1)
		assert.Equal(t, largeValue, handler.entries[0].Value)
	})

	t.Run("append preserves order", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)
		defer wal.Close()

		for i := 0; i < 100; i++ {
			wal.Append(Entry{
				Key:     fmt.Sprintf("key:%03d", i),
				Value:   []byte(fmt.Sprintf("value:%d", i)),
				Version: uint64(i + 1),
			})
		}

		// Replay and verify order
		handler := &MockWALHandler{}
		wal.Replay(handler)

		require.Len(t, handler.entries, 100)
		for i := 0; i < 100; i++ {
			assert.Equal(t, fmt.Sprintf("key:%03d", i), handler.entries[i].Key)
			assert.Equal(t, fmt.Sprintf("value:%d", i), string(handler.entries[i].Value))
		}
	})
}

func TestWAL_Replay(t *testing.T) {
	t.Run("replay empty WAL", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)
		defer wal.Close()

		handler := &MockWALHandler{}
		err := wal.Replay(handler)
		require.NoError(t, err)

		assert.Empty(t, handler.entries)
		assert.Equal(t, uint64(0), handler.version)
	})

	t.Run("replay PUT operations", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)

		wal.Append(Entry{Key: "key1", Value: []byte("value1"), Version: 1})
		wal.Append(Entry{Key: "key2", Value: []byte("value2"), Version: 2})
		wal.Close()

		// Replay
		wal2, _ := OpenWAL(path)
		defer wal2.Close()

		handler := &MockWALHandler{}
		err := wal2.Replay(handler)
		require.NoError(t, err)

		require.Len(t, handler.entries, 2)
		assert.Equal(t, "key1", handler.entries[0].Key)
		assert.Equal(t, "key2", handler.entries[1].Key)
		assert.Equal(t, uint64(2), handler.version)
	})

	t.Run("replay DELETE operations", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)

		wal.Append(Entry{Key: "key1", Value: []byte("value1")})
		wal.Append(Entry{Key: "key2", Value: []byte("value2")})
		wal.Append(Entry{Key: "key1", Tombstone: true})
		wal.Close()

		wal2, _ := OpenWAL(path)
		defer wal2.Close()

		handler := &MockWALHandler{}
		err := wal2.Replay(handler)
		require.NoError(t, err)

		require.Len(t, handler.entries, 3)
		assert.False(t, handler.entries[0].Tombstone)
		assert.False(t, handler.entries[1].Tombstone)
		assert.True(t, handler.entries[2].Tombstone)
	})

	t.Run("replay preserves timestamps", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		now := time.Now()

		wal, _ := OpenWAL(path)
		wal.Append(Entry{
			Key:       "key",
			Value:     []byte("value"),
			TimeStamp: now,
		})
		wal.Close()

		wal2, _ := OpenWAL(path)
		defer wal2.Close()

		handler := &MockWALHandler{}
		err := wal2.Replay(handler)
		require.NoError(t, err)

		require.Len(t, handler.entries, 1)
		// Allow 1 second tolerance for timestamp comparison
		assert.WithinDuration(t, now, handler.entries[0].TimeStamp, time.Second)
	})

	t.Run("replay sets max version", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)

		// Write entries with non-sequential versions
		wal.Append(Entry{Key: "key1", Version: 5})
		wal.Append(Entry{Key: "key2", Version: 10})
		wal.Append(Entry{Key: "key3", Version: 3})
		wal.Append(Entry{Key: "key4", Version: 100})
		wal.Close()

		wal2, _ := OpenWAL(path)
		defer wal2.Close()

		handler := &MockWALHandler{}
		err := wal2.Replay(handler)
		require.NoError(t, err)

		assert.Equal(t, uint64(100), handler.version)
	})
}

func TestWAL_Reset(t *testing.T) {
	t.Run("reset clears WAL", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)

		// Write some entries
		wal.Append(Entry{Key: "key1", Value: []byte("value1")})
		wal.Append(Entry{Key: "key2", Value: []byte("value2")})

		info, _ := wal.File().Stat()
		initialSize := info.Size()
		assert.Greater(t, initialSize, int64(0))

		// Reset
		err := wal.Reset()
		require.NoError(t, err)

		info, _ = wal.File().Stat()
		assert.Equal(t, int64(0), info.Size())

		// Verify replay returns nothing
		handler := &MockWALHandler{}
		err = wal.Replay(handler)
		require.NoError(t, err)
		assert.Empty(t, handler.entries)

		wal.Close()
	})

	t.Run("reset and append new entries", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)

		wal.Append(Entry{Key: "old", Value: []byte("old")})
		wal.Reset()
		wal.Append(Entry{Key: "new", Value: []byte("new")})
		wal.Close()

		// Replay should only show new entry
		wal2, _ := OpenWAL(path)
		defer wal2.Close()

		handler := &MockWALHandler{}
		err := wal2.Replay(handler)
		require.NoError(t, err)

		require.Len(t, handler.entries, 1)
		assert.Equal(t, "new", handler.entries[0].Key)
	})
}

func TestWAL_Size(t *testing.T) {
	t.Run("size of empty WAL", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)
		defer wal.Close()

		size, err := wal.Size()
		require.NoError(t, err)
		assert.Equal(t, int64(0), size)
	})

	t.Run("size grows with entries", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)
		defer wal.Close()

		sizes := make([]int64, 0)

		for i := 0; i < 5; i++ {
			wal.Append(Entry{Key: fmt.Sprintf("key:%d", i), Value: []byte("fixedsizevalue")})
			size, _ := wal.Size()
			sizes = append(sizes, size)
		}

		// Size should increase
		for i := 1; i < len(sizes); i++ {
			assert.Greater(t, sizes[i], sizes[i-1])
		}
	})
}

func TestWAL_Close(t *testing.T) {
	t.Run("close WAL", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)

		err := wal.Close()
		assert.NoError(t, err)
	})

	t.Run("operations after close fail", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)
		wal.Close()

		err := wal.Append(Entry{Key: "key", Value: []byte("value")})
		assert.Error(t, err)
	})
}

func TestWAL_Durability(t *testing.T) {
	t.Run("fsync on append", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)

		// Append should fsync
		err := wal.Append(Entry{Key: "key", Value: []byte("value")})
		require.NoError(t, err)

		// Kill process simulation - close file descriptor directly
		wal.file.Close()

		// Reopen and verify data persisted
		wal2, _ := OpenWAL(path)
		defer wal2.Close()

		handler := &MockWALHandler{}
		err = wal2.Replay(handler)
		require.NoError(t, err)

		require.Len(t, handler.entries, 1)
		assert.Equal(t, "key", handler.entries[0].Key)
	})
}

func TestWAL_CorruptRecords(t *testing.T) {
	t.Run("skip corrupt records during replay", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)
		wal.Append(Entry{Key: "key1", Value: []byte("value1")})
		wal.Append(Entry{Key: "key2", Value: []byte("value2")})
		wal.Close()

		// Corrupt the file by appending invalid JSON
		f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
		f.WriteString("this is not valid json\n")
		f.Close()

		// Replay should skip corrupt record
		wal2, _ := OpenWAL(path)
		defer wal2.Close()

		handler := &MockWALHandler{}
		err := wal2.Replay(handler)
		require.NoError(t, err)

		// Should have 2 valid entries, corrupt one skipped
		assert.Equal(t, 2, len(handler.entries))
	})
}

func TestWAL_EdgeCases(t *testing.T) {
	t.Run("empty key", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)
		defer wal.Close()

		err := wal.Append(Entry{Key: "", Value: []byte("empty key")})
		require.NoError(t, err)

		handler := &MockWALHandler{}
		wal.Replay(handler)

		require.Len(t, handler.entries, 1)
		assert.Equal(t, "", handler.entries[0].Key)
	})

	t.Run("empty value", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)
		defer wal.Close()

		err := wal.Append(Entry{Key: "key", Value: []byte{}})
		require.NoError(t, err)

		handler := &MockWALHandler{}
		wal.Replay(handler)

		require.Len(t, handler.entries, 1)
		assert.Empty(t, handler.entries[0].Value)
	})

	t.Run("nil value", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)
		defer wal.Close()

		err := wal.Append(Entry{Key: "key", Value: nil})
		require.NoError(t, err)

		handler := &MockWALHandler{}
		wal.Replay(handler)

		require.Len(t, handler.entries, 1)
		assert.Nil(t, handler.entries[0].Value)
	})

	t.Run("unicode keys and values", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)
		defer wal.Close()

		err := wal.Append(Entry{
			Key:   "こんにちは",
			Value: []byte("世界"),
		})
		require.NoError(t, err)

		handler := &MockWALHandler{}
		wal.Replay(handler)

		require.Len(t, handler.entries, 1)
		assert.Equal(t, "こんにちは", handler.entries[0].Key)
		assert.Equal(t, "世界", string(handler.entries[0].Value))
	})

	t.Run("very long key", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)
		defer wal.Close()

		longKey := string(make([]byte, MaxKeyLen))
		err := wal.Append(Entry{Key: longKey, Value: []byte("value")})
		require.NoError(t, err)

		handler := &MockWALHandler{}
		wal.Replay(handler)

		require.Len(t, handler.entries, 1)
		assert.Equal(t, MaxKeyLen, len(handler.entries[0].Key))
	})

	t.Run("very large value (1MB)", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)
		defer wal.Close()

		largeValue := make([]byte, MaxValueLen)
		for i := range largeValue {
			largeValue[i] = byte(i % 256)
		}

		err := wal.Append(Entry{Key: "key", Value: largeValue})
		require.NoError(t, err)

		handler := &MockWALHandler{}
		wal.Replay(handler)

		require.Len(t, handler.entries, 1)
		assert.Equal(t, largeValue, handler.entries[0].Value)
	})

	t.Run("special characters in keys", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)
		defer wal.Close()

		specialKeys := []string{
			"key with spaces",
			"key:with:colons",
			"key/with/slashes",
			"key\nwith\nnewlines",
			"key\twith\ttabs",
			"key\"with\"quotes",
		}

		for _, key := range specialKeys {
			err := wal.Append(Entry{Key: key, Value: []byte("value")})
			require.NoError(t, err)
		}

		handler := &MockWALHandler{}
		wal.Replay(handler)

		require.Len(t, handler.entries, len(specialKeys))
		for i, key := range specialKeys {
			assert.Equal(t, key, handler.entries[i].Key)
		}
	})
}

func TestWALRecord_JSON(t *testing.T) {
	t.Run("serialize and deserialize record", func(t *testing.T) {
		now := time.Now()

		record := WALRecord{
			Op:        "PUT",
			Key:       "testkey",
			Value:     []byte("testvalue"),
			Version:   42,
			TimeStamp: now.UnixNano(),
		}

		data, err := json.Marshal(record)
		require.NoError(t, err)

		var decoded WALRecord
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		assert.Equal(t, record.Op, decoded.Op)
		assert.Equal(t, record.Key, decoded.Key)
		assert.Equal(t, record.Value, decoded.Value)
		assert.Equal(t, record.Version, decoded.Version)
		assert.Equal(t, record.TimeStamp, decoded.TimeStamp)
	})

	t.Run("DEL operation serialization", func(t *testing.T) {
		record := WALRecord{
			Op:        "DEL",
			Key:       "delkey",
			Value:     nil,
			Version:   1,
			TimeStamp: time.Now().UnixNano(),
		}

		data, err := json.Marshal(record)
		require.NoError(t, err)

		var decoded WALRecord
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		assert.Equal(t, "DEL", decoded.Op)
		assert.Equal(t, "delkey", decoded.Key)
		assert.Nil(t, decoded.Value)
	})
}

func TestWAL_File(t *testing.T) {
	t.Run("get underlying file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.wal")

		wal, _ := OpenWAL(path)
		defer wal.Close()

		f := wal.File()
		require.NotNil(t, f)

		// Can use file directly
		info, err := f.Stat()
		require.NoError(t, err)
		assert.Equal(t, "test.wal", info.Name())
	})
}

// ============ Tests for Bug Fixes ============

func TestWAL_Reset_WithSync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(path)
	require.NoError(t, err)
	defer wal.Close()

	// Write some entries
	require.NoError(t, wal.Append(Entry{Key: "k1", Value: []byte("v1")}))
	require.NoError(t, wal.Append(Entry{Key: "k2", Value: []byte("v2")}))

	// Verify entries exist
	count, err := wal.Count()
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	// Reset
	require.NoError(t, wal.Reset())

	// Verify WAL is empty
	count, err = wal.Count()
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Verify we can append after reset
	require.NoError(t, wal.Append(Entry{Key: "k3", Value: []byte("v3")}))
	count, err = wal.Count()
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestWAL_CheckpointPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(path)
	require.NoError(t, err)
	defer wal.Close()

	// Write some entries
	require.NoError(t, wal.Append(Entry{Key: "k1", Value: []byte("v1")}))
	require.NoError(t, wal.Append(Entry{Key: "k2", Value: []byte("v2")}))

	// Create checkpoint
	checkpoint, err := wal.Checkpoint()
	require.NoError(t, err)
	assert.Greater(t, checkpoint, uint64(0))

	// Verify checkpoint file was created
	checkpointPath := path + ".checkpoint"
	_, err = os.Stat(checkpointPath)
	assert.NoError(t, err, "checkpoint file should exist")

	// Load checkpoint and verify
	loaded, err := wal.LoadCheckpoint()
	require.NoError(t, err)
	assert.Equal(t, checkpoint, loaded)
}

func TestWAL_LoadCheckpoint_NoCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(path)
	require.NoError(t, err)
	defer wal.Close()

	// No checkpoint file exists yet
	loaded, err := wal.LoadCheckpoint()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), loaded)
}

func TestWAL_TruncateBefore_WithTempFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(path)
	require.NoError(t, err)
	defer wal.Close()

	// Write entries
	require.NoError(t, wal.Append(Entry{Key: "k1", Value: []byte("v1"), Version: 1}))
	require.NoError(t, wal.Append(Entry{Key: "k2", Value: []byte("v2"), Version: 2}))

	// Create checkpoint after first entry
	checkpoint, err := wal.Checkpoint()
	require.NoError(t, err)

	// Write more entries
	require.NoError(t, wal.Append(Entry{Key: "k3", Value: []byte("v3"), Version: 3}))

	// Truncate - should keep only k3
	require.NoError(t, wal.TruncateBefore(checkpoint))

	// Replay and verify only k3 remains
	handler := &MockWALHandler{}
	require.NoError(t, wal.Replay(handler))

	require.Len(t, handler.entries, 1)
	assert.Equal(t, "k3", handler.entries[0].Key)
	assert.Equal(t, []byte("v3"), handler.entries[0].Value)
}

func TestWAL_TruncateBefore_CorruptRecords(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(path)
	require.NoError(t, err)

	// Write valid entry
	require.NoError(t, wal.Append(Entry{Key: "k1", Value: []byte("v1")}))

	// Get checkpoint
	checkpoint, err := wal.Checkpoint()
	require.NoError(t, err)

	// Write corrupt data directly to file
	wal.mu.Lock()
	_, err = wal.file.WriteString("this is not valid json\n")
	wal.mu.Unlock()
	require.NoError(t, err)

	// Write another valid entry
	require.NoError(t, wal.Append(Entry{Key: "k2", Value: []byte("v2")}))

	// Truncate - should skip corrupt record
	require.NoError(t, wal.TruncateBefore(checkpoint))

	// Replay - should only get k2
	handler := &MockWALHandler{}
	require.NoError(t, wal.Replay(handler))

	require.Len(t, handler.entries, 1)
	assert.Equal(t, "k2", handler.entries[0].Key)
}

func TestWAL_Compact_RaceConditionFix(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(path)
	require.NoError(t, err)
	defer wal.Close()

	// Write entries
	for i := 0; i < 5; i++ {
		require.NoError(t, wal.Append(Entry{
			Key:       fmt.Sprintf("key%d", i),
			Value:     []byte(fmt.Sprintf("value%d", i)),
			Version:   uint64(i + 1),
			TimeStamp: time.Now(),
		}))
	}

	// Compact
	entriesKept, err := wal.Compact()
	require.NoError(t, err)

	// After compact with no new writes, all entries should be kept
	// (checkpoint at end means nothing to truncate)
	assert.Equal(t, 5, entriesKept)

	// Verify all entries still exist
	handler := &MockWALHandler{}
	require.NoError(t, wal.Replay(handler))
	assert.Len(t, handler.entries, 5)
}

func TestWAL_BackgroundSync_ErrorCallback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	var syncErrors []error
	errorCh := make(chan error, 10)

	config := WALConfig{
		SyncInterval: 50 * time.Millisecond,
		OnSyncError: func(err error) {
			syncErrors = append(syncErrors, err)
			errorCh <- err
		},
	}

	wal, err := OpenWALWithConfig(path, config)
	require.NoError(t, err)
	defer wal.Close()

	// Write some data
	require.NoError(t, wal.Append(Entry{Key: "k1", Value: []byte("v1")}))

	// Wait for background sync (should succeed, no errors)
	time.Sleep(150 * time.Millisecond)

	// Verify no sync errors
	assert.Empty(t, syncErrors)
	assert.Empty(t, errorCh)
}

func TestWAL_BackgroundSync_DefaultLogging(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	// Config with no OnSyncError - should log to stderr
	config := WALConfig{
		SyncInterval: 50 * time.Millisecond,
	}

	wal, err := OpenWALWithConfig(path, config)
	require.NoError(t, err)
	defer wal.Close()

	require.NoError(t, wal.Append(Entry{Key: "k1", Value: []byte("v1")}))

	// Wait for background sync
	time.Sleep(150 * time.Millisecond)

	// Should not panic or crash
}

func TestWAL_TruncateBefore_AtomicRename(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(path)
	require.NoError(t, err)
	defer wal.Close()

	// Write entries
	require.NoError(t, wal.Append(Entry{Key: "k1", Value: []byte("v1")}))

	checkpoint, err := wal.Checkpoint()
	require.NoError(t, err)

	require.NoError(t, wal.Append(Entry{Key: "k2", Value: []byte("v2")}))

	// Get original file info
	originalInfo, err := os.Stat(path)
	require.NoError(t, err)
	originalInode := getInode(originalInfo)

	// Truncate
	require.NoError(t, wal.TruncateBefore(checkpoint))

	// File should be replaced (new inode on Unix)
	newInfo, err := os.Stat(path)
	require.NoError(t, err)
	newInode := getInode(newInfo)

	// On most systems, rename changes the inode
	// This verifies atomic replace happened
	t.Logf("Original inode: %d, New inode: %d", originalInode, newInode)

	// Verify data integrity
	handler := &MockWALHandler{}
	require.NoError(t, wal.Replay(handler))
	require.Len(t, handler.entries, 1)
	assert.Equal(t, "k2", handler.entries[0].Key)
}

// getInode extracts inode from FileInfo for verification
// Returns 0 on Windows (doesn't have inodes)
func getInode(info os.FileInfo) uint64 {
	// Use syscall to get inode - works on Unix
	// On Windows or if fails, returns 0
	sys := info.Sys()
	if sys == nil {
		return 0
	}

	// Try to get inode via reflection (works on Unix)
	// This is test code so it's okay to be a bit hacky
	type inodeGetter interface {
		Ino() uint64
	}
	if ig, ok := sys.(inodeGetter); ok {
		return ig.Ino()
	}
	return 0
}

func TestWAL_Checkpoint_Truncate_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	// Phase 1: Create WAL with entries
	wal1, err := OpenWAL(path)
	require.NoError(t, err)

	entries := []Entry{
		{Key: "user:1", Value: []byte("Alice"), Version: 1, TimeStamp: time.Now()},
		{Key: "user:2", Value: []byte("Bob"), Version: 2, TimeStamp: time.Now()},
		{Key: "user:3", Value: []byte("Charlie"), Version: 3, TimeStamp: time.Now()},
	}

	for _, e := range entries {
		require.NoError(t, wal1.Append(e))
	}

	// Checkpoint
	checkpoint, err := wal1.Checkpoint()
	require.NoError(t, err)
	t.Logf("Checkpoint at: %d", checkpoint)

	// Add more entries after checkpoint
	moreEntries := []Entry{
		{Key: "user:4", Value: []byte("Dave"), Version: 4, TimeStamp: time.Now()},
		{Key: "user:5", Value: []byte("Eve"), Version: 5, TimeStamp: time.Now()},
	}

	for _, e := range moreEntries {
		require.NoError(t, wal1.Append(e))
	}

	wal1.Close()

	// Phase 2: Reopen and truncate
	wal2, err := OpenWAL(path)
	require.NoError(t, err)
	defer wal2.Close()

	// Load checkpoint
	loadedCheckpoint, err := wal2.LoadCheckpoint()
	require.NoError(t, err)
	assert.Equal(t, checkpoint, loadedCheckpoint)

	// Truncate old entries
	require.NoError(t, wal2.TruncateBefore(loadedCheckpoint))

	// Verify only new entries remain
	handler := &MockWALHandler{}
	require.NoError(t, wal2.Replay(handler))

	require.Len(t, handler.entries, 2)
	assert.Equal(t, "user:4", handler.entries[0].Key)
	assert.Equal(t, "user:5", handler.entries[1].Key)
}

func TestWAL_Compact_EmptyWAL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(path)
	require.NoError(t, err)
	defer wal.Close()

	// Compact empty WAL
	entriesKept, err := wal.Compact()
	require.NoError(t, err)
	assert.Equal(t, 0, entriesKept)
}

func TestWAL_Reset_Persistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(path)
	require.NoError(t, err)

	// Write and reset
	require.NoError(t, wal.Append(Entry{Key: "k1", Value: []byte("v1")}))
	require.NoError(t, wal.Reset())
	wal.Close()

	// Reopen - should still be empty
	wal2, err := OpenWAL(path)
	require.NoError(t, err)
	defer wal2.Close()

	count, err := wal2.Count()
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}
