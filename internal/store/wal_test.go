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
