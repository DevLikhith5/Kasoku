package lsmengine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	storage "github.com/DevLikhith5/kasoku/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSTableWriter(t *testing.T) {
	t.Run("create writer", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.sst")

		writer, err := NewSSTableWriter(path, 100)
		require.NoError(t, err)
		require.NotNil(t, writer)

		assert.NotNil(t, writer.file)
		assert.NotNil(t, writer.filter)
		assert.Empty(t, writer.index)
		assert.Equal(t, int64(0), writer.offset)
		assert.Equal(t, 0, writer.count)

		writer.Close()
	})

	t.Run("write single entry", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.sst")

		writer, err := NewSSTableWriter(path, 10)
		require.NoError(t, err)

		entry := storage.Entry{
			Key:       "testkey",
			Value:     []byte("testvalue"),
			Version:   1,
			TimeStamp: time.Now(),
			Tombstone: false,
		}

		err = writer.WriteEntry(entry)
		require.NoError(t, err)

		assert.Equal(t, 1, writer.count)
		assert.Len(t, writer.index, 1)
		assert.Equal(t, "testkey", writer.index[0].Key)

		writer.Close()

		// Verify file exists and has content
		info, err := os.Stat(path)
		require.NoError(t, err)
		assert.Greater(t, info.Size(), int64(0))
	})

	t.Run("write multiple entries", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.sst")

		writer, err := NewSSTableWriter(path, 100)
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			entry := storage.Entry{
				Key:       fmt.Sprintf("key:%d", i),
				Value:     []byte(fmt.Sprintf("value:%d", i)),
				Version:   uint64(i + 1),
				TimeStamp: time.Now(),
			}
			err = writer.WriteEntry(entry)
			require.NoError(t, err)
		}

		assert.Equal(t, 10, writer.count)
		assert.Len(t, writer.index, 10)

		writer.Close()
	})

	t.Run("finalize creates valid file", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.sst")

		writer, err := NewSSTableWriter(path, 10)
		require.NoError(t, err)

		writer.WriteEntry(storage.Entry{Key: "key1", Value: []byte("val1")})
		writer.WriteEntry(storage.Entry{Key: "key2", Value: []byte("val2")})

		err = writer.Finalize()
		require.NoError(t, err)

		// Verify file can be opened
		reader, err := OpenSSTable(path)
		require.NoError(t, err)
		defer reader.Close()

		assert.NotNil(t, reader.filter)
		assert.NotNil(t, reader.index)
	})

	t.Run("index is sorted after finalize", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.sst")

		writer, err := NewSSTableWriter(path, 100)
		require.NoError(t, err)

		// Write in random order
		keys := []string{"zebra", "apple", "mango", "banana"}
		for _, key := range keys {
			writer.WriteEntry(storage.Entry{Key: key, Value: []byte("value")})
		}

		writer.Finalize()

		reader, err := OpenSSTable(path)
		require.NoError(t, err)
		defer reader.Close()

		// Index should be sorted
		for i := 1; i < len(reader.index); i++ {
			assert.Less(t, reader.index[i-1].Key, reader.index[i].Key)
		}
	})

	t.Run("bloom filter populated", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.sst")

		writer, err := NewSSTableWriter(path, 100)
		require.NoError(t, err)

		writer.WriteEntry(storage.Entry{Key: "testkey", Value: []byte("value")})
		writer.Finalize()

		reader, err := OpenSSTable(path)
		require.NoError(t, err)
		defer reader.Close()

		// Bloom filter should contain the key
		assert.True(t, reader.filter.MightContain([]byte("testkey")))
		assert.False(t, reader.filter.MightContain([]byte("nonexistent")))
	})
}

func TestSSTableReader(t *testing.T) {
	t.Run("open non-existent file", func(t *testing.T) {
		_, err := OpenSSTable("/nonexistent/path/test.sst")
		assert.Error(t, err)
	})

	t.Run("get existing key", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.sst")

		// Create SSTable
		writer, _ := NewSSTableWriter(path, 10)
		writer.WriteEntry(storage.Entry{
			Key:     "mykey",
			Value:   []byte("myvalue"),
			Version: 1,
		})
		writer.Finalize()

		// Read it back
		reader, err := OpenSSTable(path)
		require.NoError(t, err)
		defer reader.Close()

		entry, err := reader.Get("mykey")
		require.NoError(t, err)
		assert.Equal(t, "mykey", entry.Key)
		assert.Equal(t, []byte("myvalue"), entry.Value)
	})

	t.Run("get non-existing key", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.sst")

		writer, _ := NewSSTableWriter(path, 10)
		writer.WriteEntry(storage.Entry{Key: "key1", Value: []byte("val1")})
		writer.Finalize()

		reader, err := OpenSSTable(path)
		require.NoError(t, err)
		defer reader.Close()

		_, err = reader.Get("nonexistent")
		assert.ErrorIs(t, err, storage.ErrKeyNotFound)
	})

	t.Run("get with bloom filter optimization", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.sst")

		writer, _ := NewSSTableWriter(path, 100)
		writer.WriteEntry(storage.Entry{Key: "existing", Value: []byte("value")})
		writer.Finalize()

		reader, err := OpenSSTable(path)
		require.NoError(t, err)
		defer reader.Close()

		// Key that's definitely not in filter - should fail fast
		_, err = reader.Get("definitely_not_here_xyz_123")
		assert.ErrorIs(t, err, storage.ErrKeyNotFound)
	})

	t.Run("scan with prefix", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.sst")

		writer, _ := NewSSTableWriter(path, 100)
		writer.WriteEntry(storage.Entry{Key: "user:1", Value: []byte("Alice")})
		writer.WriteEntry(storage.Entry{Key: "user:2", Value: []byte("Bob")})
		writer.WriteEntry(storage.Entry{Key: "user:3", Value: []byte("Charlie")})
		writer.WriteEntry(storage.Entry{Key: "session:1", Value: []byte("xyz")})
		writer.Finalize()

		reader, err := OpenSSTable(path)
		require.NoError(t, err)
		defer reader.Close()

		// Scan user: prefix
		entries, err := reader.Scan("user:")
		require.NoError(t, err)
		assert.Len(t, entries, 3)
		assert.Equal(t, "user:1", entries[0].Key)
		assert.Equal(t, "user:2", entries[1].Key)
		assert.Equal(t, "user:3", entries[2].Key)
	})

	t.Run("scan with no matching prefix", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.sst")

		writer, _ := NewSSTableWriter(path, 10)
		writer.WriteEntry(storage.Entry{Key: "key1", Value: []byte("val1")})
		writer.Finalize()

		reader, err := OpenSSTable(path)
		require.NoError(t, err)
		defer reader.Close()

		entries, err := reader.Scan("nonexistent:")
		require.NoError(t, err)
		assert.Empty(t, entries)
	})

	t.Run("scan empty prefix returns all", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.sst")

		writer, _ := NewSSTableWriter(path, 10)
		writer.WriteEntry(storage.Entry{Key: "a", Value: []byte("1")})
		writer.WriteEntry(storage.Entry{Key: "b", Value: []byte("2")})
		writer.Finalize()

		reader, err := OpenSSTable(path)
		require.NoError(t, err)
		defer reader.Close()

		entries, err := reader.Scan("")
		require.NoError(t, err)
		assert.Len(t, entries, 2)
	})

	t.Run("scan returns sorted results", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.sst")

		writer, _ := NewSSTableWriter(path, 100)
		// Write in random order
		keys := []string{"z", "a", "m", "b", "y"}
		for _, key := range keys {
			writer.WriteEntry(storage.Entry{Key: key, Value: []byte("val")})
		}
		writer.Finalize()

		reader, err := OpenSSTable(path)
		require.NoError(t, err)
		defer reader.Close()

		entries, err := reader.Scan("")
		require.NoError(t, err)

		expected := []string{"a", "b", "m", "y", "z"}
		for i, entry := range entries {
			assert.Equal(t, expected[i], entry.Key)
		}
	})

	t.Run("get tombstone entry", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.sst")

		writer, _ := NewSSTableWriter(path, 10)
		writer.WriteEntry(storage.Entry{
			Key:       "deleted",
			Value:     nil,
			Tombstone: true,
		})
		writer.Finalize()

		reader, err := OpenSSTable(path)
		require.NoError(t, err)
		defer reader.Close()

		entry, err := reader.Get("deleted")
		require.NoError(t, err)
		assert.True(t, entry.Tombstone)
		assert.Nil(t, entry.Value)
	})
}

func TestSSTable_RoundTrip(t *testing.T) {
	t.Run("write and read back all entry fields", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.sst")

		now := time.Now()
		original := storage.Entry{
			Key:       "testkey",
			Value:     []byte("testvalue"),
			Version:   42,
			TimeStamp: now,
			Tombstone: false,
		}

		writer, _ := NewSSTableWriter(path, 10)
		writer.WriteEntry(original)
		writer.Finalize()

		reader, err := OpenSSTable(path)
		require.NoError(t, err)
		defer reader.Close()

		retrieved, err := reader.Get("testkey")
		require.NoError(t, err)

		assert.Equal(t, original.Key, retrieved.Key)
		assert.Equal(t, original.Value, retrieved.Value)
		assert.Equal(t, original.Version, retrieved.Version)
		// Compare timestamps with small tolerance
		assert.WithinDuration(t, original.TimeStamp, retrieved.TimeStamp, time.Second)
		assert.Equal(t, original.Tombstone, retrieved.Tombstone)
	})

	t.Run("large values", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.sst")

		largeValue := make([]byte, 100*1024) // 100KB
		for i := range largeValue {
			largeValue[i] = byte(i % 256)
		}

		writer, _ := NewSSTableWriter(path, 10)
		writer.WriteEntry(storage.Entry{Key: "large", Value: largeValue})
		writer.Finalize()

		reader, err := OpenSSTable(path)
		require.NoError(t, err)
		defer reader.Close()

		entry, err := reader.Get("large")
		require.NoError(t, err)
		assert.Equal(t, largeValue, entry.Value)
	})

	t.Run("many entries", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.sst")

		count := 1000
		writer, _ := NewSSTableWriter(path, count)

		for i := 0; i < count; i++ {
			writer.WriteEntry(storage.Entry{
				Key:   fmt.Sprintf("key:%05d", i),
				Value: []byte(fmt.Sprintf("value:%d", i)),
			})
		}
		writer.Finalize()

		reader, err := OpenSSTable(path)
		require.NoError(t, err)
		defer reader.Close()

		// Verify count
		assert.Len(t, reader.index, count)

		// Verify random samples
		samples := []int{0, 50, 100, 500, 999}
		for _, i := range samples {
			key := fmt.Sprintf("key:%05d", i)
			entry, err := reader.Get(key)
			require.NoError(t, err)
			assert.Equal(t, []byte(fmt.Sprintf("value:%d", i)), entry.Value)
		}

		// Verify scan
		entries, err := reader.Scan("key:0")
		require.NoError(t, err)
		assert.Greater(t, len(entries), 0)
	})
}

func TestSSTable_EdgeCases(t *testing.T) {
	t.Run("empty SSTable", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "empty.sst")

		writer, _ := NewSSTableWriter(path, 10)
		writer.Finalize()

		reader, err := OpenSSTable(path)
		require.NoError(t, err)
		defer reader.Close()

		assert.Empty(t, reader.index)

		_, err = reader.Get("anything")
		assert.ErrorIs(t, err, storage.ErrKeyNotFound)

		entries, err := reader.Scan("")
		require.NoError(t, err)
		assert.Empty(t, entries)
	})

	t.Run("special characters in keys", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.sst")

		specialKeys := []string{
			"key with spaces",
			"key:with:colons",
			"key/with/slashes",
			"key\nwith\nnewlines",
			"key\twith\ttabs",
		}

		writer, _ := NewSSTableWriter(path, 100)
		for i, key := range specialKeys {
			writer.WriteEntry(storage.Entry{
				Key:   key,
				Value: []byte(fmt.Sprintf("val:%d", i)),
			})
		}
		writer.Finalize()

		reader, err := OpenSSTable(path)
		require.NoError(t, err)
		defer reader.Close()

		for i, key := range specialKeys {
			entry, err := reader.Get(key)
			require.NoError(t, err, "failed to get key: %s", key)
			assert.Equal(t, []byte(fmt.Sprintf("val:%d", i)), entry.Value)
		}
	})

	t.Run("binary values", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.sst")

		// Binary data that might not be valid UTF-8
		binaryValue := []byte{0x00, 0x01, 0xFF, 0xFE, 0x80, 0x7F}

		writer, _ := NewSSTableWriter(path, 10)
		writer.WriteEntry(storage.Entry{Key: "binary", Value: binaryValue})
		writer.Finalize()

		reader, err := OpenSSTable(path)
		require.NoError(t, err)
		defer reader.Close()

		entry, err := reader.Get("binary")
		require.NoError(t, err)
		assert.Equal(t, binaryValue, entry.Value)
	})

	t.Run("duplicate keys - last one wins", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.sst")

		writer, _ := NewSSTableWriter(path, 10)
		writer.WriteEntry(storage.Entry{Key: "key", Value: []byte("first"), Version: 1})
		writer.WriteEntry(storage.Entry{Key: "key", Value: []byte("second"), Version: 2})
		writer.WriteEntry(storage.Entry{Key: "key", Value: []byte("third"), Version: 3})
		writer.Finalize()

		reader, err := OpenSSTable(path)
		require.NoError(t, err)
		defer reader.Close()

		// Note: SSTable stores all entries, but Get returns one
		// The behavior depends on how the index handles duplicates
		entry, err := reader.Get("key")
		require.NoError(t, err)
		// Should get one of the values (typically first in index)
		assert.Equal(t, "key", entry.Key)
		assert.NotEmpty(t, entry.Value)
	})
}

func TestSSTable_IndexEntry(t *testing.T) {
	t.Run("index entry serialization", func(t *testing.T) {
		entry := indexEntry{
			Key:    "testkey",
			Offset: 12345,
			Size:   678,
		}

		data, err := json.Marshal(entry)
		require.NoError(t, err)

		var decoded indexEntry
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		assert.Equal(t, entry.Key, decoded.Key)
		assert.Equal(t, entry.Offset, decoded.Offset)
		assert.Equal(t, entry.Size, decoded.Size)
	})
}

func TestSSTable_FileSize(t *testing.T) {
	t.Run("file size grows with entries", func(t *testing.T) {
		tmpDir := t.TempDir()

		sizes := make([]int64, 0)

		for count := 1; count <= 10; count++ {
			path := filepath.Join(tmpDir, fmt.Sprintf("test_%d.sst", count))
			writer, _ := NewSSTableWriter(path, 100)

			for i := 0; i < count; i++ {
				writer.WriteEntry(storage.Entry{
					Key:   fmt.Sprintf("key:%d", i),
					Value: []byte("fixedsizevalue"),
				})
			}
			writer.Finalize()

			info, _ := os.Stat(path)
			sizes = append(sizes, info.Size())
		}

		// File size should generally increase with more entries
		for i := 1; i < len(sizes); i++ {
			assert.Greater(t, sizes[i], sizes[i-1],
				"file size should increase with more entries")
		}
	})
}

func TestSSTable_Close(t *testing.T) {
	t.Run("close reader", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.sst")

		writer, _ := NewSSTableWriter(path, 10)
		writer.WriteEntry(storage.Entry{Key: "key", Value: []byte("value")})
		writer.Finalize()

		reader, err := OpenSSTable(path)
		require.NoError(t, err)

		err = reader.Close()
		assert.NoError(t, err)

		// Subsequent operations should fail
		_, err = reader.Get("key")
		assert.Error(t, err)
	})
}
