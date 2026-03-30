package lsmengine

import (
	"fmt"
	"testing"
	"time"

	storage "github.com/DevLikhith5/kasoku/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSkipList_Basic(t *testing.T) {
	t.Run("new skiplist is empty", func(t *testing.T) {
		sl := NewSkipList(16, 0.5)
		assert.Equal(t, 0, sl.Size())

		_, found := sl.Get("nonexistent")
		assert.False(t, found)
	})

	t.Run("put and get single entry", func(t *testing.T) {
		sl := NewSkipList(16, 0.5)
		entry := storage.Entry{
			Key:       "testkey",
			Value:     []byte("testvalue"),
			Version:   1,
			TimeStamp: time.Now(),
		}

		sl.Put(entry)

		retrieved, found := sl.Get("testkey")
		require.True(t, found)
		assert.Equal(t, "testkey", retrieved.Key)
		assert.Equal(t, []byte("testvalue"), retrieved.Value)
		assert.Equal(t, uint64(1), retrieved.Version)
		assert.Equal(t, 1, sl.Size())
	})

	t.Run("update existing entry", func(t *testing.T) {
		sl := NewSkipList(16, 0.5)

		sl.Put(storage.Entry{Key: "key", Value: []byte("v1"), Version: 1})
		sl.Put(storage.Entry{Key: "key", Value: []byte("v2"), Version: 2})
		sl.Put(storage.Entry{Key: "key", Value: []byte("v3"), Version: 3})

		assert.Equal(t, 1, sl.Size()) // size should not increase on update

		entry, found := sl.Get("key")
		require.True(t, found)
		assert.Equal(t, []byte("v3"), entry.Value)
		assert.Equal(t, uint64(3), entry.Version)
	})
}

func TestSkipList_MultipleEntries(t *testing.T) {
	t.Run("insert multiple entries in random order", func(t *testing.T) {
		sl := NewSkipList(16, 0.5)

		// Insert in random order
		keys := []string{"zebra", "apple", "mango", "banana", "cherry"}
		for i, key := range keys {
			sl.Put(storage.Entry{
				Key:       key,
				Value:     []byte(fmt.Sprintf("value-%d", i)),
				Version:   uint64(i + 1),
				TimeStamp: time.Now(),
			})
		}

		assert.Equal(t, 5, sl.Size())

		// Verify all entries
		for i, key := range keys {
			entry, found := sl.Get(key)
			require.True(t, found, "key %s not found", key)
			assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), entry.Value)
		}
	})

	t.Run("entries are sorted by key", func(t *testing.T) {
		sl := NewSkipList(16, 0.5)

		sl.Put(storage.Entry{Key: "c", Value: []byte("3")})
		sl.Put(storage.Entry{Key: "a", Value: []byte("1")})
		sl.Put(storage.Entry{Key: "e", Value: []byte("5")})
		sl.Put(storage.Entry{Key: "b", Value: []byte("2")})
		sl.Put(storage.Entry{Key: "d", Value: []byte("4")})

		entries := sl.Entries()
		require.Len(t, entries, 5)

		expectedOrder := []string{"a", "b", "c", "d", "e"}
		for i, entry := range entries {
			assert.Equal(t, expectedOrder[i], entry.Key)
		}
	})
}

func TestSkipList_Seek(t *testing.T) {
	t.Run("seek to existing key", func(t *testing.T) {
		sl := NewSkipList(16, 0.5)

		sl.Put(storage.Entry{Key: "apple", Value: []byte("1")})
		sl.Put(storage.Entry{Key: "banana", Value: []byte("2")})
		sl.Put(storage.Entry{Key: "cherry", Value: []byte("3")})

		node := sl.Seek("banana")
		require.NotNil(t, node)
		assert.Equal(t, "banana", node.entry.Key)
		assert.Equal(t, []byte("2"), node.entry.Value)
	})

	t.Run("seek to non-existing key", func(t *testing.T) {
		sl := NewSkipList(16, 0.5)

		sl.Put(storage.Entry{Key: "apple", Value: []byte("1")})
		sl.Put(storage.Entry{Key: "cherry", Value: []byte("3")})

		// Seek to "banana" should return "cherry" (next greater)
		node := sl.Seek("banana")
		require.NotNil(t, node)
		assert.Equal(t, "cherry", node.entry.Key)
	})

	t.Run("seek to key greater than all", func(t *testing.T) {
		sl := NewSkipList(16, 0.5)

		sl.Put(storage.Entry{Key: "apple", Value: []byte("1")})
		sl.Put(storage.Entry{Key: "banana", Value: []byte("2")})

		node := sl.Seek("zebra")
		assert.Nil(t, node)
	})

	t.Run("seek on empty skiplist", func(t *testing.T) {
		sl := NewSkipList(16, 0.5)
		node := sl.Seek("anything")
		assert.Nil(t, node)
	})

	t.Run("seek to prefix for scan", func(t *testing.T) {
		sl := NewSkipList(16, 0.5)

		sl.Put(storage.Entry{Key: "app:1", Value: []byte("1")})
		sl.Put(storage.Entry{Key: "app:2", Value: []byte("2")})
		sl.Put(storage.Entry{Key: "app:3", Value: []byte("3")})
		sl.Put(storage.Entry{Key: "user:1", Value: []byte("4")})
		sl.Put(storage.Entry{Key: "user:2", Value: []byte("5")})

		node := sl.Seek("app:")
		require.NotNil(t, node)
		assert.Equal(t, "app:1", node.entry.Key)
	})
}

func TestSkipList_Entries(t *testing.T) {
	t.Run("entries on empty list", func(t *testing.T) {
		sl := NewSkipList(16, 0.5)
		entries := sl.Entries()
		assert.Empty(t, entries)
	})

	t.Run("entries returns all in sorted order", func(t *testing.T) {
		sl := NewSkipList(16, 0.5)

		for i := 10; i >= 1; i-- {
			sl.Put(storage.Entry{
				Key:   fmt.Sprintf("key:%02d", i),
				Value: []byte(fmt.Sprintf("val:%d", i)),
			})
		}

		entries := sl.Entries()
		require.Len(t, entries, 10)

		for i := 0; i < 10; i++ {
			assert.Equal(t, fmt.Sprintf("key:%02d", i+1), entries[i].Key)
			assert.Equal(t, []byte(fmt.Sprintf("val:%d", i+1)), entries[i].Value)
		}
	})
}

func TestSkipList_LargeDataset(t *testing.T) {
	t.Run("insert 1000 entries", func(t *testing.T) {
		sl := NewSkipList(16, 0.5)

		for i := 0; i < 1000; i++ {
			sl.Put(storage.Entry{
				Key:       fmt.Sprintf("key:%05d", i),
				Value:     []byte(fmt.Sprintf("value:%d", i)),
				Version:   uint64(i),
				TimeStamp: time.Now(),
			})
		}

		assert.Equal(t, 1000, sl.Size())

		// Verify random samples
		samples := []int{0, 50, 100, 500, 999}
		for _, i := range samples {
			key := fmt.Sprintf("key:%05d", i)
			entry, found := sl.Get(key)
			require.True(t, found, "key %s not found", key)
			assert.Equal(t, []byte(fmt.Sprintf("value:%d", i)), entry.Value)
		}

		// Verify all entries are sorted
		entries := sl.Entries()
		require.Len(t, entries, 1000)
		for i := 1; i < len(entries); i++ {
			assert.Less(t, entries[i-1].Key, entries[i].Key)
		}
	})
}

func TestSkipList_Tombstones(t *testing.T) {
	t.Run("handle tombstone entries", func(t *testing.T) {
		sl := NewSkipList(16, 0.5)

		// Insert normal entry
		sl.Put(storage.Entry{
			Key:       "testkey",
			Value:     []byte("value"),
			Tombstone: false,
		})

		entry, found := sl.Get("testkey")
		require.True(t, found)
		assert.False(t, entry.Tombstone)

		// Update with tombstone
		sl.Put(storage.Entry{
			Key:       "testkey",
			Value:     nil,
			Tombstone: true,
		})

		entry, found = sl.Get("testkey")
		require.True(t, found)
		assert.True(t, entry.Tombstone)
		assert.Nil(t, entry.Value)
	})
}

func TestSkipList_DeterministicLevel(t *testing.T) {
	t.Run("test with p=0 (all level 1)", func(t *testing.T) {
		sl := NewSkipList(4, 0.0)

		for i := 0; i < 100; i++ {
			sl.Put(storage.Entry{Key: fmt.Sprintf("key:%d", i)})
		}

		// All nodes should be at level 1
		assert.Equal(t, 1, sl.level)

		// Still should work correctly
		for i := 0; i < 100; i++ {
			_, found := sl.Get(fmt.Sprintf("key:%d", i))
			assert.True(t, found)
		}
	})

	t.Run("test with p=1 (all max level)", func(t *testing.T) {
		sl := NewSkipList(4, 1.0)

		for i := 0; i < 100; i++ {
			sl.Put(storage.Entry{Key: fmt.Sprintf("key:%d", i)})
		}

		// All nodes should be at max level
		assert.Equal(t, 4, sl.level)
	})
}

func TestSkipList_EdgeCases(t *testing.T) {
	t.Run("empty key", func(t *testing.T) {
		sl := NewSkipList(16, 0.5)
		sl.Put(storage.Entry{Key: "", Value: []byte("empty key value")})

		entry, found := sl.Get("")
		require.True(t, found)
		assert.Equal(t, "", entry.Key)
		assert.Equal(t, []byte("empty key value"), entry.Value)
	})

	t.Run("very long key", func(t *testing.T) {
		sl := NewSkipList(16, 0.5)
		longKey := string(make([]byte, 1000))
		for i := range longKey {
			longKey = string(rune('a' + i%26))
		}

		sl.Put(storage.Entry{Key: longKey, Value: []byte("long key value")})

		entry, found := sl.Get(longKey)
		require.True(t, found)
		assert.Equal(t, longKey, entry.Key)
	})

	t.Run("very large value", func(t *testing.T) {
		sl := NewSkipList(16, 0.5)
		largeValue := make([]byte, 1024*1024) // 1MB
		for i := range largeValue {
			largeValue[i] = byte(i % 256)
		}

		sl.Put(storage.Entry{Key: "large", Value: largeValue})

		entry, found := sl.Get("large")
		require.True(t, found)
		assert.Equal(t, largeValue, entry.Value)
	})

	t.Run("special characters in keys", func(t *testing.T) {
		sl := NewSkipList(16, 0.5)
		specialKeys := []string{
			"key with spaces",
			"key:with:colons",
			"key/with/slashes",
			"key.with.dots",
			"key-with-dashes",
			"key_with_underscores",
		}

		for i, key := range specialKeys {
			sl.Put(storage.Entry{Key: key, Value: []byte(fmt.Sprintf("val:%d", i))})
		}

		for i, key := range specialKeys {
			entry, found := sl.Get(key)
			require.True(t, found, "key '%s' not found", key)
			assert.Equal(t, []byte(fmt.Sprintf("val:%d", i)), entry.Value)
		}
	})
}

func TestSkipList_Concurrent(t *testing.T) {
	// Note: SkipList is not thread-safe by design
	// This test documents that behavior
	t.Run("concurrent access without locks", func(t *testing.T) {
		sl := NewSkipList(16, 0.5)

		// Insert initial data
		for i := 0; i < 100; i++ {
			sl.Put(storage.Entry{Key: fmt.Sprintf("key:%d", i), Value: []byte("initial")})
		}

		done := make(chan bool)

		// Start multiple goroutines reading
		for i := 0; i < 10; i++ {
			go func() {
				for j := 0; j < 100; j++ {
					sl.Get(fmt.Sprintf("key:%d", j%100))
				}
				done <- true
			}()
		}

		// Wait for all readers
		for i := 0; i < 10; i++ {
			<-done
		}
	})
}

func TestSkipList_SeekForScan(t *testing.T) {
	t.Run("scan with prefix using seek", func(t *testing.T) {
		sl := NewSkipList(16, 0.5)

		// Insert entries with different prefixes
		prefixes := []string{"app:", "user:", "config:", "session:"}
		for _, prefix := range prefixes {
			for i := 1; i <= 3; i++ {
				sl.Put(storage.Entry{
					Key:   fmt.Sprintf("%s%d", prefix, i),
					Value: []byte(fmt.Sprintf("%s%d-value", prefix, i)),
				})
			}
		}

		// Test scanning each prefix
		for _, prefix := range prefixes {
			node := sl.Seek(prefix)
			count := 0
			for node != nil && len(node.entry.Key) >= len(prefix) && node.entry.Key[:len(prefix)] == prefix {
				count++
				node = node.forward[0]
			}
			assert.Equal(t, 3, count, "prefix %s should have 3 entries", prefix)
		}
	})
}
