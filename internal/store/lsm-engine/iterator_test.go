package lsmengine

import (
	"testing"

	storage "github.com/DevLikhith5/kasoku/internal/store"
	"github.com/stretchr/testify/assert"
)

func TestIterator_Basic(t *testing.T) {
	t.Run("empty iterator", func(t *testing.T) {
		it := NewIterator(nil, "")
		assert.False(t, it.First())
		assert.False(t, it.Valid())
		assert.Equal(t, -1, it.Pos())
	})

	t.Run("single entry", func(t *testing.T) {
		entries := []storage.Entry{
			{Key: "key1", Value: []byte("value1")},
		}
		it := NewIterator(entries, "")
		assert.True(t, it.First())
		assert.True(t, it.Valid())
		assert.Equal(t, "key1", it.Key())
		assert.Equal(t, []byte("value1"), it.Value())
		assert.Equal(t, 0, it.Pos())
	})

	t.Run("multiple entries", func(t *testing.T) {
		entries := []storage.Entry{
			{Key: "key1", Value: []byte("value1")},
			{Key: "key2", Value: []byte("value2")},
			{Key: "key3", Value: []byte("value3")},
		}
		it := NewIterator(entries, "")
		
		// First
		assert.True(t, it.First())
		assert.Equal(t, "key1", it.Key())
		
		// Next
		assert.True(t, it.Next())
		assert.Equal(t, "key2", it.Key())
		
		assert.True(t, it.Next())
		assert.Equal(t, "key3", it.Key())
		
		// No more entries
		assert.False(t, it.Next())
		assert.False(t, it.Valid())
	})
}

func TestIterator_Seek(t *testing.T) {
	entries := []storage.Entry{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
		{Key: "c", Value: []byte("3")},
		{Key: "d", Value: []byte("4")},
		{Key: "e", Value: []byte("5")},
	}

	t.Run("seek to middle", func(t *testing.T) {
		it := NewIterator(entries, "")
		assert.True(t, it.Seek("c"))
		assert.Equal(t, "c", it.Key())
		assert.Equal(t, 2, it.Pos())
	})

	t.Run("seek to first", func(t *testing.T) {
		it := NewIterator(entries, "")
		assert.True(t, it.Seek("a"))
		assert.Equal(t, "a", it.Key())
		assert.Equal(t, 0, it.Pos())
	})

	t.Run("seek to last", func(t *testing.T) {
		it := NewIterator(entries, "")
		assert.True(t, it.Seek("e"))
		assert.Equal(t, "e", it.Key())
		assert.Equal(t, 4, it.Pos())
	})

	t.Run("seek beyond last", func(t *testing.T) {
		it := NewIterator(entries, "")
		assert.False(t, it.Seek("z"))
		assert.False(t, it.Valid())
	})

	t.Run("seek to non-existent key", func(t *testing.T) {
		it := NewIterator(entries, "")
		assert.True(t, it.Seek("bb")) // should position at "c"
		assert.Equal(t, "c", it.Key())
	})
}

func TestIterator_Prev(t *testing.T) {
	entries := []storage.Entry{
		{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Value: []byte("value2")},
		{Key: "key3", Value: []byte("value3")},
	}

	it := NewIterator(entries, "")
	
	// Go to last
	assert.True(t, it.Last())
	assert.Equal(t, "key3", it.Key())
	
	// Previous
	assert.True(t, it.Prev())
	assert.Equal(t, "key2", it.Key())
	
	assert.True(t, it.Prev())
	assert.Equal(t, "key1", it.Key())
	
	// No more previous
	assert.False(t, it.Prev())
	assert.False(t, it.Valid())
}

func TestIterator_Last(t *testing.T) {
	t.Run("empty iterator", func(t *testing.T) {
		it := NewIterator(nil, "")
		assert.False(t, it.Last())
	})

	t.Run("single entry", func(t *testing.T) {
		entries := []storage.Entry{{Key: "key1", Value: []byte("value1")}}
		it := NewIterator(entries, "")
		assert.True(t, it.Last())
		assert.Equal(t, "key1", it.Key())
	})

	t.Run("multiple entries", func(t *testing.T) {
		entries := []storage.Entry{
			{Key: "key1", Value: []byte("value1")},
			{Key: "key2", Value: []byte("value2")},
			{Key: "key3", Value: []byte("value3")},
		}
		it := NewIterator(entries, "")
		assert.True(t, it.Last())
		assert.Equal(t, "key3", it.Key())
		assert.Equal(t, 2, it.Pos())
	})
}

func TestIterator_Prefix(t *testing.T) {
	entries := []storage.Entry{
		{Key: "user:1", Value: []byte("v1")},
		{Key: "user:2", Value: []byte("v2")},
		{Key: "config:a", Value: []byte("c1")},
		{Key: "user:3", Value: []byte("v3")},
	}

	t.Run("prefix filter", func(t *testing.T) {
		it := NewIterator(entries, "user:")
		assert.Equal(t, 3, it.Total())
		
		assert.True(t, it.First())
		assert.Equal(t, "user:1", it.Key())
		
		assert.True(t, it.Next())
		assert.Equal(t, "user:2", it.Key())
		
		assert.True(t, it.Next())
		assert.Equal(t, "user:3", it.Key())
		
		assert.False(t, it.Next())
	})

	t.Run("empty prefix", func(t *testing.T) {
		it := NewIterator(entries, "")
		assert.Equal(t, 4, it.Total())
	})
}

func TestIterator_Close(t *testing.T) {
	entries := []storage.Entry{{Key: "key1", Value: []byte("value1")}}
	it := NewIterator(entries, "")
	
	assert.True(t, it.First())
	assert.NoError(t, it.Close())
	
	assert.False(t, it.Valid())
	assert.False(t, it.Next())
}

func TestIterator_Entry(t *testing.T) {
	entries := []storage.Entry{
		{Key: "key1", Value: []byte("value1"), Version: 1},
	}
	it := NewIterator(entries, "")
	
	assert.True(t, it.First())
	entry := it.Entry()
	assert.Equal(t, "key1", entry.Key)
	assert.Equal(t, []byte("value1"), entry.Value)
	assert.Equal(t, uint64(1), entry.Version)
}

func TestIterator_SeekToFirstLast(t *testing.T) {
	entries := []storage.Entry{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
		{Key: "c", Value: []byte("3")},
	}

	it := NewIterator(entries, "")
	
	// SeekToFirst
	assert.True(t, it.SeekToFirst())
	assert.Equal(t, "a", it.Key())
	
	// SeekToLast
	assert.True(t, it.SeekToLast())
	assert.Equal(t, "c", it.Key())
}

func TestBlockCache_Basic(t *testing.T) {
	cache := NewBlockCache(3)

	t.Run("put and get", func(t *testing.T) {
		cache.Put("key1", []byte("value1"))
		val, ok := cache.Get("key1")
		assert.True(t, ok)
		assert.Equal(t, []byte("value1"), val)
	})

	t.Run("get missing key", func(t *testing.T) {
		_, ok := cache.Get("nonexistent")
		assert.False(t, ok)
	})

	t.Run("lru eviction", func(t *testing.T) {
		cache := NewBlockCache(2)
		cache.Put("key1", []byte("v1"))
		cache.Put("key2", []byte("v2"))
		
		// Add third key - should evict key1
		cache.Put("key3", []byte("v3"))
		
		_, ok := cache.Get("key1")
		assert.False(t, ok) // evicted
		
		_, ok = cache.Get("key2")
		assert.True(t, ok)
		
		_, ok = cache.Get("key3")
		assert.True(t, ok)
	})

	t.Run("update moves to end", func(t *testing.T) {
		cache := NewBlockCache(2)
		cache.Put("key1", []byte("v1"))
		cache.Put("key2", []byte("v2"))
		
		// Update key1
		cache.Put("key1", []byte("v1-updated"))
		
		// Add key3 - should evict key2 (least recently used)
		cache.Put("key3", []byte("v3"))
		
		val, ok := cache.Get("key1")
		assert.True(t, ok)
		assert.Equal(t, []byte("v1-updated"), val)
		
		_, ok = cache.Get("key2")
		assert.False(t, ok) // evicted
	})
}

func TestBlockCache_Concurrent(t *testing.T) {
	cache := NewBlockCache(100)
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := string(rune('a' + id))
				cache.Put(key, []byte("value"))
				cache.Get(key)
			}
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
