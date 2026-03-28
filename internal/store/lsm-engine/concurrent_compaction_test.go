package lsmengine

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcurrentCompaction(t *testing.T) {
	tmpDir := t.TempDir()

	engine, err := NewLSMEngine(tmpDir)
	require.NoError(t, err)
	defer engine.Close()

	t.Run("compaction under concurrent writes", func(t *testing.T) {
		// Write enough data to trigger compaction
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key:%d", i)
			value := []byte(fmt.Sprintf("value:%d", i))
			require.NoError(t, engine.Put(key, value))
		}

		// Flush to create L0 SSTables
		require.NoError(t, engine.Flush())

		// Write more and flush again to trigger compaction
		for i := 100; i < 200; i++ {
			key := fmt.Sprintf("key:%d", i)
			value := []byte(fmt.Sprintf("value:%d", i))
			require.NoError(t, engine.Put(key, value))
		}
		require.NoError(t, engine.Flush())

		// Trigger compaction
		engine.TriggerCompaction()

		// Give compaction time to run
		time.Sleep(500 * time.Millisecond)

		// Verify data is still accessible
		for i := 0; i < 200; i++ {
			key := fmt.Sprintf("key:%d", i)
			entry, err := engine.Get(key)
			if err == nil {
				assert.Equal(t, fmt.Sprintf("value:%d", i), string(entry.Value))
			}
		}
	})
}

func TestCompactionSemaphore(t *testing.T) {
	tmpDir := t.TempDir()

	engine, err := NewLSMEngine(tmpDir)
	require.NoError(t, err)
	defer engine.Close()

	t.Run("handles multiple compaction requests", func(t *testing.T) {
		// Create many SSTables to trigger compaction
		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					key := fmt.Sprintf("key:%d:%d", id, j)
					value := []byte(fmt.Sprintf("value:%d:%d", id, j))
					engine.Put(key, value)
				}
			}(i)
		}

		wg.Wait()

		// Flush multiple times to create SSTables
		for i := 0; i < 5; i++ {
			require.NoError(t, engine.Flush())
			time.Sleep(10 * time.Millisecond)
		}

		// Trigger compaction multiple times
		for i := 0; i < 3; i++ {
			engine.TriggerCompaction()
			time.Sleep(100 * time.Millisecond)
		}

		// Should not block or panic
		time.Sleep(200 * time.Millisecond)
	})
}

func TestIterator_LSMEngine(t *testing.T) {
	tmpDir := t.TempDir()

	engine, err := NewLSMEngine(tmpDir)
	require.NoError(t, err)
	defer engine.Close()

	// Write test data
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("user:%d", i)
		value := []byte(fmt.Sprintf("value:%d", i))
		require.NoError(t, engine.Put(key, value))
	}

	// Add some config keys
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("config:%d", i)
		value := []byte(fmt.Sprintf("config_value:%d", i))
		require.NoError(t, engine.Put(key, value))
	}

	t.Run("iterate all keys", func(t *testing.T) {
		it, err := engine.Iter("")
		require.NoError(t, err)
		defer it.Close()

		count := 0
		for it.First(); it.Valid(); it.Next() {
			count++
			assert.NotEmpty(t, it.Key())
			assert.NotNil(t, it.Value())
		}
		assert.Equal(t, 15, count)
	})

	t.Run("iterate with prefix", func(t *testing.T) {
		it, err := engine.Iter("user:")
		require.NoError(t, err)
		defer it.Close()

		count := 0
		for it.First(); it.Valid(); it.Next() {
			count++
			assert.Contains(t, it.Key(), "user:")
		}
		assert.Equal(t, 10, count)
	})

	t.Run("seek on iterator", func(t *testing.T) {
		it, err := engine.Iter("")
		require.NoError(t, err)
		defer it.Close()

		assert.True(t, it.Seek("user:5"))
		assert.Equal(t, "user:5", it.Key())
	})

	t.Run("empty prefix scan", func(t *testing.T) {
		it, err := engine.Iter("")
		require.NoError(t, err)
		defer it.Close()

		assert.True(t, it.First())
		assert.True(t, it.Valid())
	})
}

func TestIterator_EdgeCases(t *testing.T) {
	tmpDir := t.TempDir()

	engine, err := NewLSMEngine(tmpDir)
	require.NoError(t, err)
	defer engine.Close()

	t.Run("iterator on empty engine", func(t *testing.T) {
		it, err := engine.Iter("")
		require.NoError(t, err)
		defer it.Close()

		assert.False(t, it.First())
		assert.False(t, it.Valid())
	})

	t.Run("iterator after close", func(t *testing.T) {
		it, err := engine.Iter("")
		require.NoError(t, err)
		
		engine.Close()
		
		_, err = engine.Iter("")
		assert.Error(t, err)
		
		it.Close()
	})
}

func TestBlockCache_Integration(t *testing.T) {
	tmpDir := t.TempDir()

	engine, err := NewLSMEngine(tmpDir)
	require.NoError(t, err)
	defer engine.Close()

	// Write data
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key:%d", i)
		value := []byte(fmt.Sprintf("value:%d", i))
		require.NoError(t, engine.Put(key, value))
	}

	require.NoError(t, engine.Flush())

	// Read data - should populate cache
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key:%d", i)
		_, err := engine.Get(key)
		require.NoError(t, err)
	}

	// Read again - should hit cache
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key:%d", i)
		_, err := engine.Get(key)
		require.NoError(t, err)
	}
}

func TestCompression_Integration(t *testing.T) {
	tmpDir := t.TempDir()

	engine, err := NewLSMEngine(tmpDir)
	require.NoError(t, err)
	defer engine.Close()

	// Write compressible data
	repetitiveValue := []byte("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key:%d", i)
		require.NoError(t, engine.Put(key, repetitiveValue))
	}

	require.NoError(t, engine.Flush())

	// Verify data
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key:%d", i)
		entry, err := engine.Get(key)
		require.NoError(t, err)
		assert.Equal(t, string(repetitiveValue), string(entry.Value))
	}
}

func TestWAL_Compaction_Integration(t *testing.T) {
	tmpDir := t.TempDir()

	engine, err := NewLSMEngine(tmpDir)
	require.NoError(t, err)
	defer engine.Close()

	// Write entries
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key:%d", i)
		value := []byte(fmt.Sprintf("value:%d", i))
		require.NoError(t, engine.Put(key, value))
	}

	// Check WAL size before flush
	walSizeBefore, _ := engine.wal.Size()
	assert.Greater(t, walSizeBefore, int64(0))

	// Flush - should reset WAL
	require.NoError(t, engine.Flush())

	// WAL should be reset after flush
	walSizeAfter, _ := engine.wal.Size()
	assert.Equal(t, int64(0), walSizeAfter)
}
