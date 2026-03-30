package storage

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runEngineTests(t *testing.T, engine StorageEngine) {
	t.Helper()
	t.Run("put and get", func(t *testing.T) {
		require.NoError(t, engine.Put("hello", []byte("world")))
		entry, err := engine.Get("hello")
		require.NoError(t, err)
		assert.Equal(t, "world", string(entry.Value))
		assert.Equal(t, "hello", string(entry.Key))
		assert.False(t, entry.Tombstone)
	})

	t.Run("get mssing key", func(t *testing.T) {
		_, err := engine.Get("non-existent")
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("overwrite key", func(t *testing.T) {
		require.NoError(t, engine.Put("key", []byte("v1")))
		require.NoError(t, engine.Put("key", []byte("v2")))
		entry, err := engine.Get("key")
		require.NoError(t, err)

		assert.Equal(t, "v2", string(entry.Value))
	})

	t.Run("delete key", func(t *testing.T) {
		require.NoError(t, engine.Put("del-me", []byte("value")))
		require.NoError(t, engine.Delete("del-me"))
		_, err := engine.Get("del-me")
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})
	t.Run("delete missing key", func(t *testing.T) {
		err := engine.Delete("never-existed")
		assert.ErrorIs(t, err, ErrKeyNotFound)

	})

	t.Run("scan prefix", func(t *testing.T) {
		engine.Put("user:1", []byte("Alice"))
		engine.Put("user:2", []byte("Bob"))
		engine.Put("session:1", []byte("xyz"))
		entries, err := engine.Scan("user:")
		require.NoError(t, err)
		assert.Equal(t, 2, len(entries))
		assert.Equal(t, "user:1", entries[0].Key)
		assert.Equal(t, "user:2", entries[1].Key)
	})

	t.Run("version increments", func(t *testing.T) {
		engine.Put("versioned", []byte("v1"))
		e1, _ := engine.Get("versioned")
		engine.Put("versioned", []byte("v2"))
		e2, _ := engine.Get("versioned")
		assert.Greater(t, e2.Version, e1.Version)
	})

	t.Run("key too long", func(t *testing.T) {
		longKey := string(make([]byte, MaxKeyLen+1))
		err := engine.Put(longKey, []byte("value"))
		assert.ErrorIs(t, err, ErrKeyTooLong)
	})

	t.Run("100 keys", func(t *testing.T) {
		for i := range 100 {
			key := fmt.Sprintf("bulk:%04d", i)
			require.NoError(t, engine.Put(key, []byte(fmt.Sprintf("val:%d", i))))
		}

		for i := range 100 {
			key := fmt.Sprintf("bulk:%04d", i)
			entry, err := engine.Get(key)
			require.NoError(t, err, "missing key: %s", key)
			assert.Equal(t, fmt.Sprintf("val:%d", i), string(entry.Value))
		}
	})
}

func TestHashMapEngine(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")

	engine, err := NewHashmapEngine(walPath)
	require.NoError(t, err)
	defer engine.Close()

	runEngineTests(t, engine)
}
