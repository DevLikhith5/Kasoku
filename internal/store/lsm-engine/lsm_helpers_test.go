package lsmengine

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	storage "github.com/DevLikhith5/kasoku/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeSSTables(t *testing.T) {
	tmpDir := t.TempDir()

	// Create first SSTable
	path1 := filepath.Join(tmpDir, "L0_1.sst")
	writer1, err := NewSSTableWriter(path1, 10, 0.01)
	require.NoError(t, err)
	writer1.WriteEntry(storage.Entry{Key: "a", Value: []byte("v1"), Version: 1, TimeStamp: time.Now()})
	writer1.WriteEntry(storage.Entry{Key: "b", Value: []byte("v1"), Version: 1, TimeStamp: time.Now()})
	writer1.Finalize()

	// Create second SSTable
	path2 := filepath.Join(tmpDir, "L0_2.sst")
	writer2, err := NewSSTableWriter(path2, 10, 0.01)
	require.NoError(t, err)
	writer2.WriteEntry(storage.Entry{Key: "c", Value: []byte("v1"), Version: 1, TimeStamp: time.Now()})
	writer2.WriteEntry(storage.Entry{Key: "d", Value: []byte("v1"), Version: 1, TimeStamp: time.Now()})
	writer2.Finalize()

	// Open SSTables
	reader1, err := OpenSSTable(path1)
	require.NoError(t, err)
	defer reader1.Close()

	reader2, err := OpenSSTable(path2)
	require.NoError(t, err)
	defer reader2.Close()

	// Merge
	merged := mergeSSTables([]*SSTableReader{reader1, reader2})

	// Should have all entries sorted by key
	assert.Equal(t, 4, len(merged))
	assert.Equal(t, "a", merged[0].Key)
	assert.Equal(t, "b", merged[1].Key)
	assert.Equal(t, "c", merged[2].Key)
	assert.Equal(t, "d", merged[3].Key)
}

func TestMergeSSTables_MultipleVersions(t *testing.T) {
	tmpDir := t.TempDir()

	// Create first SSTable with older versions
	path1 := filepath.Join(tmpDir, "L0_1.sst")
	writer1, err := NewSSTableWriter(path1, 10, 0.01)
	require.NoError(t, err)
	writer1.WriteEntry(storage.Entry{Key: "key", Value: []byte("old"), Version: 1, TimeStamp: time.Now()})
	writer1.Finalize()

	// Create second SSTable with newer version
	path2 := filepath.Join(tmpDir, "L0_2.sst")
	writer2, err := NewSSTableWriter(path2, 10, 0.01)
	require.NoError(t, err)
	writer2.WriteEntry(storage.Entry{Key: "key", Value: []byte("new"), Version: 2, TimeStamp: time.Now()})
	writer2.Finalize()

	reader1, err := OpenSSTable(path1)
	require.NoError(t, err)
	defer reader1.Close()

	reader2, err := OpenSSTable(path2)
	require.NoError(t, err)
	defer reader2.Close()

	// Merge
	merged := mergeSSTables([]*SSTableReader{reader1, reader2})

	// Should have both versions, newest first
	assert.Equal(t, 2, len(merged))
	assert.Equal(t, "key", merged[0].Key)
	assert.Equal(t, uint64(2), merged[0].Version)
	assert.Equal(t, []byte("new"), merged[0].Value)
	assert.Equal(t, uint64(1), merged[1].Version)
	assert.Equal(t, []byte("old"), merged[1].Value)
}

func TestMergeSSTables_Empty(t *testing.T) {
	// Empty slice
	result := mergeSSTables([]*SSTableReader{})
	assert.Empty(t, result)

	// Nil slice
	result = mergeSSTables(nil)
	assert.Empty(t, result)
}

func TestDeduplicateEntries(t *testing.T) {
	now := time.Now()

	// Entries sorted by key, then by version descending (as mergeSSTables returns)
	entries := []storage.Entry{
		{Key: "key1", Value: []byte("v3"), Version: 3, TimeStamp: now},
		{Key: "key1", Value: []byte("v2"), Version: 2, TimeStamp: now},
		{Key: "key1", Value: []byte("v1"), Version: 1, TimeStamp: now},
		{Key: "key2", Value: []byte("v1"), Version: 1, TimeStamp: now},
		{Key: "key3", Value: []byte("v2"), Version: 2, TimeStamp: now},
		{Key: "key3", Value: []byte("v1"), Version: 1, TimeStamp: now},
	}

	// Deduplicate with long TTL (keep all non-old tombstones)
	result := deduplicateEntries(entries, 24*time.Hour)

	// Should keep only latest version of each key
	assert.Equal(t, 3, len(result))

	// Find each key
	keyMap := make(map[string]storage.Entry)
	for _, e := range result {
		keyMap[e.Key] = e
	}

	assert.Equal(t, []byte("v3"), keyMap["key1"].Value)
	assert.Equal(t, []byte("v1"), keyMap["key2"].Value)
	assert.Equal(t, []byte("v2"), keyMap["key3"].Value)
}

func TestDeduplicateEntries_WithTombstones(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-48 * time.Hour) // 2 days ago

	// Entries sorted by key, then version descending
	entries := []storage.Entry{
		{Key: "key1", Value: nil, Version: 2, TimeStamp: oldTime, Tombstone: true},
		{Key: "key1", Value: []byte("v1"), Version: 1, TimeStamp: oldTime},
		{Key: "key2", Value: nil, Version: 2, TimeStamp: now, Tombstone: true},
		{Key: "key2", Value: []byte("v1"), Version: 1, TimeStamp: now},
	}

	// Deduplicate with 24 hour TTL
	result := deduplicateEntries(entries, 24*time.Hour)

	// key1's tombstone is old (>24h) - garbage collected, key1 not in result
	// key2's tombstone is recent (<24h) - kept
	// So we get 1 entry (key2 tombstone)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "key2", result[0].Key)
	assert.True(t, result[0].Tombstone)
}

func TestDeduplicateEntries_AllTombstonesExpired(t *testing.T) {
	oldTime := time.Now().Add(-48 * time.Hour)

	entries := []storage.Entry{
		{Key: "key1", Value: nil, Version: 1, TimeStamp: oldTime, Tombstone: true},
		{Key: "key2", Value: nil, Version: 1, TimeStamp: oldTime, Tombstone: true},
	}

	// All tombstones are old - should be garbage collected
	result := deduplicateEntries(entries, 24*time.Hour)

	assert.Empty(t, result)
}

func TestDeduplicateEntries_Empty(t *testing.T) {
	result := deduplicateEntries([]storage.Entry{}, 24*time.Hour)
	assert.Empty(t, result)

	result = deduplicateEntries(nil, 24*time.Hour)
	assert.Empty(t, result)
}

func TestDeduplicateEntries_SingleEntry(t *testing.T) {
	entries := []storage.Entry{
		{Key: "single", Value: []byte("value"), Version: 1, TimeStamp: time.Now()},
	}

	result := deduplicateEntries(entries, 24*time.Hour)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "single", result[0].Key)
}

func TestLSMEngine_PutEntry_Replay(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewLSMEngine(dir)
	require.NoError(t, err)
	defer engine.Close()

	// Test PutEntry directly (used during WAL replay)
	entry := storage.Entry{Key: "test", Value: []byte("value"), Version: 1, TimeStamp: time.Now()}
	err = engine.PutEntry(entry)
	require.NoError(t, err)

	// Should be in active memtable
	gotEntry, found := engine.active.Get("test")
	assert.True(t, found)
	assert.Equal(t, []byte("value"), gotEntry.Value)
}

func TestLSMEngine_SetVersion_Replay(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewLSMEngine(dir)
	require.NoError(t, err)
	defer engine.Close()

	// Test SetVersion directly (used during WAL replay)
	engine.SetVersion(100)

	// Put should use version starting from 100
	engine.Put("key", []byte("value"))
	entry, _ := engine.Get("key")
	assert.Greater(t, entry.Version, uint64(100))
}

func TestLSMEngine_FlushMemTable_Empty(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewLSMEngine(dir)
	require.NoError(t, err)
	defer engine.Close()

	// Flush without any data - should not crash
	err = engine.flushMemTable()
	assert.NoError(t, err)
}

func TestLSMEngine_FlushMemTable_WithData(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewLSMEngine(dir)
	require.NoError(t, err)
	defer engine.Close()

	// Add data to active memtable
	engine.Put("key1", []byte("value1"))
	engine.Put("key2", []byte("value2"))

	// Force flush to ensure data is in SSTables
	require.NoError(t, engine.Flush())

	// Data should still be accessible
	entry, err := engine.Get("key1")
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), entry.Value)
}

func TestLSMEngine_LoadSSTables_Empty(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewLSMEngine(dir)
	require.NoError(t, err)
	defer engine.Close()

	// Empty directory - should work fine
	assert.NoError(t, err)
	assert.Empty(t, engine.levels)
}

func TestLSMEngine_LoadSSTables_WithFiles(t *testing.T) {
	tmpDir := t.TempDir()

	// Create SSTable files
	path1 := filepath.Join(tmpDir, "L0_1.sst")
	writer1, err := NewSSTableWriter(path1, 10, 0.01)
	require.NoError(t, err)
	writer1.WriteEntry(storage.Entry{Key: "a", Value: []byte("v"), Version: 1, TimeStamp: time.Now()})
	writer1.Finalize()

	path2 := filepath.Join(tmpDir, "L1_1.sst")
	writer2, err := NewSSTableWriter(path2, 10, 0.01)
	require.NoError(t, err)
	writer2.WriteEntry(storage.Entry{Key: "b", Value: []byte("v"), Version: 1, TimeStamp: time.Now()})
	writer2.Finalize()

	// Create engine with existing SSTables
	engine, err := NewLSMEngine(tmpDir)
	require.NoError(t, err)
	defer engine.Close()

	time.Sleep(100 * time.Millisecond)

	// Should have loaded SSTables
	engine.mu.RLock()
	levelCount := len(engine.levels)
	engine.mu.RUnlock()

	assert.GreaterOrEqual(t, levelCount, 1)
}

func TestLSMEngine_ReplayWAL(t *testing.T) {
	dir := t.TempDir()

	// Create engine and add data
	engine1, err := NewLSMEngine(dir)
	require.NoError(t, err)
	engine1.Put("key1", []byte("value1"))
	engine1.Put("key2", []byte("value2"))
	engine1.Close()

	// Create new engine - should replay WAL
	engine2, err := NewLSMEngine(dir)
	require.NoError(t, err)
	defer engine2.Close()

	time.Sleep(100 * time.Millisecond)

	// Data should be recovered
	entry, err := engine2.Get("key1")
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), entry.Value)
}

func TestLSMEngine_CompactLevel(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewLSMEngine(dir)
	require.NoError(t, err)
	defer engine.Close()

	// Add some data
	for i := 0; i < 50; i++ {
		engine.Put(string(rune('a'+i%26)), []byte("value"))
	}

	time.Sleep(300 * time.Millisecond)

	// Manually trigger compaction
	engine.compactLevel(0)

	// Should not crash - compaction runs in background
	time.Sleep(200 * time.Millisecond)
}

func TestLSMEngine_CompactLevel_NotEnoughSSTables(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewLSMEngine(dir)
	require.NoError(t, err)
	defer engine.Close()

	// Add minimal data
	engine.Put("key", []byte("value"))
	time.Sleep(100 * time.Millisecond)

	// Try to compact - should not do anything (need 4+ SSTables)
	engine.compactLevel(0)

	// Should not crash
}

func TestLSMEngine_FlushLoop(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewLSMEngine(dir)
	require.NoError(t, err)
	defer engine.Close()

	// Add data to trigger flush
	for i := 0; i < 100; i++ {
		engine.Put(string(rune('a'+i%26)), []byte("value"))
	}

	// Wait for flush loop
	time.Sleep(500 * time.Millisecond)

	// Data should still be accessible
	entry, err := engine.Get("a")
	require.NoError(t, err)
	assert.NotNil(t, entry.Value)
}

func TestLSMEngine_CompactLoop(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewLSMEngine(dir)
	require.NoError(t, err)
	defer engine.Close()

	// Add data
	for i := 0; i < 100; i++ {
		engine.Put(string(rune('a'+i%26)), []byte("value"))
	}

	time.Sleep(300 * time.Millisecond)

	// Trigger compaction
	select {
	case engine.compCh <- struct{}{}:
	default:
	}

	time.Sleep(300 * time.Millisecond)

	// Should not crash
}

func TestSSTableWriter_ErrorCases(t *testing.T) {
	// Try to create writer in invalid directory
	_, err := NewSSTableWriter("/nonexistent/dir/test.sst", 10, 0.01)
	assert.Error(t, err)
}

func TestSSTableReader_ErrorCases(t *testing.T) {
	// Try to open non-existent file
	_, err := OpenSSTable("/nonexistent/file.sst")
	assert.Error(t, err)

	// Try to open invalid file
	tmpDir := t.TempDir()
	invalidPath := filepath.Join(tmpDir, "invalid.sst")
	os.WriteFile(invalidPath, []byte("garbage"), 0644)

	_, err = OpenSSTable(invalidPath)
	assert.Error(t, err)
}

func TestMemTable_WriteAfterFull(t *testing.T) {
	// Very small memtable
	mt := NewMemTable(50)

	// Fill it
	mt.Put(storage.Entry{Key: "key1", Value: []byte("value1data"), Version: 1, TimeStamp: time.Now()})
	mt.Put(storage.Entry{Key: "key2", Value: []byte("value2data"), Version: 1, TimeStamp: time.Now()})

	// Check if full (depends on actual size calculation)
	_ = mt.IsFull()

	// Can still add entries (no hard limit enforced)
	mt.Put(storage.Entry{Key: "key3", Value: []byte("value3"), Version: 1, TimeStamp: time.Now()})
	assert.Greater(t, mt.Len(), 0)
}

func TestEngineStats(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewLSMEngine(dir)
	require.NoError(t, err)
	defer engine.Close()

	stats := engine.Stats()

	// Verify stats structure
	assert.GreaterOrEqual(t, stats.KeyCount, int64(0))
	assert.GreaterOrEqual(t, stats.DiskBytes, int64(0))
	assert.GreaterOrEqual(t, stats.MemBytes, int64(0))
	assert.GreaterOrEqual(t, stats.BloomFPRate, 0.0)
}

func TestLSMEngine_Scan_MemtableAndSSTable(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewLSMEngine(dir)
	require.NoError(t, err)
	defer engine.Close()

	// Add data to memtable
	engine.Put("mem:key1", []byte("memvalue1"))
	engine.Put("mem:key2", []byte("memvalue2"))

	// Add data that might be in SSTable after flush
	for i := 0; i < 100; i++ {
		engine.Put(string(rune('a'+i%26)), []byte("sstvalue"))
	}

	time.Sleep(300 * time.Millisecond)

	// Scan memtable keys
	entries, err := engine.Scan("mem:")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(entries), 2)
}

func TestLSMEngine_Get_FromAllLevels(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewLSMEngine(dir)
	require.NoError(t, err)
	defer engine.Close()

	// Add data
	engine.Put("key1", []byte("value1"))
	engine.Put("key2", []byte("value2"))

	time.Sleep(200 * time.Millisecond)

	// Get should search memtable, immutable, and all SSTable levels
	entry, err := engine.Get("key1")
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), entry.Value)
}

func TestLSMEngine_Get_TombstoneInSSTable(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewLSMEngine(dir)
	require.NoError(t, err)
	defer engine.Close()

	// Add and delete
	engine.Put("del-key", []byte("value"))
	engine.Delete("del-key")

	time.Sleep(200 * time.Millisecond)

	// Should return not found
	_, err = engine.Get("del-key")
	assert.Error(t, err)
}

func TestLSMEngine_Get_BloomFilterOptimization(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewLSMEngine(dir)
	require.NoError(t, err)
	defer engine.Close()

	// Add some data
	for i := 0; i < 10; i++ {
		engine.Put(string(rune('a'+i)), []byte("value"))
	}

	time.Sleep(200 * time.Millisecond)

	// Get non-existent key - bloom filter should quickly rule it out
	_, err = engine.Get("zzzzz-not-here")
	assert.Error(t, err)
}
