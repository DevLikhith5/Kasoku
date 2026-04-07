package lsmengine

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	storage "github.com/DevLikhith5/kasoku/internal/store"
)

type LSMEngine struct {
	mu        sync.RWMutex
	active    *MemTable
	immutable []*MemTable // queue of memtables waiting to flush
	wal       *storage.WAL
	levels    [][]*SSTableReader
	version   atomic.Uint64
	dir       string
	closed    atomic.Bool
	flushCh   chan struct{}
	compCh    chan struct{}
	flushDone chan struct{} // signaled when a flush completes, used for backpressure
	wg        sync.WaitGroup
	config    LSMConfig
}

type LSMConfig struct {
	MemTableSize        int64 //soft limit for memtable
	MaxMemtableBytes    int64
	WALSyncInterval     time.Duration
	CompactionThreshold int
	L0SizeThreshold     int64 //hard limit for memtable
	BloomFPRate         float64
	LevelRatio          float64
}

// PutEntry implements WALReplayHandler for LSMEngine
func (e *LSMEngine) PutEntry(entry storage.Entry) error {
	e.active.Put(entry)
	return nil
}

// SetVersion implements WALReplayHandler for LSMEngine
func (e *LSMEngine) SetVersion(version uint64) {
	e.version.Store(version)
}

func NewLSMEngine(dir string) (*LSMEngine, error) {
	return NewLSMEngineWithConfig(dir, LSMConfig{})
}

func NewLSMEngineWithConfig(dir string, cfg LSMConfig) (*LSMEngine, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// Apply defaults for zero values
	if cfg.MemTableSize <= 0 {
		cfg.MemTableSize = DefaultMemTableSize
	}
	if cfg.MaxMemtableBytes <= 0 {
		cfg.MaxMemtableBytes = DefaultMemTableSize * 4 // 256MB
	}
	if cfg.CompactionThreshold <= 0 {
		cfg.CompactionThreshold = 4
	}
	if cfg.L0SizeThreshold <= 0 {
		cfg.L0SizeThreshold = DefaultMemTableSize * 2 // 128MB
	}
	if cfg.BloomFPRate <= 0 {
		cfg.BloomFPRate = 0.01
	}
	if cfg.LevelRatio <= 0 {
		cfg.LevelRatio = 2.0
	}

	wal, err := storage.OpenWALWithConfig(filepath.Join(dir, "wal.log"), storage.WALConfig{
		SyncInterval: cfg.WALSyncInterval,
	})
	if err != nil {
		return nil, err
	}

	e := &LSMEngine{
		active:    NewMemTable(cfg.MemTableSize),
		wal:       wal,
		dir:       dir,
		flushCh:   make(chan struct{}, 1),
		compCh:    make(chan struct{}, 1),
		flushDone: make(chan struct{}, 1),
		config:    cfg,
	}

	if err := e.loadSSTables(); err != nil {
		return nil, err
	}

	// Load persisted version counter before WAL replay
	if err := e.loadVersion(); err != nil {
		return nil, err
	}

	if err := e.replayWAL(); err != nil {
		return nil, err
	}

	// Flush any replayed WAL entries to SSTables and reset the WAL.
	// This ensures clean state and prevents WAL from growing across restarts.
	if err := e.flushMemTable(); err != nil {
		return nil, err
	}

	// Persist the version counter so it survives WAL reset
	if err := e.saveVersion(); err != nil {
		return nil, err
	}

	e.wg.Add(2)
	go e.flushLoop()
	go e.compactLoop()

	return e, nil
}

func (e *LSMEngine) Put(key string, value []byte) error {
	if e.closed.Load() {
		return storage.ErrEngineClosed
	}

	if len(key) > storage.MaxKeyLen {
		return storage.ErrKeyTooLong
	}
	if len(value) > storage.MaxValueLen {
		return storage.ErrValueTooLarge
	}

	entry := storage.Entry{
		Key:       key,
		Value:     value,
		Version:   e.version.Add(1),
		TimeStamp: time.Now(),
	}
	//
	if err := e.wal.Append(entry); err != nil {
		return err
	}

	e.mu.Lock()
	e.active.Put(entry)
	full := e.active.IsFull()
	// Hard limit: block writes if active exceeds L0SizeThreshold
	overHardLimit := e.active.Size() >= e.config.L0SizeThreshold
	// System cap: block if total memtable memory exceeds MaxMemtableBytes
	totalMem := e.active.Size()
	for _, mem := range e.immutable {
		totalMem += mem.Size()
	}
	overSystemCap := totalMem >= e.config.MaxMemtableBytes
	e.mu.Unlock()

	if full {
		select {
		case e.flushCh <- struct{}{}:
		default:
		}
	}

	// Backpressure: wait for at least one flush to complete
	for overHardLimit || overSystemCap {
		if overSystemCap {
			fmt.Printf("[BACKPRESSURE] total memtable memory exceeded system cap (%d bytes), blocking writes\n", e.config.MaxMemtableBytes)
		} else {
			fmt.Printf("[BACKPRESSURE] active memtable exceeded hard limit (%d bytes), blocking writes\n", e.config.L0SizeThreshold)
		}
		<-e.flushDone
		// Re-check after flush
		e.mu.RLock()
		totalMem = e.active.Size()
		for _, mem := range e.immutable {
			totalMem += mem.Size()
		}
		overHardLimit = e.active.Size() >= e.config.L0SizeThreshold
		overSystemCap = totalMem >= e.config.MaxMemtableBytes
		e.mu.RUnlock()
	}

	return nil
}

func (e *LSMEngine) Get(key string) (storage.Entry, error) {
	if e.closed.Load() {
		return storage.Entry{}, storage.ErrEngineClosed
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if entry, ok := e.active.Get(key); ok {
		if entry.Tombstone {
			return storage.Entry{}, storage.ErrKeyNotFound
		}
		return entry, nil
	}

	for _, mem := range e.immutable {
		if entry, ok := mem.Get(key); ok {
			if entry.Tombstone {
				return storage.Entry{}, storage.ErrKeyNotFound
			}
			return entry, nil
		}
	}

	for _, level := range e.levels {
		for _, sst := range level {

			if !sst.filter.MightContain([]byte(key)) {
				continue
			}

			entry, err := sst.Get(key)
			if err == storage.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return storage.Entry{}, err
			}

			if entry.Tombstone {
				return storage.Entry{}, storage.ErrKeyNotFound
			}

			return entry, nil
		}
	}

	return storage.Entry{}, storage.ErrKeyNotFound
}

func (e *LSMEngine) flushLoop() {
	defer e.wg.Done()

	for range e.flushCh {
		if e.closed.Load() {
			return
		}

		if err := e.flushMemTable(); err != nil {
			fmt.Println("flush error:", err)
		}
	}
}

// maxFilesForLevel returns the max SSTables allowed at a given level.
// Creates a pyramid using level_ratio: L0=4, L1=4*ratio, L2=4*ratio^2, ...
func (e *LSMEngine) maxFilesForLevel(level int) int {
	ratio := e.config.LevelRatio
	result := float64(e.config.CompactionThreshold)
	for range level {
		result *= ratio
	}
	return int(result)
}

func (e *LSMEngine) compactLoop() {
	defer e.wg.Done()

	for range e.compCh {
		if e.closed.Load() {
			return
		}

		// Collect levels that need compaction (no hard cap — pyramid grows)
		e.mu.RLock()
		var levelsToCompact []int
		for level := 0; level < len(e.levels); level++ {
			if len(e.levels[level]) >= e.maxFilesForLevel(level) {
				levelsToCompact = append(levelsToCompact, level)
			} else {
				break // Stop at first level that doesn't need compaction
			}
		}
		e.mu.RUnlock()

		if len(levelsToCompact) == 0 {
			continue
		}

		// Compact non-adjacent levels concurrently
		// Even levels (0, 2, 4...) can compact concurrently
		// Odd levels (1, 3, 5...) can compact concurrently
		// But we must compact L0 first before L1, L1 before L2, etc.

		// For safety and simplicity: compact sequentially from L0 upwards
		// This ensures correctness - L0 compaction must complete before L1 compaction
		for _, level := range levelsToCompact {
			e.compactLevel(level)
		}
	}
}

func (e *LSMEngine) compactLevel(level int) {
	e.mu.Lock()
	if level >= len(e.levels) || len(e.levels[level]) < e.maxFilesForLevel(level) {
		e.mu.Unlock()
		return
	}

	// Take a snapshot of SSTables to compact, but DO NOT remove them yet.
	// They must remain visible to Get() during the entire merge process.
	toMerge := make([]*SSTableReader, len(e.levels[level]))
	copy(toMerge, e.levels[level])
	e.mu.Unlock()

	fmt.Printf("[COMPACTION] level %d: merging %d SSTables\n", level, len(toMerge))

	// Collect all entries from all SSTables using merge sort
	merged := mergeSSTables(toMerge)

	// Deduplicate: keep only the latest version of each key
	// Remove old tombstones
	const tombstoneTTL = 24 * time.Hour
	deduped := deduplicateEntries(merged, tombstoneTTL)

	if len(deduped) == 0 {
		// All entries were old tombstones — atomically remove, then delete files
		e.mu.Lock()
		e.levels[level] = nil
		e.mu.Unlock()
		for _, sst := range toMerge {
			sst.Close()
			os.Remove(sst.path)
		}
		fmt.Printf("[COMPACTION] level %d: all tombstones expired, cleaned up\n", level)
		return
	}

	// Write merged result as new SSTable
	nextLevel := level + 1
	sstPath := filepath.Join(e.dir,
		fmt.Sprintf("L%d_%d.sst", nextLevel, time.Now().UnixNano()))
	writer, err := NewSSTableWriter(sstPath, len(deduped), e.config.BloomFPRate)
	if err != nil {
		fmt.Printf("[COMPACTION] error creating writer: %v\n", err)
		return
	}

	for _, entry := range deduped {
		if err := writer.WriteEntry(entry); err != nil {
			fmt.Printf("[COMPACTION] error writing entry: %v\n", err)
			os.Remove(sstPath)
			return
		}
	}

	if err := writer.Finalize(); err != nil {
		fmt.Printf("[COMPACTION] error finalizing: %v\n", err)
		os.Remove(sstPath)
		return
	}

	// Open new SSTable
	reader, err := OpenSSTable(sstPath)
	if err != nil {
		fmt.Printf("[COMPACTION] error opening new SSTable: %v\n", err)
		os.Remove(sstPath)
		return
	}

	// ATOMIC SWAP: remove old SSTables and install new one in a single lock
	e.mu.Lock()
	e.levels[level] = nil // remove old SSTables from source level
	for len(e.levels) <= nextLevel {
		e.levels = append(e.levels, nil)
	}
	e.levels[nextLevel] = append(e.levels[nextLevel], reader)
	e.mu.Unlock()

	// Now safe to close and delete old files (no longer referenced by e.levels)
	for _, sst := range toMerge {
		sst.Close()
		os.Remove(sst.path)
	}

	fmt.Printf("[COMPACTION] done: level %d -> %d (%d entries)\n", level, nextLevel, len(deduped))
}

// Flush forces a flush of the active memtable to disk
func (e *LSMEngine) Flush() error {
	return e.flushMemTable()
}

func (e *LSMEngine) flushMemTable() error {

	e.mu.Lock()
	if e.active.Len() == 0 {
		e.mu.Unlock()
		// Signal flushDone even for no-op so blocked writers can proceed
		select {
		case e.flushDone <- struct{}{}:
		default:
		}
		return nil
	}

	// Rotate: push active to immutable queue, create new active
	e.immutable = append(e.immutable, e.active)
	e.active = NewMemTable(e.config.MemTableSize)
	e.mu.Unlock()

	// Signal flushDone so any blocked writers can retry
	select {
	case e.flushDone <- struct{}{}:
	default:
	}

	// Flush all immutable memtables in order
	for {
		e.mu.Lock()
		if len(e.immutable) == 0 {
			e.mu.Unlock()
			break
		}
		mem := e.immutable[0]
		e.immutable = e.immutable[1:]
		e.mu.Unlock()

		entries := mem.Entries()
		if len(entries) == 0 {
			continue
		}

		sstPath := filepath.Join(
			e.dir,
			fmt.Sprintf("L0_%d.sst", time.Now().UnixNano()),
		)

		writer, err := NewSSTableWriter(sstPath, len(entries), e.config.BloomFPRate)
		if err != nil {
			return err
		}

		for _, entry := range entries {
			if err := writer.WriteEntry(entry); err != nil {
				return err
			}
		}

		if err := writer.Finalize(); err != nil {
			return err
		}

		reader, err := OpenSSTable(sstPath)
		if err != nil {
			return err
		}

		e.mu.Lock()
		if len(e.levels) == 0 {
			e.levels = append(e.levels, nil)
		}
		e.levels[0] = append([]*SSTableReader{reader}, e.levels[0]...)
		e.mu.Unlock()
	}

	// WAL is only safe to reset after ALL pending memtables are flushed
	if err := e.wal.Reset(); err != nil {
		return err
	}

	// Trigger compaction if L0 has too many SSTables
	e.mu.RLock()
	l0Count := len(e.levels[0])
	e.mu.RUnlock()
	// Trigger compaction if L0 has too many SSTables.
	// Skip this if the engine is closing — compCh is closed and sends would panic.
	if !e.closed.Load() && l0Count >= e.maxFilesForLevel(0) {
		select {
		case e.compCh <- struct{}{}:
		default:
		}
	}

	return nil
}

func (e *LSMEngine) loadSSTables() error {
	files, err := os.ReadDir(e.dir)
	if err != nil {
		return err
	}

	levelMap := make(map[int][]*SSTableReader)

	for _, f := range files {
		if f.IsDir() {
			continue
		}

		name := f.Name()

		if filepath.Ext(name) != ".sst" {
			continue
		}

		var level int
		_, err := fmt.Sscanf(name, "L%d_", &level)
		if err != nil {
			continue
		}

		path := filepath.Join(e.dir, name)

		reader, err := OpenSSTable(path)
		if err != nil {
			return err
		}

		levelMap[level] = append(levelMap[level], reader)
	}

	if len(levelMap) == 0 {
		return nil
	}

	maxLevel := 0
	for lvl := range levelMap {
		if lvl > maxLevel {
			maxLevel = lvl
		}
	}

	e.levels = make([][]*SSTableReader, maxLevel+1)

	for lvl, readers := range levelMap {
		sort.Slice(readers, func(i, j int) bool {
			return readers[i].path > readers[j].path
		})
		e.levels[lvl] = readers
	}

	return nil
}

func (e *LSMEngine) replayWAL() error {

	return e.wal.Replay(e)
}

func (e *LSMEngine) Delete(key string) error {
	if e.closed.Load() {
		return storage.ErrEngineClosed
	}

	entry := storage.Entry{
		Key:       key,
		Value:     nil,
		Version:   e.version.Add(1),
		TimeStamp: time.Now(),
		Tombstone: true,
	}

	if err := e.wal.Append(entry); err != nil {
		return err
	}

	e.mu.Lock()
	e.active.Put(entry)
	full := e.active.IsFull()
	overHardLimit := e.active.Size() >= e.config.L0SizeThreshold
	totalMem := e.active.Size()
	for _, mem := range e.immutable {
		totalMem += mem.Size()
	}
	overSystemCap := totalMem >= e.config.MaxMemtableBytes
	e.mu.Unlock()

	if full {
		select {
		case e.flushCh <- struct{}{}:
		default:
		}
	}

	for overHardLimit || overSystemCap {
		if overSystemCap {
			fmt.Printf("[BACKPRESSURE] total memtable memory exceeded system cap (%d bytes), blocking writes\n", e.config.MaxMemtableBytes)
		} else {
			fmt.Printf("[BACKPRESSURE] active memtable exceeded hard limit (%d bytes), blocking writes\n", e.config.L0SizeThreshold)
		}
		<-e.flushDone
		e.mu.RLock()
		totalMem = e.active.Size()
		for _, mem := range e.immutable {
			totalMem += mem.Size()
		}
		overHardLimit = e.active.Size() >= e.config.L0SizeThreshold
		overSystemCap = totalMem >= e.config.MaxMemtableBytes
		e.mu.RUnlock()
	}

	return nil
}

func (e *LSMEngine) Scan(prefix string) ([]storage.Entry, error) {
	if e.closed.Load() {
		return nil, storage.ErrEngineClosed
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// Use map to track latest version of each key
	result := make(map[string]storage.Entry)

	// Scan SSTables from oldest to newest (L0 first, then L1, etc.)
	// Within each level, SSTables are stored newest-first, so iterate in reverse
	for _, level := range e.levels {
		for i := len(level) - 1; i >= 0; i-- {
			sst := level[i]
			entries, err := sst.Scan(prefix)
			if err != nil {
				continue
			}
			for _, entry := range entries {
				if !entry.Tombstone {
					result[entry.Key] = entry
				} else {
					delete(result, entry.Key)
				}
			}
		}
	}

	// Scan immutable memtables (newest first - last in queue is newest)
	for i := len(e.immutable) - 1; i >= 0; i-- {
		for _, entry := range e.immutable[i].Scan(prefix) {
			if !entry.Tombstone {
				result[entry.Key] = entry
			} else {
				delete(result, entry.Key)
			}
		}
	}

	// Scan active memtable (newest - takes precedence)
	for _, entry := range e.active.Scan(prefix) {
		if !entry.Tombstone {
			result[entry.Key] = entry
		} else {
			delete(result, entry.Key)
		}
	}

	// Convert map to sorted slice
	entries := make([]storage.Entry, 0, len(result))
	for _, entry := range result {
		entries = append(entries, entry)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})

	return entries, nil
}

func (e *LSMEngine) Keys() ([]string, error) {
	entries, err := e.Scan("")
	if err != nil {
		return nil, err
	}

	keys := make([]string, len(entries))
	for i, entry := range entries {
		keys[i] = entry.Key
	}
	return keys, nil
}

// Iter creates a new iterator for range scans with cursor
// If prefix is empty, iterates over all keys
func (e *LSMEngine) Iter(prefix string) (*Iterator, error) {
	if e.closed.Load() {
		return nil, storage.ErrEngineClosed
	}

	entries, err := e.Scan(prefix)
	if err != nil {
		return nil, err
	}

	return NewIterator(entries, prefix), nil
}

func (e *LSMEngine) Stats() storage.EngineStats {
	// Get unique key count via Scan (handles tombstones and versioning correctly)
	// We can't hold the lock while calling Scan (it also acquires the lock),
	// so we do a lock-free snapshot approach.
	uniqueKeys := e.countUniqueKeys()

	e.mu.RLock()
	var memBytes int64

	if e.active != nil {
		memBytes += e.active.Size()
	}
	for _, mem := range e.immutable {
		memBytes += mem.Size()
	}

	// Only count SSTable files (exclude WAL since it's transient and will be cleaned up)
	var diskBytes int64

	for _, level := range e.levels {
		for _, sst := range level {
			if sst.path != "" {
				info, err := os.Stat(sst.path)
				if err == nil {
					diskBytes += info.Size()
				}
			}
		}
	}
	e.mu.RUnlock()

	return storage.EngineStats{
		KeyCount:    uniqueKeys,
		DiskBytes:   diskBytes,
		MemBytes:    memBytes,
		BloomFPRate: 0.01,
	}
}

// countUniqueKeys returns the count of unique, non-tombstoned keys
func (e *LSMEngine) countUniqueKeys() int64 {
	if e.closed.Load() {
		return 0
	}

	// Use Scan() which properly handles tombstones across all levels and memtables
	entries, err := e.Scan("")
	if err != nil {
		return 0
	}

	return int64(len(entries))
}

func (e *LSMEngine) Close() error {
	if e.closed.Swap(true) {
		return nil
	}

	// Flush remaining data BEFORE closing channels.
	// flushMemTable() may send on compCh to trigger compaction —
	// doing this after close(compCh) causes a "send on closed channel" panic.
	if err := e.flushMemTable(); err != nil {
		fmt.Println("final flush error:", err)
	}

	// Persist version counter before closing
	if err := e.saveVersion(); err != nil {
		fmt.Println("save version error:", err)
	}

	close(e.flushCh)
	close(e.compCh)
	e.wg.Wait()

	e.mu.Lock()
	defer e.mu.Unlock()

	for _, level := range e.levels {
		for _, sst := range level {
			sst.Close()
		}
	}

	return e.wal.Close()
}

// TriggerCompaction manual triggers the background compaction process
func (e *LSMEngine) TriggerCompaction() {
	select {
	case e.compCh <- struct{}{}:
	default:
	}
}

// saveVersion persists the current version counter to disk
func (e *LSMEngine) saveVersion() error {
	versionPath := filepath.Join(e.dir, "VERSION")
	tmpPath := versionPath + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(f, "%d", e.version.Load())
	if err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, versionPath)
}

// loadVersion restores the version counter from disk
func (e *LSMEngine) loadVersion() error {
	versionPath := filepath.Join(e.dir, "VERSION")
	data, err := os.ReadFile(versionPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No version file yet, start from 0
		}
		return err
	}
	var version uint64
	_, err = fmt.Sscanf(string(data), "%d", &version)
	if err != nil {
		return err
	}
	e.version.Store(version)
	return nil
}

// mergeSSTables merges multiple SSTables using merge sort
// Returns all entries sorted by key, with all versions preserved
func mergeSSTables(ssts []*SSTableReader) []storage.Entry {
	if len(ssts) == 0 {
		return nil
	}

	// Collect all entries from all SSTables
	var allEntries []storage.Entry
	for _, sst := range ssts {
		entries, _ := sst.Scan("")
		allEntries = append(allEntries, entries...)
	}

	if len(allEntries) == 0 {
		return nil
	}

	// Sort by key, then by version (descending - newest first)
	sort.Slice(allEntries, func(i, j int) bool {
		if allEntries[i].Key != allEntries[j].Key {
			return allEntries[i].Key < allEntries[j].Key
		}
		return allEntries[i].Version > allEntries[j].Version
	})

	return allEntries
}

// deduplicateEntries keeps the latest version of each key
// and removes tombstones older than ttl
func deduplicateEntries(entries []storage.Entry, tombstoneTTL time.Duration) []storage.Entry {
	if len(entries) == 0 {
		return nil
	}

	var result []storage.Entry
	i := 0
	for i < len(entries) {
		// Find all versions of this key
		j := i + 1
		for j < len(entries) && entries[j].Key == entries[i].Key {
			j++
		}
		// entries[i:j] are all versions of the same key
		// They are sorted by version descending - first one is latest
		latest := entries[i]

		// Skip tombstones that are old enough
		if latest.Tombstone && time.Since(latest.TimeStamp) > tombstoneTTL {
			i = j
			continue // garbage collect this tombstone
		}

		result = append(result, latest)
		i = j
	}

	return result
}
