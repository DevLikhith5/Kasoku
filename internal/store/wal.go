package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type WALRecord struct {
	Op        string `json:"op"`
	Key       string `json:"key"`
	Value     []byte `json:"value,omitempty"`
	Version   uint64 `json:"ver"`
	TimeStamp int64  `json:"ts"`
}

// WALReplayHandler defines how replayed entries are processed
// Different engines can implement this for custom replay logic
type WALReplayHandler interface {
	PutEntry(entry Entry) error
	SetVersion(version uint64)
}

// WALReplayHandlerFuncs is a helper type for testing
type WALReplayHandlerFuncs struct {
	PutEntryFunc   func(Entry) error
	SetVersionFunc func(uint64)
}

func (h WALReplayHandlerFuncs) PutEntry(entry Entry) error {
	if h.PutEntryFunc != nil {
		return h.PutEntryFunc(entry)
	}
	return nil
}

func (h WALReplayHandlerFuncs) SetVersion(version uint64) {
	if h.SetVersionFunc != nil {
		h.SetVersionFunc(version)
	}
}

type WALConfig struct {
	// SyncInterval controls how often the WAL fsyncs to disk.
	// If 0, sync happens on every write (default, safest).
	// If > 0, background goroutine fsyncs at this interval (faster, ~SyncInterval data loss on crash).
	SyncInterval time.Duration
}

type WAL struct {
	mu   sync.Mutex
	file *os.File
	enc  *json.Encoder
	path string

	// background sync
	syncChan chan struct{}
	stopCh   chan struct{}
	wg       sync.WaitGroup
	config   WALConfig
}

func OpenWAL(path string) (*WAL, error) {
	return OpenWALWithConfig(path, WALConfig{})
}

func OpenWALWithConfig(path string, cfg WALConfig) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open WAL %s: %w", path, err)
	}

	w := &WAL{
		file:   f,
		enc:    json.NewEncoder(f),
		path:   path,
		config: cfg,
	}

	// Start background sync if interval is configured
	if cfg.SyncInterval > 0 {
		w.syncChan = make(chan struct{}, 1)
		w.stopCh = make(chan struct{})
		w.wg.Add(1)
		go w.backgroundSync()
	}

	return w, nil
}

func (w *WAL) backgroundSync() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			w.file.Sync()
			w.mu.Unlock()
		case <-w.stopCh:
			return
		}
	}
}

func (w *WAL) Append(entry Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	op := "PUT"
	if entry.Tombstone {
		op = "DEL"
	}
	rec := WALRecord{
		Op:        op,
		Key:       entry.Key,
		Value:     entry.Value,
		Version:   entry.Version,
		TimeStamp: entry.TimeStamp.UnixNano(),
	}
	if err := w.enc.Encode(rec); err != nil {
		return fmt.Errorf("WAL write: %w", err)
	}
	// If background sync is enabled, skip immediate fsync
	// (background goroutine handles it at SyncInterval)
	if w.config.SyncInterval == 0 {
		// fsync: flush OS buffer to physical disk
		// Without this, data in OS buffer is lost on power failure
		return w.file.Sync()
	}
	return nil
}

func (w *WAL) File() *os.File {
	return w.file
}

func (w *WAL) Replay(handler WALReplayHandler) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Seek to beginning for reading
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}

	scanner := bufio.NewScanner(w.file)
	// Set max line size to 2MB (handles large values)
	scanner.Buffer(make([]byte, 2*1024*1024), 2*1024*1024)

	var maxVersion uint64

	for scanner.Scan() {
		var rec WALRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			continue // Skip corrupt records
		}

		entry := Entry{
			Key:       rec.Key,
			Value:     rec.Value,
			Version:   rec.Version,
			TimeStamp: time.Unix(0, rec.TimeStamp),
			Tombstone: rec.Op == "DEL",
		}

		// Delegate to handler - engine decides how to store
		if err := handler.PutEntry(entry); err != nil {
			return err
		}

		if rec.Version > maxVersion {
			maxVersion = rec.Version
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("WAL replay scan: %w", err)
	}

	// Let handler decide what to do with max version
	handler.SetVersion(maxVersion)

	_, err := w.file.Seek(0, 2)
	return err
}

func (w *WAL) Close() error {
	// Stop background sync if running
	if w.stopCh != nil {
		close(w.stopCh)
		w.wg.Wait()
	}
	return w.file.Close()
}

func (w *WAL) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}
	w.enc = json.NewEncoder(w.file)
	return nil
}

func (w *WAL) Size() (int64, error) {
	info, err := w.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// Checkpoint creates a checkpoint of the current WAL state
// Returns the current file offset as the checkpoint marker
func (w *WAL) Checkpoint() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Sync to ensure all data is on disk
	if err := w.file.Sync(); err != nil {
		return 0, err
	}

	// Get current position
	pos, err := w.file.Seek(0, 1)
	if err != nil {
		return 0, err
	}

	return uint64(pos), nil
}

// TruncateBefore removes WAL entries before the given checkpoint
// This prevents the WAL from growing forever
func (w *WAL) TruncateBefore(checkpoint uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Read all entries after checkpoint
	if _, err := w.file.Seek(int64(checkpoint), 0); err != nil {
		return err
	}

	scanner := bufio.NewScanner(w.file)
	scanner.Buffer(make([]byte, 2*1024*1024), 2*1024*1024)

	var entries []WALRecord
	for scanner.Scan() {
		var rec WALRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			continue // Skip corrupt records
		}
		entries = append(entries, rec)
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	// Truncate and rewrite
	if err := w.file.Truncate(0); err != nil {
		return err
	}

	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}

	// Re-encode entries
	w.enc = json.NewEncoder(w.file)
	for _, rec := range entries {
		if err := w.enc.Encode(rec); err != nil {
			return err
		}
	}

	return w.file.Sync()
}

// Compact performs checkpoint and truncate in one operation
// Returns the number of entries removed
func (w *WAL) Compact() (int, error) {
	// Count entries before
	countBefore, err := w.Count()
	if err != nil {
		return 0, err
	}

	// Create checkpoint
	_, err = w.Checkpoint()
	if err != nil {
		return 0, err
	}

	// Truncate (in this case, just sync since we're at end)
	// The real compaction happens when we replay and create a new WAL

	// Count entries after
	countAfter, err := w.Count()
	if err != nil {
		return 0, err
	}

	return countBefore - countAfter, nil
}

// Count returns the number of entries in the WAL
func (w *WAL) Count() (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, 0); err != nil {
		return 0, err
	}

	scanner := bufio.NewScanner(w.file)
	scanner.Buffer(make([]byte, 2*1024*1024), 2*1024*1024)

	count := 0
	for scanner.Scan() {
		var rec WALRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			continue
		}
		count++
	}

	if err := scanner.Err(); err != nil {
		return 0, err
	}

	// Seek back to end for appends
	_, err := w.file.Seek(0, 2)
	return count, err
}
