package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
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

	// OnSyncError is called when background fsync fails.
	// If not set, errors are logged to stderr.
	// Use this to handle errors gracefully (e.g., stop accepting writes).
	OnSyncError func(error)
}

type WAL struct {
	mu   sync.Mutex
	file *os.File
	enc  *json.Encoder
	path string

	// background sync
	stopCh chan struct{}
	wg     sync.WaitGroup
	config WALConfig
}

func OpenWAL(path string) (*WAL, error) {
	return OpenWALWithConfig(path, WALConfig{})
}

func OpenWALWithConfig(path string, cfg WALConfig) (*WAL, error) {
	if cfg.SyncInterval < 0 {
		return nil, fmt.Errorf("WAL SyncInterval cannot be negative")
	}

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
			if err := w.file.Sync(); err != nil {
				if w.config.OnSyncError != nil {
					w.config.OnSyncError(err)
				} else {
					fmt.Fprintf(os.Stderr, "WAL fsync error: %v\n", err)
				}
			}
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

// File returns the underlying file handle.
// Deprecated: This exposes internal state and may be removed in a future version.
// Most operations should use Append(), Replay(), or other WAL methods directly.
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

	// Ensure file position is reset to end on exit (for subsequent Appends)
	defer func() {
		_, _ = w.file.Seek(0, io.SeekEnd)
	}()

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

	return nil
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
	// Sync to ensure truncate is persisted
	if err := w.file.Sync(); err != nil {
		return err
	}
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
// The checkpoint is also persisted to a .checkpoint file
func (w *WAL) Checkpoint() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Sync to ensure all data is on disk
	if err := w.file.Sync(); err != nil {
		return 0, err
	}

	// Get current position
	pos, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}

	checkpoint := uint64(pos)

	// Persist checkpoint to file
	if err := w.saveCheckpoint(checkpoint); err != nil {
		return 0, fmt.Errorf("save checkpoint: %w", err)
	}

	return checkpoint, nil
}

// saveCheckpoint persists the checkpoint marker to disk
func (w *WAL) saveCheckpoint(checkpoint uint64) error {
	tmpPath := w.path + ".checkpoint.tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%d", checkpoint)
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

	return os.Rename(tmpPath, w.path+".checkpoint")
}

// LoadCheckpoint loads the last checkpoint marker from disk
// Returns 0 if no checkpoint exists
func (w *WAL) LoadCheckpoint() (uint64, error) {
	checkpointPath := w.path + ".checkpoint"
	data, err := os.ReadFile(checkpointPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil // No checkpoint yet
		}
		return 0, err
	}

	var checkpoint uint64
	_, err = fmt.Sscanf(string(data), "%d", &checkpoint)
	if err != nil {
		return 0, fmt.Errorf("parse checkpoint: %w", err)
	}

	return checkpoint, nil
}

// TruncateBefore removes WAL entries before the given checkpoint
// This prevents the WAL from growing forever
// Uses a temp file to avoid loading all entries into memory
// Assumes caller holds the lock (use truncateBeforeUnlocked for internal calls)
func (w *WAL) TruncateBefore(checkpoint uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.truncateBeforeUnlocked(checkpoint)
}

// truncateBeforeUnlocked is the internal version that assumes lock is held
func (w *WAL) truncateBeforeUnlocked(checkpoint uint64) error {
	// Read all entries after checkpoint
	if _, err := w.file.Seek(int64(checkpoint), io.SeekStart); err != nil {
		return err
	}

	scanner := bufio.NewScanner(w.file)
	scanner.Buffer(make([]byte, 2*1024*1024), 2*1024*1024)

	// Create temp file for atomic write
	tmpPath := w.path + ".tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpEnc := json.NewEncoder(tmpFile)
	defer func() {
		tmpFile.Close()
		if err != nil {
			os.Remove(tmpPath) // Clean up on error
		}
	}()

	for scanner.Scan() {
		var rec WALRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			continue // Skip corrupt records
		}
		if err := tmpEnc.Encode(rec); err != nil {
			tmpFile.Close()
			return fmt.Errorf("write temp file: %w", err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan WAL: %w", err)
	}

	// Sync temp file before replacing
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		return fmt.Errorf("sync temp file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}

	// Close original file before rename (required on Windows)
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close original file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, w.path); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}

	// Reopen file
	f, err := os.OpenFile(w.path, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("reopen WAL: %w", err)
	}
	w.file = f
	w.enc = json.NewEncoder(f)

	return nil
}

// Compact rewrites the WAL to remove gaps and reclaim space
// Returns the number of entries kept after compaction
// Uses a temp file to avoid loading all entries into memory
func (w *WAL) Compact() (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Seek to beginning for reading
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	scanner := bufio.NewScanner(w.file)
	scanner.Buffer(make([]byte, 2*1024*1024), 2*1024*1024)

	// Create temp file for atomic write
	tmpPath := w.path + ".compact.tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return 0, fmt.Errorf("create temp file: %w", err)
	}
	tmpEnc := json.NewEncoder(tmpFile)

	entryCount := 0
	for scanner.Scan() {
		var rec WALRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			continue // Skip corrupt records
		}
		if err := tmpEnc.Encode(rec); err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			return 0, fmt.Errorf("write temp file: %w", err)
		}
		entryCount++
	}

	if err := scanner.Err(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return 0, fmt.Errorf("scan WAL: %w", err)
	}

	// Sync temp file before replacing
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return 0, fmt.Errorf("sync temp file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return 0, fmt.Errorf("close temp file: %w", err)
	}

	// Close original file before rename
	if err := w.file.Close(); err != nil {
		os.Remove(tmpPath)
		return 0, fmt.Errorf("close original file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, w.path); err != nil {
		os.Remove(tmpPath)
		return 0, fmt.Errorf("rename temp file: %w", err)
	}

	// Reopen file
	f, err := os.OpenFile(w.path, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return 0, fmt.Errorf("reopen WAL: %w", err)
	}
	w.file = f
	w.enc = json.NewEncoder(f)

	return entryCount, nil
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
	_, err := w.file.Seek(0, io.SeekEnd)
	return count, err
}
