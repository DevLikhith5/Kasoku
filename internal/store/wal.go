package storage

import (
	"bufio"
	"encoding/binary"
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

type WALReplayHandler interface {
	PutEntry(entry Entry) error
	SetVersion(version uint64)
}

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
	SyncInterval time.Duration
	OnSyncError  func(error)
}

const (
	walOpPut = byte(0)
	walOpDel = byte(1)
)

type WAL struct {
	mu     sync.Mutex
	file   *os.File
	wbuf   *bufio.Writer
	path   string
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
		wbuf:   bufio.NewWriterSize(f, 32*1024),
		path:   path,
		config: cfg,
	}
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
			w.wbuf.Flush()
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
	keyLen := len(entry.Key)
	valLen := len(entry.Value)
	if w.wbuf.Available() < keyLen+valLen+25 {
		if err := w.wbuf.Flush(); err != nil {
			return fmt.Errorf("WAL flush: %w", err)
		}
	}
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(keyLen))
	w.wbuf.Write(hdr[:])
	w.wbuf.WriteString(entry.Key)
	binary.LittleEndian.PutUint32(hdr[:], uint32(valLen))
	w.wbuf.Write(hdr[:])
	if valLen > 0 {
		w.wbuf.Write(entry.Value)
	}
	var verBuf [8]byte
	binary.LittleEndian.PutUint64(verBuf[:], entry.Version)
	w.wbuf.Write(verBuf[:])
	var tsBuf [8]byte
	binary.LittleEndian.PutUint64(tsBuf[:], uint64(entry.TimeStamp.UnixNano()))
	w.wbuf.Write(tsBuf[:])
	op := walOpPut
	if entry.Tombstone {
		op = walOpDel
	}
	w.wbuf.WriteByte(op)
	if w.config.SyncInterval == 0 {
		w.wbuf.Flush()
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
	w.wbuf.Flush()
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}
	defer func() { _, _ = w.file.Seek(0, io.SeekEnd) }()

	var firstByte [1]byte
	if _, err := w.file.Read(firstByte[:]); err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	isJSON := firstByte[0] == '{'
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}

	var maxVersion uint64
	if isJSON {
		scanner := bufio.NewScanner(w.file)
		scanner.Buffer(make([]byte, 2*1024*1024), 2*1024*1024)
		for scanner.Scan() {
			var rec WALRecord
			if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
				continue
			}
			entry := Entry{
				Key: rec.Key, Value: rec.Value, Version: rec.Version,
				TimeStamp: time.Unix(0, rec.TimeStamp), Tombstone: rec.Op == "DEL",
			}
			if err := handler.PutEntry(entry); err != nil {
				return err
			}
			if rec.Version > maxVersion {
				maxVersion = rec.Version
			}
		}
	} else {
		for {
			var hdr [4]byte
			if _, err := io.ReadFull(w.file, hdr[:]); err != nil {
				if err == io.EOF {
					break
				}
				break
			}
			keyLen := binary.LittleEndian.Uint32(hdr[:])
			keyBuf := make([]byte, keyLen)
			if _, err := io.ReadFull(w.file, keyBuf); err != nil {
				break
			}
			if _, err := io.ReadFull(w.file, hdr[:]); err != nil {
				break
			}
			valLen := binary.LittleEndian.Uint32(hdr[:])
			var value []byte
			if valLen > 0 {
				value = make([]byte, valLen)
				if _, err := io.ReadFull(w.file, value); err != nil {
					break
				}
			}
			var verBuf [8]byte
			if _, err := io.ReadFull(w.file, verBuf[:]); err != nil {
				break
			}
			version := binary.LittleEndian.Uint64(verBuf[:])
			var tsBuf [8]byte
			if _, err := io.ReadFull(w.file, tsBuf[:]); err != nil {
				break
			}
			timestamp := int64(binary.LittleEndian.Uint64(tsBuf[:]))
			var opBuf [1]byte
			if _, err := io.ReadFull(w.file, opBuf[:]); err != nil {
				break
			}
			entry := Entry{
				Key: string(keyBuf), Value: value, Version: version,
				TimeStamp: time.Unix(0, timestamp), Tombstone: opBuf[0] == walOpDel,
			}
			if err := handler.PutEntry(entry); err != nil {
				return err
			}
			if version > maxVersion {
				maxVersion = version
			}
		}
	}
	handler.SetVersion(maxVersion)
	return nil
}

func (w *WAL) Close() error {
	if w.stopCh != nil {
		close(w.stopCh)
		w.wg.Wait()
	}
	w.mu.Lock()
	w.wbuf.Flush()
	w.mu.Unlock()
	return w.file.Close()
}

func (w *WAL) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	// Flush any buffered data before truncating
	w.wbuf.Flush()
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}
	w.wbuf.Reset(w.file)
	return nil
}

func (w *WAL) Size() (int64, error) {
	info, err := w.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (w *WAL) Checkpoint() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.wbuf.Flush()
	if err := w.file.Sync(); err != nil {
		return 0, err
	}
	pos, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	checkpoint := uint64(pos)
	if err := w.saveCheckpoint(checkpoint); err != nil {
		return 0, fmt.Errorf("save checkpoint: %w", err)
	}
	return checkpoint, nil
}

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

func (w *WAL) LoadCheckpoint() (uint64, error) {
	data, err := os.ReadFile(w.path + ".checkpoint")
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
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

func (w *WAL) TruncateBefore(checkpoint uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.truncateBeforeUnlocked(checkpoint)
}

func (w *WAL) truncateBeforeUnlocked(checkpoint uint64) error {
	if _, err := w.file.Seek(int64(checkpoint), io.SeekStart); err != nil {
		return err
	}
	scanner := bufio.NewScanner(w.file)
	scanner.Buffer(make([]byte, 2*1024*1024), 2*1024*1024)
	tmpPath := w.path + ".tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpEnc := json.NewEncoder(tmpFile)
	defer func() {
		tmpFile.Close()
		if err != nil {
			os.Remove(tmpPath)
		}
	}()
	for scanner.Scan() {
		var rec WALRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			continue
		}
		if err := tmpEnc.Encode(rec); err != nil {
			tmpFile.Close()
			return fmt.Errorf("write temp file: %w", err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan WAL: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		return fmt.Errorf("sync temp file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close original file: %w", err)
	}
	if err := os.Rename(tmpPath, w.path); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}
	f, err := os.OpenFile(w.path, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("reopen WAL: %w", err)
	}
	w.file = f
	w.wbuf.Reset(f)
	return nil
}

func (w *WAL) Compact() (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	scanner := bufio.NewScanner(w.file)
	scanner.Buffer(make([]byte, 2*1024*1024), 2*1024*1024)
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
			continue
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
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return 0, fmt.Errorf("sync temp file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return 0, fmt.Errorf("close temp file: %w", err)
	}
	if err := w.file.Close(); err != nil {
		os.Remove(tmpPath)
		return 0, fmt.Errorf("close original file: %w", err)
	}
	if err := os.Rename(tmpPath, w.path); err != nil {
		os.Remove(tmpPath)
		return 0, fmt.Errorf("rename temp file: %w", err)
	}
	f, err := os.OpenFile(w.path, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return 0, fmt.Errorf("reopen WAL: %w", err)
	}
	w.file = f
	w.wbuf.Reset(f)
	return entryCount, nil
}

func (w *WAL) Count() (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.wbuf.Flush()
	if _, err := w.file.Seek(0, 0); err != nil {
		return 0, err
	}
	var firstByte [1]byte
	if _, err := w.file.Read(firstByte[:]); err != nil {
		if err == io.EOF {
			return 0, nil
		}
		return 0, err
	}
	isJSON := firstByte[0] == '{'
	if _, err := w.file.Seek(0, 0); err != nil {
		return 0, err
	}
	count := 0
	if isJSON {
		scanner := bufio.NewScanner(w.file)
		scanner.Buffer(make([]byte, 2*1024*1024), 2*1024*1024)
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
	} else {
		for {
			var hdr [4]byte
			if _, err := io.ReadFull(w.file, hdr[:]); err != nil {
				if err == io.EOF {
					break
				}
				break
			}
			keyLen := binary.LittleEndian.Uint32(hdr[:])
			if _, err := w.file.Seek(int64(keyLen), io.SeekCurrent); err != nil {
				break
			}
			if _, err := io.ReadFull(w.file, hdr[:]); err != nil {
				break
			}
			valLen := binary.LittleEndian.Uint32(hdr[:])
			if _, err := w.file.Seek(int64(valLen+8+8+1), io.SeekCurrent); err != nil {
				break
			}
			count++
		}
	}
	_, err := w.file.Seek(0, io.SeekEnd)
	return count, err
}
