package storage

import (
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type HashMapEngine struct {
	mu sync.RWMutex
	data map[string]Entry
	version atomic.Uint64
	closed atomic.Bool
	wal     *WAL
}

// PutEntry implements WALReplayHandler for HashMapEngine
func (h *HashMapEngine) PutEntry(entry Entry) error {
	h.data[entry.Key] = entry
	return nil
}

// SetVersion implements WALReplayHandler for HashMapEngine
func (h *HashMapEngine) SetVersion(version uint64) {
	h.version.Store(version)
}
func NewHashmapEngine(walPath string) (*HashMapEngine, error) {
	engine :=  &HashMapEngine{
		data: make(map[string]Entry),
	}
	wal, err := OpenWAL(walPath)
	if err != nil { return nil, err }
	engine.wal = wal

	if err := wal.Replay(engine); err != nil {
		return nil, err
	}
	return engine, nil
}

func (h *HashMapEngine) validate(key string, value []byte)error {
	if h.closed.Load() {
		return ErrEngineClosed
	}

	if len(key) > MaxKeyLen {
		return ErrKeyTooLong
	}
	
	if len(value) > MaxValueLen {
		return ErrValueTooLarge
	}
	return nil
}

func (h *HashMapEngine) Put(key string, value []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.validate(key,value); err != nil {
		return err
	}
	entry := Entry{
		Key: key,
		Value: value,
		Version: h.version.Add(1),
		TimeStamp: time.Now(),
	}
	if h.wal != nil {
		if err := h.wal.Append(entry); err != nil {
			return err
		}
	}

	h.data[key] = entry
	return nil
}

func (h *HashMapEngine) Get(key string) (Entry, error) {
	if h.closed.Load() { return Entry{}, ErrEngineClosed }
	h.mu.RLock()
	defer h.mu.RUnlock()

	entry, ok := h.data[key]
	if !ok || entry.Tombstone {
		return Entry{}, ErrKeyNotFound
	}
	return entry, nil
}

func (h *HashMapEngine) Delete(key string) error {
    if err := h.validate(key, nil); err != nil { return err }
    h.mu.Lock()
    defer h.mu.Unlock()
    existing, ok := h.data[key]
    if !ok || existing.Tombstone { return ErrKeyNotFound }
    tombstone := Entry{
        Key: key, Version: h.version.Add(1),
        TimeStamp: time.Now(), Tombstone: true,
    }
    if h.wal != nil {
        if err := h.wal.Append(tombstone); err != nil { return err }
    }
    h.data[key] = tombstone
    return nil
}
 


func (h *HashMapEngine) Scan(prefix string)([]Entry, error){
	if h.closed.Load(){ return nil, ErrEngineClosed}
	h.mu.RLock()
	defer h.mu.RUnlock()
	var result []Entry
	for k,v := range h.data {
		if strings.HasPrefix(k,prefix) && !v.Tombstone {
			result = append(result, v)
		}
	}
	sort.Slice(result, func(i,j int) bool {
		return result[i].Key < result[j].Key
	})
	return result,nil
}

func (h *HashMapEngine) Keys() ([]string,error) {
	if h.closed.Load() { return nil, ErrEngineClosed }
	h.mu.RLock()
	defer h.mu.RUnlock()

	var keys []string
	for k,v := range h.data {
		if !v.Tombstone { keys = append(keys, k)}
	}
	sort.Strings(keys)
	return keys, nil
}


func (h *HashMapEngine) Stats() EngineStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var totalBytes int64
	for _, v := range h.data {
		totalBytes += int64(len(v.Key) + len(v.Value))
	}

	return EngineStats{
		KeyCount: int64(len(h.data)),
		MemBytes: totalBytes,
		DiskBytes: 0,
		BloomFPRate: 0,
	}
}

func (h *HashMapEngine) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	h.closed.Store(true)
	if h.wal != nil {
		return h.wal.Close()
	}
	return nil
}