package lsmengine

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	storage "github.com/DevLikhith5/kasoku/internal/store"
	"github.com/golang/snappy"
)

const (
	DefaultBlockSize = 4096 // 4KB blocks
)

type indexEntry struct {
	Key        string `json:"k"`
	Offset     int64  `json:"o"`
	Size       int32  `json:"s"`
	Compressed bool   `json:"c"` // whether data block is compressed
}

type SSTableWriter struct {
	file      *os.File
	filter    *BloomFilter
	index     []indexEntry
	offset    int64
	count     int
	blockSize int
	blockBuf  []byte
	compress  bool
}

func NewSSTableWriter(path string, expectedEntries int, bloomFPRate float64) (*SSTableWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil,
			fmt.Errorf("create sstable %s: %w", path, err)
	}
	return &SSTableWriter{
		file:      f,
		filter:    NewBloomFilter(expectedEntries, bloomFPRate),
		blockSize: DefaultBlockSize,
		compress:  true, // enable compression by default
	}, nil
}

func (w *SSTableWriter) WriteEntry(entry storage.Entry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	var lenBuf [4]byte
	var finalData []byte
	compressed := false

	// Compress data block
	if w.compress {
		compressedData := snappy.Encode(nil, data)
		// Only use compression if it actually saves space
		if len(compressedData) < len(data) {
			finalData = compressedData
			compressed = true
		} else {
			finalData = data
		}
	} else {
		finalData = data
	}

	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(finalData)))

	if _, err := w.file.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := w.file.Write(finalData); err != nil {
		return err
	}

	w.filter.Add([]byte(entry.Key))
	w.index = append(w.index, indexEntry{
		Key:        entry.Key,
		Offset:     w.offset,
		Size:       int32(len(finalData) + 4), // +4 for length prefix
		Compressed: compressed,
	})
	w.offset += int64(len(finalData) + 4)
	w.count++
	return nil
}

func (w *SSTableWriter) Finalize() error {
	// Defensive: ensure index is sorted (MemTable should already be sorted)
	//Safety net required even though this is redundant, negligible delta increase in tc than disk io
	sort.Slice(w.index, func(i, j int) bool {
		return w.index[i].Key < w.index[j].Key
	})

	indexOffset := w.offset

	indexData, err := json.Marshal(w.index)
	if err != nil {
		return err
	}

	if _, err := w.file.Write(indexData); err != nil {
		return err
	}

	bloomOffset := indexOffset + int64(len(indexData))

	bloomData := w.filter.Encode()
	if _, err := w.file.Write(bloomData); err != nil {
		return err
	}

	var footer [32]byte

	binary.LittleEndian.PutUint64(footer[0:], uint64(indexOffset))
	binary.LittleEndian.PutUint64(footer[8:], uint64(len(indexData)))
	binary.LittleEndian.PutUint64(footer[16:], uint64(bloomOffset))
	binary.LittleEndian.PutUint64(footer[24:], uint64(len(bloomData)))

	if _, err := w.file.Write(footer[:]); err != nil {
		return err
	}

	if err := w.file.Sync(); err != nil {
		return err
	}

	return w.file.Close()
}

func (w *SSTableWriter) Close() error {
	return w.file.Close()
}

type SSTableReader struct {
	file       *os.File
	filter     *BloomFilter
	index      []indexEntry
	path       string
	blockCache *BlockCache
	mu         sync.RWMutex
}

// BlockCache is an LRU cache for data blocks
type BlockCache struct {
	mu          sync.RWMutex
	cache       map[string][]byte
	keys        []string
	maxSize     int
	currentSize int
}

// NewBlockCache creates an LRU block cache
func NewBlockCache(maxBlocks int) *BlockCache {
	return &BlockCache{
		cache:   make(map[string][]byte),
		keys:    make([]string, 0, maxBlocks),
		maxSize: maxBlocks,
	}
}

// Get retrieves a block from cache
func (bc *BlockCache) Get(key string) ([]byte, bool) {
	bc.mu.Lock() // Write lock needed because we update LRU order
	defer bc.mu.Unlock()

	data, ok := bc.cache[key]
	if !ok {
		return nil, false
	}

	// Move to end (most recently used)
	for i, k := range bc.keys {
		if k == key {
			bc.keys = append(bc.keys[:i], bc.keys[i+1:]...)
			bc.keys = append(bc.keys, key)
			break
		}
	}

	return data, true
}

// Put adds a block to cache with LRU eviction
func (bc *BlockCache) Put(key string, data []byte) {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	// If already exists, update and move to end
	if _, ok := bc.cache[key]; ok {
		bc.cache[key] = data
		// Move to end (most recently used)
		for i, k := range bc.keys {
			if k == key {
				bc.keys = append(bc.keys[:i], bc.keys[i+1:]...)
				break
			}
		}
		bc.keys = append(bc.keys, key)
		return
	}

	// Evict oldest if at capacity
	if len(bc.keys) >= bc.maxSize {
		oldest := bc.keys[0]
		delete(bc.cache, oldest)
		bc.keys = bc.keys[1:]
	}

	bc.cache[key] = data
	bc.keys = append(bc.keys, key)
}

// NewBlockCache creates a global block cache (can be shared across SSTables)
var globalBlockCache *BlockCache
var blockCacheOnce sync.Once

// InitBlockCache initializes the global block cache
func InitBlockCache(maxBlocks int) {
	blockCacheOnce.Do(func() {
		globalBlockCache = NewBlockCache(maxBlocks)
	})
}

// GetBlockCache returns the global block cache
func GetBlockCache() *BlockCache {
	if globalBlockCache == nil {
		globalBlockCache = NewBlockCache(1024) // default 1024 blocks
	}
	return globalBlockCache
}

// OpenSSTable loads the footer, bloom filter, and index from disk
// Data blocks are NOT loaded until Get() is called — lazy loading
func OpenSSTable(path string) (*SSTableReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open sstable %s: %w", path, err)
	}
	r := &SSTableReader{
		file:       f,
		path:       path,
		blockCache: GetBlockCache(),
	}

	// Read 32-byte footer from end of file
	var footer [32]byte
	if _, err := f.ReadAt(footer[:], mustFileSize(f)-32); err != nil {
		return nil, err
	}
	indexOffset := int64(binary.LittleEndian.Uint64(footer[0:]))
	indexSize := int64(binary.LittleEndian.Uint64(footer[8:]))
	bloomOffset := int64(binary.LittleEndian.Uint64(footer[16:]))
	bloomSize := int64(binary.LittleEndian.Uint64(footer[24:]))

	// Load bloom filter
	bloomData := make([]byte, bloomSize)
	if _, err := f.ReadAt(bloomData, bloomOffset); err != nil {
		return nil, err
	}
	r.filter, err = DecodeBloomFilter(bloomData)
	if err != nil {
		return nil, err
	}

	// Load index
	indexData := make([]byte, indexSize)
	if _, err := f.ReadAt(indexData, indexOffset); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(indexData, &r.index); err != nil {
		return nil, err
	}

	return r, nil
}

// Get retrieves an entry by key
// Returns storage.ErrKeyNotFound if key is not in this SSTable
func (r *SSTableReader) Get(key string) (storage.Entry, error) {
	// Step 1: Bloom filter check — zero disk reads if key absent
	if !r.filter.MightContain([]byte(key)) {
		return storage.Entry{}, storage.ErrKeyNotFound
	}

	// Step 2: Binary search the index
	idx := sort.Search(len(r.index), func(i int) bool {
		return r.index[i].Key >= key
	})
	if idx >= len(r.index) || r.index[idx].Key != key {
		return storage.Entry{}, storage.ErrKeyNotFound // bloom false positive
	}

	// Step 3: Check block cache first
	entry := r.index[idx]
	cacheKey := fmt.Sprintf("%s:%d", r.path, entry.Offset)

	var data []byte
	var cached bool

	r.mu.RLock()
	data, cached = r.blockCache.Get(cacheKey)
	r.mu.RUnlock()

	if !cached {
		// Step 4: Read the data block from disk
		buf := make([]byte, entry.Size)
		if _, err := r.file.ReadAt(buf, entry.Offset); err != nil {
			return storage.Entry{}, err
		}
		data = buf

		// Cache the block
		r.blockCache.Put(cacheKey, data)
	}

	// Step 5: Decompress if needed
	if entry.Compressed {
		decompressed, err := snappy.Decode(nil, data[4:])
		if err != nil {
			return storage.Entry{}, err
		}
		var result storage.Entry
		if err := json.Unmarshal(decompressed, &result); err != nil {
			return storage.Entry{}, err
		}
		return result, nil
	}

	// Not compressed
	var result storage.Entry
	if err := json.Unmarshal(data[4:], &result); err != nil {
		return storage.Entry{}, err
	}
	return result, nil
}

// Scan returns all entries with the given prefix
func (r *SSTableReader) Scan(prefix string) ([]storage.Entry, error) {
	// Binary search for start of prefix range in index
	start := sort.Search(len(r.index), func(i int) bool {
		return r.index[i].Key >= prefix
	})

	var result []storage.Entry
	for i := start; i < len(r.index); i++ {
		if !strings.HasPrefix(r.index[i].Key, prefix) {
			break
		}

		entry := r.index[i]
		cacheKey := fmt.Sprintf("%s:%d", r.path, entry.Offset)

		var data []byte
		var cached bool

		r.mu.RLock()
		data, cached = r.blockCache.Get(cacheKey)
		r.mu.RUnlock()

		if !cached {
			// Read the data block from disk
			buf := make([]byte, entry.Size)
			if _, err := r.file.ReadAt(buf, entry.Offset); err != nil {
				continue
			}
			data = buf

			// Cache the block
			r.blockCache.Put(cacheKey, data)
		}

		// Decompress if needed
		var s storage.Entry
		if entry.Compressed {
			decompressed, err := snappy.Decode(nil, data[4:])
			if err != nil {
				continue
			}
			if err := json.Unmarshal(decompressed, &s); err != nil {
				continue
			}
		} else {
			if err := json.Unmarshal(data[4:], &s); err != nil {
				continue
			}
		}
		result = append(result, s)
	}
	return result, nil
}

func (r *SSTableReader) Close() error {
	return r.file.Close()
}

func mustFileSize(f *os.File) int64 {
	info, err := f.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}
