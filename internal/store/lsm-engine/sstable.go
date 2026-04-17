package lsmengine

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	storage "github.com/DevLikhith5/kasoku/internal/store"
	"github.com/golang/snappy"
)

const (
	DefaultBlockSize   = 64 * 1024  // 64KB blocks (optimized for SSD sequential I/O)
	MaxBlockSize       = 256 * 1024 // 256KB max block size
	BinaryEncodingMode = true       // BINARY ONLY - for maximum performance
)

type indexEntry struct {
	Key        string `json:"k"`
	Offset     int64  `json:"o"`
	Size       int32  `json:"s"`
	Compressed bool   `json:"c"` // whether data block is compressed
	Binary     bool   `json:"b"` // whether binary encoding was used (NEW - for decode compatibility)
}

type SSTableWriter struct {
	file      *os.File
	filter    *BloomFilter
	index     []indexEntry
	offset    int64
	count     int
	blockSize int

	compress bool
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

// Magic byte to identify binary format - ensures we can distinguish from any legacy data
const binaryMagic = 0xBF

// encodeEntryBinary encodes an entry to binary format for fast serialization
// Format: [magic:1][keyLen:4][keyBytes][valLen:4][valueBytes][version:8][timestamp:8][tombstone:1][vcLen:4][vcData]
func encodeEntryBinary(entry storage.Entry) []byte {
	keyLen := len(entry.Key)
	valLen := len(entry.Value)

	// Calculate VectorClock size
	vcLen := 0
	var vcData []byte
	if entry.VectorClock != nil && len(entry.VectorClock) > 0 {
		vcData = encodeVectorClock(entry.VectorClock)
		vcLen = len(vcData)
	}

	// Fixed overhead: 1 (magic) + 4 (keyLen) + 4 (valLen) + 8 (version) + 8 (timestamp) + 1 (tombstone) + 4 (vcLen)
	// Plus variable: keyLen + valLen + vcLen
	buf := make([]byte, 1+4+keyLen+4+valLen+8+8+1+4+vcLen)
	pos := 0

	// Magic byte (1 byte)
	buf[pos] = binaryMagic
	pos++

	// Key length (4 bytes)
	binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(keyLen))
	pos += 4

	// Key bytes
	if keyLen > 0 {
		copy(buf[pos:pos+keyLen], entry.Key)
		pos += keyLen
	}

	// Value length (4 bytes)
	binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(valLen))
	pos += 4

	// Value bytes
	if valLen > 0 {
		copy(buf[pos:pos+valLen], entry.Value)
		pos += valLen
	}

	// Version (8 bytes)
	binary.LittleEndian.PutUint64(buf[pos:pos+8], entry.Version)
	pos += 8

	// Timestamp (8 bytes)
	ts := entry.TimeStamp.UnixNano()
	if ts == 0 {
		ts = time.Now().UnixNano()
	}
	binary.LittleEndian.PutUint64(buf[pos:pos+8], uint64(ts))
	pos += 8

	// Tombstone (1 byte)
	if entry.Tombstone {
		buf[pos] = 1
	}
	pos++

	// VectorClock length (4 bytes)
	binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(vcLen))
	pos += 4

	// VectorClock data
	if vcLen > 0 {
		copy(buf[pos:pos+vcLen], vcData)
	}

	return buf
}

func encodeVectorClock(vc storage.VectorClock) []byte {
	if vc == nil || len(vc) == 0 {
		return nil
	}

	// Format: [numEntries:4][key1Len:2][key1][val1:8][key2Len:2][key2][val2:8]...
	var buf []byte
	buf = append(buf, make([]byte, 4)...)

	count := 0
	for k, v := range vc {
		keyLen := uint16(len(k))
		buf = append(buf, make([]byte, 2)...)
		binary.LittleEndian.PutUint16(buf[len(buf)-2:], keyLen)
		buf = append(buf, k...)

		valBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(valBuf, v)
		buf = append(buf, valBuf...)
		count++
	}

	binary.LittleEndian.PutUint32(buf[:4], uint32(count))
	return buf
}

func decodeVectorClock(data []byte) storage.VectorClock {
	if len(data) < 4 {
		return nil
	}

	count := binary.LittleEndian.Uint32(data[:4])
	if count == 0 {
		return nil
	}

	vc := make(storage.VectorClock, count)
	pos := 4

	for i := uint32(0); i < count; i++ {
		if pos+2 > len(data) {
			break
		}
		keyLen := binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2

		if pos+int(keyLen)+8 > len(data) {
			break
		}
		key := string(data[pos : pos+int(keyLen)])
		pos += int(keyLen)

		val := binary.LittleEndian.Uint64(data[pos : pos+8])
		pos += 8

		vc[key] = val
	}

	return vc
}

// decodeEntryBinary decodes binary entry back to storage.Entry
// Requires data to start with magic byte
func decodeEntryBinary(data []byte) (storage.Entry, error) {
	var entry storage.Entry

	if len(data) < 1 {
		return entry, fmt.Errorf("data too short for magic byte")
	}

	// Check magic byte
	if data[0] != binaryMagic {
		return entry, fmt.Errorf("not binary format: magic byte mismatch")
	}

	pos := 1 // Skip magic

	// Read key length
	if len(data) < pos+4 {
		return entry, fmt.Errorf("data too short for key length")
	}
	keyLen := binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	// Read key
	if keyLen > 0 {
		if len(data) < pos+int(keyLen) {
			return entry, fmt.Errorf("data too short for key")
		}
		entry.Key = string(data[pos : pos+int(keyLen)])
		pos += int(keyLen)
	}

	// Read value length
	if len(data) < pos+4 {
		return entry, fmt.Errorf("data too short for value length")
	}
	valLen := binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	// Read value
	if valLen > 0 {
		if len(data) < pos+int(valLen) {
			return entry, fmt.Errorf("data too short for value")
		}
		entry.Value = data[pos : pos+int(valLen)]
		pos += int(valLen)
	}

	// Read version
	if len(data) < pos+8 {
		return entry, fmt.Errorf("data too short for version")
	}
	entry.Version = binary.LittleEndian.Uint64(data[pos:])
	pos += 8

	// Read timestamp
	if len(data) < pos+8 {
		return entry, fmt.Errorf("data too short for timestamp")
	}
	ts := binary.LittleEndian.Uint64(data[pos:])
	entry.TimeStamp = time.Unix(0, int64(ts))
	pos += 8

	// Read tombstone
	if pos < len(data) {
		entry.Tombstone = data[pos] == 1
		pos++
	}

	// Read vector clock length
	if pos+4 <= len(data) {
		vcLen := binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4
		if vcLen > 0 && pos+int(vcLen) <= len(data) {
			entry.VectorClock = decodeVectorClock(data[pos : pos+int(vcLen)])
		}
	}

	return entry, nil
}

func (w *SSTableWriter) WriteEntry(entry storage.Entry) error {
	var data []byte
	var err error

	// Use binary encoding for better performance
	if BinaryEncodingMode {
		data = encodeEntryBinary(entry)
	} else {
		data, err = json.Marshal(entry)
		if err != nil {
			return err
		}
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
		Binary:     BinaryEncodingMode, // Track encoding mode for read path
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

type BlockCache struct {
	mu      sync.Mutex
	cache   map[string][]byte
	keys    []string
	maxSize int
}

func NewBlockCache(maxBlocks int) *BlockCache {
	return &BlockCache{
		cache:   make(map[string][]byte),
		keys:    make([]string, 0, maxBlocks),
		maxSize: maxBlocks,
	}
}

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
var globalBlockCacheOnce sync.Once

func InitBlockCache(sizeBytes int64) {
	globalBlockCacheOnce.Do(func() {
		if globalBlockCache == nil {
			maxBlocks := max(int(sizeBytes)/DefaultBlockSize, 1)
			globalBlockCache = NewBlockCache(maxBlocks)
		}
	})
}

const (
	DefaultBlockCacheSize = 1024 * 1024 * 1024 // 1GB default block cache
)

func GetBlockCache() *BlockCache {
	globalBlockCacheOnce.Do(func() {
		if globalBlockCache == nil {
			// Default: 1GB / 64KB = 16384 blocks (optimized for 64KB blocks)
			globalBlockCache = NewBlockCache(DefaultBlockCacheSize / DefaultBlockSize)
		}
	})
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
		data = decompressed
		// After decompression, data starts directly with magic byte (no length prefix)
	} else {
		// Not compressed - skip length prefix to get to the actual data
		data = data[4:]
	}

	// Binary ONLY - check magic byte
	if len(data) > 0 && data[0] == binaryMagic {
		return decodeEntryBinary(data)
	}

	// No other formats supported
	return storage.Entry{}, fmt.Errorf("invalid data format: no magic byte found")
}

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
		blockData := data[4:] // skip length prefix
		if entry.Compressed {
			decompressed, err := snappy.Decode(nil, blockData)
			if err != nil {
				continue
			}
			blockData = decompressed
			// After decompression, data starts directly with magic byte
		}

		// Binary ONLY - check magic byte
		if len(blockData) > 0 && blockData[0] == binaryMagic {
			decoded, err := decodeEntryBinary(blockData)
			if err != nil {
				continue
			}
			s = decoded
		} else {
			continue // Invalid format
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
