package lsmengine

import (
	"encoding/binary"
	"errors"
	"math"
)

type BloomFilter struct {
	bits    []uint64
	numBits uint64
	numHash int
}

// NewBloomFilter creates a filter for n items with false positive rate p
func NewBloomFilter(n int, p float64) *BloomFilter {
	if n <= 0 {
		n = 1000
	}
	if p <= 0 || p >= 1 {
		p = 0.01
	}

	// Optimal bit array size
	m := uint64(math.Ceil(-float64(n) * math.Log(p) / (math.Log(2) * math.Log(2))))

	// Optimal number of hash functions
	k := int(math.Round(float64(m) / float64(n) * math.Log(2)))
	if k < 1 {
		k = 1
	}

	// Round up to nearest 64 bits
	words := (m + 63) / 64

	return &BloomFilter{
		bits:    make([]uint64, words),
		numBits: m,
		numHash: k,
	}
}

// hash produces two hashes using FNV-1a + mixing
func (bf *BloomFilter) hash(key []byte) (h1, h2 uint64) {
	const prime64 = 1099511628211
	const offset64 = 14695981039346656037

	h1 = offset64
	for _, b := range key {
		h1 ^= uint64(b)
		h1 *= prime64
	}

	// mix to generate second hash
	h2 = h1 ^ (h1 >> 17)
	h2 *= 0xbf58476d1ce4e5b9
	h2 ^= h2 >> 31

	return h1, h2
}

// Add inserts a key into the filter
func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := bf.hash(key)

	for i := 0; i < bf.numHash; i++ {
		pos := (h1 + uint64(i)*h2) % bf.numBits
		bf.bits[pos/64] |= 1 << (pos % 64)
	}
}

// MightContain checks if key may exist
func (bf *BloomFilter) MightContain(key []byte) bool {
	h1, h2 := bf.hash(key)

	for i := 0; i < bf.numHash; i++ {
		pos := (h1 + uint64(i)*h2) % bf.numBits
		if bf.bits[pos/64]&(1<<(pos%64)) == 0 {
			return false // definitely not present
		}
	}

	return true // probably present
}

// Encode serializes the filter
func (bf *BloomFilter) Encode() []byte {
	// Format: [numBits uint64][numHash uint32][bits...]
	buf := make([]byte, 12+8*len(bf.bits))

	binary.LittleEndian.PutUint64(buf[0:], bf.numBits)
	binary.LittleEndian.PutUint32(buf[8:], uint32(bf.numHash))

	for i, word := range bf.bits {
		binary.LittleEndian.PutUint64(buf[12+i*8:], word)
	}

	return buf
}

// DecodeBloomFilter safely reconstructs a filter
func DecodeBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 12 {
		return nil, errors.New("bloom filter: data too short")
	}

	numBits := binary.LittleEndian.Uint64(data[0:])
	numHash := int(binary.LittleEndian.Uint32(data[8:]))

	// Validate numHash
	if numHash <= 0 || numHash > 100 {
		return nil, errors.New("bloom filter: invalid numHash")
	}

	wordCount := (len(data) - 12) / 8
	if wordCount <= 0 {
		return nil, errors.New("bloom filter: no bit data")
	}

	bits := make([]uint64, wordCount)
	for i := range bits {
		bits[i] = binary.LittleEndian.Uint64(data[12+i*8:])
	}

	return &BloomFilter{
		bits:    bits,
		numBits: numBits,
		numHash: numHash,
	}, nil
}
