package lsmengine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBloomFilter_New(t *testing.T) {
	t.Run("create with valid parameters", func(t *testing.T) {
		bf := NewBloomFilter(1000, 0.01)

		assert.NotNil(t, bf)
		assert.Greater(t, bf.numBits, uint64(0))
		assert.Greater(t, bf.numHash, 0)
		assert.Less(t, bf.numHash, 10) // Should be reasonable
	})

	t.Run("create with n=0 uses default", func(t *testing.T) {
		bf := NewBloomFilter(0, 0.01)

		assert.NotNil(t, bf)
		assert.Greater(t, bf.numBits, uint64(0))
	})

	t.Run("create with negative n uses default", func(t *testing.T) {
		bf := NewBloomFilter(-100, 0.01)

		assert.NotNil(t, bf)
		assert.Greater(t, bf.numBits, uint64(0))
	})

	t.Run("create with p=0 uses default", func(t *testing.T) {
		bf := NewBloomFilter(1000, 0)

		assert.NotNil(t, bf)
	})

	t.Run("create with p=1 uses default", func(t *testing.T) {
		bf := NewBloomFilter(1000, 1)

		assert.NotNil(t, bf)
	})

	t.Run("create with negative p uses default", func(t *testing.T) {
		bf := NewBloomFilter(1000, -0.5)

		assert.NotNil(t, bf)
	})

	t.Run("create with very small n", func(t *testing.T) {
		bf := NewBloomFilter(1, 0.01)

		assert.NotNil(t, bf)
		assert.Greater(t, bf.numBits, uint64(0))
		assert.Greater(t, bf.numHash, 0)
	})

	t.Run("create with very large n", func(t *testing.T) {
		bf := NewBloomFilter(1000000, 0.01)

		assert.NotNil(t, bf)
		assert.Greater(t, bf.numBits, uint64(0))
	})

	t.Run("different false positive rates", func(t *testing.T) {
		bf1 := NewBloomFilter(1000, 0.001) // Lower FP rate
		bf2 := NewBloomFilter(1000, 0.1)   // Higher FP rate

		// Lower FP rate should have more bits
		assert.Greater(t, bf1.numBits, bf2.numBits)
	})
}

func TestBloomFilter_Add(t *testing.T) {
	t.Run("add single key", func(t *testing.T) {
		bf := NewBloomFilter(100, 0.01)

		bf.Add([]byte("testkey"))

		assert.True(t, bf.MightContain([]byte("testkey")))
	})

	t.Run("add multiple keys", func(t *testing.T) {
		bf := NewBloomFilter(1000, 0.01)

		for i := 0; i < 100; i++ {
			bf.Add([]byte(fmt.Sprintf("key:%d", i)))
		}

		// All added keys should be found
		for i := 0; i < 100; i++ {
			assert.True(t, bf.MightContain([]byte(fmt.Sprintf("key:%d", i))))
		}
	})

	t.Run("add same key multiple times", func(t *testing.T) {
		bf := NewBloomFilter(100, 0.01)

		bf.Add([]byte("key"))
		bf.Add([]byte("key"))
		bf.Add([]byte("key"))

		assert.True(t, bf.MightContain([]byte("key")))
	})

	t.Run("add empty key", func(t *testing.T) {
		bf := NewBloomFilter(100, 0.01)

		bf.Add([]byte{})

		assert.True(t, bf.MightContain([]byte{}))
	})

	t.Run("add nil key", func(t *testing.T) {
		bf := NewBloomFilter(100, 0.01)

		bf.Add(nil)

		assert.True(t, bf.MightContain(nil))
	})
}

func TestBloomFilter_MightContain(t *testing.T) {
	t.Run("no false negatives", func(t *testing.T) {
		bf := NewBloomFilter(1000, 0.01)

		// Add 1000 keys
		for i := 0; i < 1000; i++ {
			bf.Add([]byte(fmt.Sprintf("key:%d", i)))
		}

		// All must be found (no false negatives allowed)
		for i := 0; i < 1000; i++ {
			assert.True(t, bf.MightContain([]byte(fmt.Sprintf("key:%d", i))),
				"false negative for key:%d", i)
		}
	})

	t.Run("false positive rate within bounds", func(t *testing.T) {
		bf := NewBloomFilter(1000, 0.01)

		// Add 1000 keys
		for i := 0; i < 1000; i++ {
			bf.Add([]byte(fmt.Sprintf("key:%d", i)))
		}

		// Check false positive rate with unseen keys
		falsePositives := 0
		total := 10000

		for i := 10000; i < 10000+total; i++ {
			if bf.MightContain([]byte(fmt.Sprintf("unseen:%d", i))) {
				falsePositives++
			}
		}

		fpRate := float64(falsePositives) / float64(total)

		t.Logf("False positive rate: %.4f (target: 0.01)", fpRate)

		// Allow some tolerance in false positive rate
		assert.Less(t, fpRate, 0.03, "false positive rate too high")
	})

	t.Run("definitely not present", func(t *testing.T) {
		bf := NewBloomFilter(100, 0.01)

		// Add some keys
		for i := 0; i < 10; i++ {
			bf.Add([]byte(fmt.Sprintf("key:%d", i)))
		}

		// Keys that are definitely not added
		assert.False(t, bf.MightContain([]byte("notpresent")))
		assert.False(t, bf.MightContain([]byte("neveradded")))
		assert.False(t, bf.MightContain([]byte("xyz123")))
	})

	t.Run("empty filter", func(t *testing.T) {
		bf := NewBloomFilter(100, 0.01)

		// Empty filter should return false for everything
		assert.False(t, bf.MightContain([]byte("anything")))
		assert.False(t, bf.MightContain([]byte{}))
		assert.False(t, bf.MightContain(nil))
	})
}

func TestBloomFilter_Encode(t *testing.T) {
	t.Run("encode empty filter", func(t *testing.T) {
		bf := NewBloomFilter(100, 0.01)

		data := bf.Encode()

		assert.Greater(t, len(data), 0)
		// Format: [numBits uint64][numHash uint32][bits...]
		assert.GreaterOrEqual(t, len(data), 12) // 8 + 4 bytes header
	})

	t.Run("encode with data", func(t *testing.T) {
		bf := NewBloomFilter(100, 0.01)

		for i := 0; i < 10; i++ {
			bf.Add([]byte(fmt.Sprintf("key:%d", i)))
		}

		data := bf.Encode()

		assert.Greater(t, len(data), 12)
	})

	t.Run("encode preserves state", func(t *testing.T) {
		bf := NewBloomFilter(100, 0.01)

		bf.Add([]byte("key1"))
		bf.Add([]byte("key2"))

		data1 := bf.Encode()
		data2 := bf.Encode()

		assert.Equal(t, data1, data2)
	})

	t.Run("different filters produce different encodings", func(t *testing.T) {
		bf1 := NewBloomFilter(100, 0.01)
		bf2 := NewBloomFilter(100, 0.01)

		bf1.Add([]byte("key1"))
		bf2.Add([]byte("key2"))

		data1 := bf1.Encode()
		data2 := bf2.Encode()

		// Should be different (different bits set)
		assert.NotEqual(t, data1, data2)
	})
}

func TestBloomFilter_Decode(t *testing.T) {
	t.Run("decode valid data", func(t *testing.T) {
		bf := NewBloomFilter(100, 0.01)

		for i := 0; i < 10; i++ {
			bf.Add([]byte(fmt.Sprintf("key:%d", i)))
		}

		data := bf.Encode()

		decoded, err := DecodeBloomFilter(data)
		require.NoError(t, err)
		require.NotNil(t, decoded)

		// Verify decoded filter works
		for i := 0; i < 10; i++ {
			assert.True(t, decoded.MightContain([]byte(fmt.Sprintf("key:%d", i))))
		}
	})

	t.Run("decode preserves false positive rate", func(t *testing.T) {
		bf := NewBloomFilter(1000, 0.01)

		for i := 0; i < 100; i++ {
			bf.Add([]byte(fmt.Sprintf("key:%d", i)))
		}

		data := bf.Encode()
		decoded, _ := DecodeBloomFilter(data)

		// Check same false positive behavior
		for i := 0; i < 100; i++ {
			original := bf.MightContain([]byte(fmt.Sprintf("key:%d", i)))
			decodedResult := decoded.MightContain([]byte(fmt.Sprintf("key:%d", i)))
			assert.Equal(t, original, decodedResult)
		}
	})

	t.Run("decode empty data", func(t *testing.T) {
		_, err := DecodeBloomFilter([]byte{})
		assert.Error(t, err)
	})

	t.Run("decode too short data", func(t *testing.T) {
		_, err := DecodeBloomFilter([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
		assert.Error(t, err)
	})

	t.Run("decode invalid numHash", func(t *testing.T) {
		// Create data with numHash = 0
		data := make([]byte, 12)
		data[0] = 100 // numBits (partial)
		data[8] = 0   // numHash = 0 (invalid)

		_, err := DecodeBloomFilter(data)
		assert.Error(t, err)
	})

	t.Run("decode invalid numHash (too large)", func(t *testing.T) {
		data := make([]byte, 20)
		data[8] = 200 // numHash = 200 (invalid - too large)

		_, err := DecodeBloomFilter(data)
		assert.Error(t, err)
	})

	t.Run("decode no bit data", func(t *testing.T) {
		data := make([]byte, 12)
		// Header only, no bits

		_, err := DecodeBloomFilter(data)
		assert.Error(t, err)
	})

	t.Run("round trip with various sizes", func(t *testing.T) {
		sizes := []int{10, 100, 1000, 10000}

		for _, n := range sizes {
			t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
				bf := NewBloomFilter(n, 0.01)

				for i := 0; i < n; i++ {
					bf.Add([]byte(fmt.Sprintf("key:%d", i)))
				}

				data := bf.Encode()
				decoded, err := DecodeBloomFilter(data)
				require.NoError(t, err)

				// Verify all keys
				for i := 0; i < n; i++ {
					assert.True(t, decoded.MightContain([]byte(fmt.Sprintf("key:%d", i))))
				}
			})
		}
	})
}

func TestBloomFilter_Hash(t *testing.T) {
	t.Run("hash produces consistent results", func(t *testing.T) {
		bf := NewBloomFilter(100, 0.01)

		key := []byte("testkey")
		h1a, h2a := bf.hash(key)
		h1b, h2b := bf.hash(key)

		assert.Equal(t, h1a, h1b)
		assert.Equal(t, h2a, h2b)
	})

	t.Run("hash produces different values for different keys", func(t *testing.T) {
		bf := NewBloomFilter(100, 0.01)

		h1a, h2a := bf.hash([]byte("key1"))
		h1b, h2b := bf.hash([]byte("key2"))

		assert.NotEqual(t, h1a, h1b)
		assert.NotEqual(t, h2a, h2b)
	})

	t.Run("hash of empty key", func(t *testing.T) {
		bf := NewBloomFilter(100, 0.01)

		h1, h2 := bf.hash([]byte{})

		assert.Greater(t, h1, uint64(0))
		assert.Greater(t, h2, uint64(0))
	})

	t.Run("hash of nil key", func(t *testing.T) {
		bf := NewBloomFilter(100, 0.01)

		h1, h2 := bf.hash(nil)

		assert.Greater(t, h1, uint64(0))
		assert.Greater(t, h2, uint64(0))
	})

	t.Run("hash distribution", func(t *testing.T) {
		bf := NewBloomFilter(10000, 0.01)

		// Hash many keys and check distribution
		buckets := make(map[uint64]int)

		for i := 0; i < 1000; i++ {
			h1, h2 := bf.hash([]byte(fmt.Sprintf("key:%d", i)))
			buckets[h1%100]++
			buckets[h2%100]++
		}

		// Check that hashes are somewhat evenly distributed
		assert.Greater(t, len(buckets), 50) // Should hit many buckets
	})
}

func TestBloomFilter_EdgeCases(t *testing.T) {
	t.Run("binary data", func(t *testing.T) {
		bf := NewBloomFilter(100, 0.01)

		binaryData := []byte{0x00, 0x01, 0xFF, 0xFE, 0x80, 0x7F}
		bf.Add(binaryData)

		assert.True(t, bf.MightContain(binaryData))
		assert.False(t, bf.MightContain([]byte{0x00, 0x01, 0xFF}))
	})

	t.Run("unicode data", func(t *testing.T) {
		bf := NewBloomFilter(100, 0.01)

		unicode := []byte("こんにちは世界🌍")
		bf.Add(unicode)

		assert.True(t, bf.MightContain(unicode))
		assert.False(t, bf.MightContain([]byte("different")))
	})

	t.Run("large key", func(t *testing.T) {
		bf := NewBloomFilter(100, 0.01)

		largeKey := make([]byte, 10000)
		for i := range largeKey {
			largeKey[i] = byte(i % 256)
		}

		bf.Add(largeKey)

		assert.True(t, bf.MightContain(largeKey))
	})

	t.Run("many keys stress test", func(t *testing.T) {
		bf := NewBloomFilter(100000, 0.01)

		// Add 100000 keys
		for i := 0; i < 100000; i++ {
			bf.Add([]byte(fmt.Sprintf("key:%d", i)))
		}

		// Verify no false negatives
		for i := 0; i < 100000; i++ {
			assert.True(t, bf.MightContain([]byte(fmt.Sprintf("key:%d", i))))
		}

		// Check false positive rate
		falsePositives := 0
		for i := 100000; i < 101000; i++ {
			if bf.MightContain([]byte(fmt.Sprintf("unseen:%d", i))) {
				falsePositives++
			}
		}

		fpRate := float64(falsePositives) / 1000
		t.Logf("False positive rate at 100k keys: %.4f", fpRate)
		assert.Less(t, fpRate, 0.05)
	})
}

func TestBloomFilter_Parameters(t *testing.T) {
	t.Run("optimal k for different m/n ratios", func(t *testing.T) {
		// Test that numHash is reasonable for different configurations
		testCases := []struct {
			n        int
			p        float64
			expected int
		}{
			{100, 0.01, 7},
			{1000, 0.01, 7},
			{10000, 0.01, 7},
			{100, 0.001, 10},
			{100, 0.1, 4},
		}

		for _, tc := range testCases {
			t.Run(fmt.Sprintf("n=%d_p=%.3f", tc.n, tc.p), func(t *testing.T) {
				bf := NewBloomFilter(tc.n, tc.p)
				assert.Greater(t, bf.numHash, 0)
				assert.Less(t, bf.numHash, 20)
			})
		}
	})

	t.Run("bit array size calculation", func(t *testing.T) {
		// Lower FP rate should require more bits
		bf1 := NewBloomFilter(1000, 0.001)
		bf2 := NewBloomFilter(1000, 0.01)
		bf3 := NewBloomFilter(1000, 0.1)

		assert.Greater(t, bf1.numBits, bf2.numBits)
		assert.Greater(t, bf2.numBits, bf3.numBits)
	})
}

func TestBloomFilter_EncodeDecode_EdgeCases(t *testing.T) {
	t.Run("encode decode with all bits set", func(t *testing.T) {
		bf := NewBloomFilter(10, 0.5) // Small filter with high FP rate

		// Add many keys to set most bits
		for i := 0; i < 100; i++ {
			bf.Add([]byte(fmt.Sprintf("key:%d", i)))
		}

		data := bf.Encode()
		decoded, err := DecodeBloomFilter(data)
		require.NoError(t, err)

		// All original keys should still be found
		for i := 0; i < 100; i++ {
			assert.True(t, decoded.MightContain([]byte(fmt.Sprintf("key:%d", i))))
		}
	})

	t.Run("encode decode preserves numHash", func(t *testing.T) {
		bf := NewBloomFilter(1000, 0.01)
		originalNumHash := bf.numHash

		data := bf.Encode()
		decoded, _ := DecodeBloomFilter(data)

		assert.Equal(t, originalNumHash, decoded.numHash)
	})

	t.Run("encode decode preserves numBits", func(t *testing.T) {
		bf := NewBloomFilter(1000, 0.01)
		originalNumBits := bf.numBits

		data := bf.Encode()
		decoded, _ := DecodeBloomFilter(data)

		assert.Equal(t, originalNumBits, decoded.numBits)
	})
}
