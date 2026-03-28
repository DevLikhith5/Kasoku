package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateKey(t *testing.T) {
	// Empty key should fail
	err := validateKey("")
	assert.Error(t, err)

	// Normal key should pass
	err = validateKey("normal-key")
	assert.NoError(t, err)

	// Key with special characters should pass
	err = validateKey("key:with:colons")
	assert.NoError(t, err)

	// Very long key should fail
	longKey := string(make([]byte, 1025))
	err = validateKey(longKey)
	assert.Error(t, err)

	// Max length key should pass
	maxKey := string(make([]byte, 1024))
	err = validateKey(maxKey)
	assert.NoError(t, err)
}

func TestValidateValue(t *testing.T) {
	// Empty value should pass (not validated in CLI)
	err := validateValue("")
	assert.NoError(t, err)
}

func TestFormatBytes(t *testing.T) {
	testCases := []struct {
		bytes    uint64
		expected string
	}{
		{0, "0 B"},
		{1, "1 B"},
		{1023, "1023 B"},
		{1024, "1.00 KB"},
		{1536, "1.50 KB"},
		{1048576, "1.00 MB"},
		{1073741824, "1.00 GB"},
	}

	for _, tc := range testCases {
		result := formatBytes(tc.bytes)
		assert.Equal(t, tc.expected, result)
	}
}

func TestTruncate(t *testing.T) {
	// Short string should not be truncated
	result := truncate("hello", 10)
	assert.Equal(t, "hello", result)

	// Exact length should not be truncated
	result = truncate("hello", 5)
	assert.Equal(t, "hello", result)

	// Long string should be truncated
	result = truncate("hello world", 8)
	assert.Equal(t, "hello...", result)
	assert.Len(t, result, 8)

	// Empty string
	result = truncate("", 5)
	assert.Equal(t, "", result)
}

func TestValidateKey_SpecialCharacters(t *testing.T) {
	specialKeys := []string{
		"key:with:colons",
		"key/with/slashes",
		"key with spaces",
		"key-with-dashes",
		"key.with.dots",
		"key_123",
	}

	for _, k := range specialKeys {
		err := validateKey(k)
		assert.NoError(t, err, "key %s should be valid", k)
	}
}

func TestFormatBytes_LargeValues(t *testing.T) {
	// Test GB
	result := formatBytes(2 * 1024 * 1024 * 1024)
	assert.Contains(t, result, "GB")

	// Test MB
	result = formatBytes(500 * 1024 * 1024)
	assert.Contains(t, result, "MB")

	// Test KB
	result = formatBytes(500 * 1024)
	assert.Contains(t, result, "KB")
}

func TestValidateKey_EdgeCases(t *testing.T) {
	// Single character key
	err := validateKey("a")
	assert.NoError(t, err)

	// Key with numbers
	err = validateKey("key123")
	assert.NoError(t, err)

	// Key with unicode
	err = validateKey("key_αβγ")
	assert.NoError(t, err)
}

func TestTruncate_EdgeCases(t *testing.T) {
	// MaxLen 0
	result := truncate("hello", 0)
	assert.Equal(t, "", result)

	// MaxLen less than 3
	result = truncate("hello", 3)
	assert.Equal(t, "", result)

	// MaxLen exactly 3
	result = truncate("hello", 3)
	assert.Equal(t, "", result)

	// MaxLen 4 - should truncate to 1 char + ...
	result = truncate("hello", 4)
	assert.Equal(t, "h...", result)
}

func TestFormatBytes_EdgeCases(t *testing.T) {
	// Zero bytes
	assert.Equal(t, "0 B", formatBytes(0))

	// Very large number
	result := formatBytes(1000000000000000)
	assert.Contains(t, result, "GB")
}

// TestMatchPattern tests the Redis-style pattern matching
func TestMatchPattern(t *testing.T) {
	testCases := []struct {
		key      string
		pattern  string
		expected bool
	}{
		// Exact match
		{"user:123", "user:123", true},
		{"user:123", "user:456", false},

		// Wildcard at end
		{"user:123", "user:*", true},
		{"user:456", "user:*", true},
		{"session:1", "user:*", false},

		// Wildcard at start
		{"user:123", "*:123", true},
		{"session:123", "*:123", true},
		{"user:456", "*:123", false},

		// Wildcard on both sides
		{"user:123", "*user*", true},
		{"myuserkey", "*user*", true},
		{"user:123", "*123*", true},
		{"session:123", "*123*", true},

		// Wildcard in middle
		{"user:123:name", "user:*:name", true},
		{"user:456:name", "user:*:name", true},
		{"user:123:other", "user:*:name", false},

		// Only wildcard
		{"anything", "*", true},
		{"user:123", "*", true},

		// Empty pattern
		{"", "", true},
		{"key", "", false},

		// Question mark (single char)
		{"user:1", "user:?", true},
		{"user:12", "user:?", false},
		{"user:12", "user:??", true},
		{"user:123", "user:???", true},
	}

	for _, tc := range testCases {
		result := simpleMatch(tc.key, tc.pattern)
		assert.Equal(t, tc.expected, result, "matchPattern(%q, %q)", tc.key, tc.pattern)
	}
}

func TestMatchPattern_ComplexPatterns(t *testing.T) {
	// Multiple wildcards
	assert.True(t, simpleMatch("user:123:data", "user:*:*"))
	assert.True(t, simpleMatch("user:123:456", "user:*:*"))
	assert.False(t, simpleMatch("user:123", "user:*:*"))

	// Wildcard with special chars
	assert.True(t, simpleMatch("key-with-dash", "key-*"))
	assert.True(t, simpleMatch("key.with.dots", "key.*"))
}
