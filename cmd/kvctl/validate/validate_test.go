package validate

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKey(t *testing.T) {
	// Empty key should fail
	err := Key("")
	assert.Error(t, err)

	// Normal key should pass
	err = Key("normal-key")
	assert.NoError(t, err)

	// Key with special characters should pass
	err = Key("key:with:colons")
	assert.NoError(t, err)

	// Very long key should fail
	longKey := string(make([]byte, 1025))
	err = Key(longKey)
	assert.Error(t, err)

	// Max length key should pass
	maxKey := string(make([]byte, 1024))
	err = Key(maxKey)
	assert.NoError(t, err)
}

func TestValue(t *testing.T) {
	// Empty value should pass
	err := Value("")
	assert.NoError(t, err)

	// Normal value should pass
	err = Value("normal-value")
	assert.NoError(t, err)
}

func TestKey_SpecialCharacters(t *testing.T) {
	specialKeys := []string{
		"key:with:colons",
		"key/with/slashes",
		"key with spaces",
		"key-with-dashes",
		"key.with.dots",
		"key_123",
	}

	for _, k := range specialKeys {
		err := Key(k)
		assert.NoError(t, err, "key %s should be valid", k)
	}
}

func TestKey_EdgeCases(t *testing.T) {
	// Single character key
	err := Key("a")
	assert.NoError(t, err)

	// Key with numbers
	err = Key("key123")
	assert.NoError(t, err)

	// Key with unicode
	err = Key("key_αβγ")
	assert.NoError(t, err)
}
