package storage 

import (
	"errors"
	"time"
)

type Entry struct {
	Key string
	Value []byte
	Version uint64
	TimeStamp time.Time
	Tombstone bool 
}

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrKeyTooLong = errors.New("key exceeds 1KB limit")
	ErrValueTooLarge = errors.New("value exceeds 1MB limit")
	ErrEngineClosed = errors.New("engine is closed")
)

const (
	MaxKeyLen = 1024 // 1KB
	MaxValueLen = 1024*1024 // 1MB
)

type EngineStats struct {
	KeyCount int64
	DiskBytes int64
	MemBytes int64
	BloomFPRate float64
}

type StorageEngine interface {
	Get(key string) (Entry, error)
	Put(key string, value []byte) error
	Delete(key string) error
	// Keys returns all non-deleted keys
	Keys() ([]string, error)
	// Scan returns all non-deleted entries with the given prefix
	Scan(prefix string)([]Entry, error)
	Stats() EngineStats
	Close() error
}