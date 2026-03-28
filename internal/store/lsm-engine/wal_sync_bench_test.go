package lsmengine

import (
	"testing"
	"time"

	storage "github.com/DevLikhith5/kasoku/internal/store"
)

// BenchmarkWALSyncInterval benchmarks the WAL with different sync intervals
func BenchmarkWALSyncInterval(b *testing.B) {
	value := []byte("test-value-for-benchmark")

	b.Run("SyncOnWrite", func(b *testing.B) {
		dir := b.TempDir()
		wal, err := storage.OpenWALWithConfig(dir+"/wal.log", storage.WALConfig{
			SyncInterval: 0, // Sync on every write
		})
		if err != nil {
			b.Fatal(err)
		}
		defer wal.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			entry := storage.Entry{
				Key:       "key",
				Value:     value,
				Version:   uint64(i),
				TimeStamp: time.Now(),
			}
			if err := wal.Append(entry); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("SyncEvery100ms", func(b *testing.B) {
		dir := b.TempDir()
		wal, err := storage.OpenWALWithConfig(dir+"/wal.log", storage.WALConfig{
			SyncInterval: 100 * time.Millisecond,
		})
		if err != nil {
			b.Fatal(err)
		}
		defer wal.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			entry := storage.Entry{
				Key:       "key",
				Value:     value,
				Version:   uint64(i),
				TimeStamp: time.Now(),
			}
			if err := wal.Append(entry); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkLSMEngineSyncInterval benchmarks the LSM engine with different sync intervals
func BenchmarkLSMEngineSyncInterval(b *testing.B) {
	value := []byte("test-value-for-benchmark")

	b.Run("SyncOnWrite", func(b *testing.B) {
		dir := b.TempDir()
		engine, err := NewLSMEngineWithConfig(dir, LSMConfig{
			WALSyncInterval: 0, // Sync on every write
		})
		if err != nil {
			b.Fatal(err)
		}
		defer engine.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := engine.Put("key", value); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("SyncEvery100ms", func(b *testing.B) {
		dir := b.TempDir()
		engine, err := NewLSMEngineWithConfig(dir, LSMConfig{
			WALSyncInterval: 100 * time.Millisecond,
		})
		if err != nil {
			b.Fatal(err)
		}
		defer engine.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := engine.Put("key", value); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Benchmark10KWrites benchmarks 10,000 writes (simulating the original issue)
func Benchmark10KWrites(b *testing.B) {
	value := []byte("test-value-for-benchmark")

	b.Run("SyncOnWrite_10K", func(b *testing.B) {
		dir := b.TempDir()
		engine, err := NewLSMEngineWithConfig(dir, LSMConfig{
			WALSyncInterval: 0,
		})
		if err != nil {
			b.Fatal(err)
		}
		defer engine.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < 10000; j++ {
				key := "key"
				if err := engine.Put(key, value); err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	b.Run("SyncEvery100ms_10K", func(b *testing.B) {
		dir := b.TempDir()
		engine, err := NewLSMEngineWithConfig(dir, LSMConfig{
			WALSyncInterval: 100 * time.Millisecond,
		})
		if err != nil {
			b.Fatal(err)
		}
		defer engine.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < 10000; j++ {
				key := "key"
				if err := engine.Put(key, value); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

// TestWALBackgroundSync verifies background sync doesn't break functionality
func TestWALBackgroundSync(t *testing.T) {
	dir := t.TempDir()
	
	wal, err := storage.OpenWALWithConfig(dir+"/wal.log", storage.WALConfig{
		SyncInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Append some entries
	for i := 0; i < 100; i++ {
		entry := storage.Entry{
			Key:       "key",
			Value:     []byte("value"),
			Version:   uint64(i),
			TimeStamp: time.Now(),
		}
		if err := wal.Append(entry); err != nil {
			t.Fatal(err)
		}
	}

	// Close should stop the background sync gracefully
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and verify data survived
	wal2, err := storage.OpenWAL(dir + "/wal.log")
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	var count int
	err = wal2.Replay(storage.WALReplayHandlerFuncs{
		PutEntryFunc: func(entry storage.Entry) error {
			count++
			return nil
		},
		SetVersionFunc: func(v uint64) {},
	})
	if err != nil {
		t.Fatal(err)
	}

	if count != 100 {
		t.Errorf("expected 100 entries, got %d", count)
	}
}

// WALReplayHandlerFuncs is a helper for testing
type WALReplayHandlerFuncs struct {
	PutEntryFunc func(storage.Entry) error
	SetVersionFunc func(uint64)
}

func (h WALReplayHandlerFuncs) PutEntry(entry storage.Entry) error {
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
