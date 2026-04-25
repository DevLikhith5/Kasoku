package tests

import (
	"fmt"
	"sync"
	"testing"
	"time"

	lsmengine "github.com/DevLikhith5/kasoku/internal/store/lsm-engine"
)

func TestServerBasicOperations(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := lsmengine.NewLSMEngineWithConfig(tmpDir, lsmengine.LSMConfig{
		MemTableSize: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	tests := []struct {
		name string
		fn   func(t *testing.T, store *lsmengine.LSMEngine)
	}{
		{"Put and Get", testPutGet},
		{"Delete", testDelete},
		{"Concurrent Put", testConcurrentPut},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(t, store)
		})
	}
}

func testPutGet(t *testing.T, store *lsmengine.LSMEngine) {
	err := store.Put("key1", []byte("value1"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	entry, err := store.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(entry.Value) != "value1" {
		t.Errorf("expected 'value1', got '%s'", string(entry.Value))
	}
}

func testDelete(t *testing.T, store *lsmengine.LSMEngine) {
	store.Put("key2", []byte("value2"))
	store.Delete("key2")

	_, err := store.Get("key2")
	if err == nil {
		t.Error("expected error after delete")
	}
}

func testConcurrentPut(t *testing.T, store *lsmengine.LSMEngine) {
	var wg sync.WaitGroup
	errors := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			err := store.Put(fmt.Sprintf("concurrent:%d", n), []byte(fmt.Sprintf("value%d", n)))
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("concurrent put failed: %v", err)
	}
}

func TestCircuitBreaker(t *testing.T) {
	cb := NewCircuitBreakerWithDefaults(3, time.Second)

	if !cb.Allow() {
		t.Error("circuit should be closed initially")
	}

	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}

	if cb.Allow() {
		t.Error("circuit should open after 3 failures")
	}

	time.Sleep(time.Second + 100*time.Millisecond)

	if !cb.Allow() {
		t.Error("circuit should half-open after timeout")
	}

	cb.RecordSuccess()
	cb.RecordSuccess()
	cb.RecordSuccess()
	if cb.State() != StateClosed {
		t.Error("circuit should close after 3 successes in half-open")
	}
}
