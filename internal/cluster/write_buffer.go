package cluster

import (
	"context"
	"sync"
	"time"
)

// WriteBuffer coalesces individual Put calls into batches and flushes them
// periodically via ReplicatedBatchPut. This amortizes per-write lock contention
// and RPC overhead under high concurrency — expected 2-3x throughput gain.
//
// Usage:
//
//	buf := NewWriteBuffer(cluster, 100, 5*time.Millisecond)
//	buf.Start()
//	defer buf.Stop()
//	buf.Put("key", []byte("value"))
type WriteBuffer struct {
	mu         sync.Mutex
	cluster    *Cluster
	pending    map[string][]byte
	maxSize    int           // flush when pending reaches this many entries
	flushEvery time.Duration // also flush on this interval regardless of size
	flushCh    chan struct{}
	stopCh     chan struct{}
	wg         sync.WaitGroup
}

// NewWriteBuffer creates a new write buffer wrapping the given cluster.
//   - maxSize: flush immediately when the buffer grows to this many entries
//   - flushEvery: background flush interval (keeps latency bounded)
func NewWriteBuffer(c *Cluster, maxSize int, flushEvery time.Duration) *WriteBuffer {
	if maxSize <= 0 {
		maxSize = 100
	}
	if flushEvery <= 0 {
		flushEvery = 5 * time.Millisecond
	}
	return &WriteBuffer{
		cluster:    c,
		pending:    make(map[string][]byte, maxSize),
		maxSize:    maxSize,
		flushEvery: flushEvery,
		flushCh:    make(chan struct{}, 1),
		stopCh:     make(chan struct{}),
	}
}

// Start launches the background flush goroutine.
func (b *WriteBuffer) Start() {
	b.wg.Add(1)
	go b.run()
}

// Stop signals the buffer to flush remaining entries and shut down.
func (b *WriteBuffer) Stop() {
	close(b.stopCh)
	b.wg.Wait()
}

// Put buffers a key-value pair. If the buffer is full it triggers an immediate flush.
func (b *WriteBuffer) Put(key string, value []byte) {
	b.mu.Lock()
	b.pending[key] = value
	full := len(b.pending) >= b.maxSize
	b.mu.Unlock()

	if full {
		// Non-blocking signal — if a flush is already queued, this is a no-op
		select {
		case b.flushCh <- struct{}{}:
		default:
		}
	}
}

// Len returns the current number of buffered entries.
func (b *WriteBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pending)
}

func (b *WriteBuffer) run() {
	defer b.wg.Done()
	ticker := time.NewTicker(b.flushEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.flush()
		case <-b.flushCh:
			b.flush()
		case <-b.stopCh:
			b.flush() // drain remaining entries on shutdown
			return
		}
	}
}

func (b *WriteBuffer) flush() {
	b.mu.Lock()
	if len(b.pending) == 0 {
		b.mu.Unlock()
		return
	}
	// Swap out the pending map so writers can continue buffering immediately
	batch := b.pending
	b.pending = make(map[string][]byte, b.maxSize)
	b.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), b.cluster.rpcTimeout)
	defer cancel()

	if err := b.cluster.ReplicatedBatchPut(ctx, batch); err != nil {
		b.cluster.logger.Error("write buffer flush failed", "keys", len(batch), "error", err)
	}
}
