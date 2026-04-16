package cluster

import (
	"context"
	"fmt"
)

// Semaphore is a counting semaphore for limiting concurrent operations.
type Semaphore struct {
	ch chan struct{}
}

// NewSemaphore creates a new semaphore with the given capacity.
func NewSemaphore(capacity int) *Semaphore {
	return &Semaphore{
		ch: make(chan struct{}, capacity),
	}
}

// Acquire blocks until a slot is available or context is cancelled.
func (s *Semaphore) Acquire(ctx context.Context) error {
	select {
	case s.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TryAcquire tries to acquire a slot without blocking.
func (s *Semaphore) TryAcquire() bool {
	select {
	case s.ch <- struct{}{}:
		return true
	default:
		return false
	}
}

// Release releases a slot back to the semaphore.
func (s *Semaphore) Release() {
	select {
	case <-s.ch:
	default:
		// Should not happen - releasing more than acquired
	}
}

// WorkerPool manages a pool of workers for limiting concurrent operations.
type WorkerPool struct {
	semaphore *Semaphore
}

// NewWorkerPool creates a new worker pool with the given capacity.
func NewWorkerPool(capacity int) *WorkerPool {
	return &WorkerPool{
		semaphore: NewSemaphore(capacity),
	}
}

// Execute executes a function with a worker from the pool.
func (p *WorkerPool) Execute(ctx context.Context, fn func() error) error {
	if err := p.semaphore.Acquire(ctx); err != nil {
		return fmt.Errorf("failed to acquire worker: %w", err)
	}
	defer p.semaphore.Release()
	return fn()
}

// TryExecute tries to execute a function without blocking.
func (p *WorkerPool) TryExecute(fn func() error) (bool, error) {
	if !p.semaphore.TryAcquire() {
		return false, nil
	}
	defer p.semaphore.Release()
	return true, fn()
}
