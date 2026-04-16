package rpc

import (
	"errors"
	"sync"
	"time"
)

// CircuitBreaker implements the circuit breaker pattern to prevent cascading failures.
// It has three states: Closed (normal), Open (failing), and Half-Open (testing).
type CircuitBreaker struct {
	mu                   sync.RWMutex
	state                State
	failures             int
	lastFailureTime      time.Time
	consecutiveSuccesses int

	// Configurable thresholds
	maxFailures      int           // Number of failures before opening
	timeout          time.Duration // How long to stay open before testing
	halfOpenMaxCalls int           // Max calls allowed in half-open state
}

// State represents the circuit breaker state
type State int

const (
	StateClosed   State = iota // Normal operation
	StateOpen                  // Failing, reject requests
	StateHalfOpen              // Testing if service recovered
)

var (
	ErrCircuitOpen = errors.New("circuit breaker is open")
)

// NewCircuitBreaker creates a new circuit breaker with default settings.
func NewCircuitBreaker() *CircuitBreaker {
	return &CircuitBreaker{
		maxFailures:      5,                // Open after 5 failures
		timeout:          30 * time.Second, // Try again after 30 seconds
		halfOpenMaxCalls: 3,                // Allow 3 test calls in half-open
	}
}

// NewCircuitBreakerWithConfig creates a circuit breaker with custom settings.
func NewCircuitBreakerWithConfig(maxFailures int, timeout time.Duration, halfOpenMaxCalls int) *CircuitBreaker {
	return &CircuitBreaker{
		maxFailures:      maxFailures,
		timeout:          timeout,
		halfOpenMaxCalls: halfOpenMaxCalls,
	}
}

// Allow checks if a request should be allowed through.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		// Check if we should transition to half-open
		if time.Since(cb.lastFailureTime) > cb.timeout {
			cb.state = StateHalfOpen
			cb.consecutiveSuccesses = 0
			return true
		}
		return false
	case StateHalfOpen:
		// Allow limited requests to test recovery
		return cb.consecutiveSuccesses < cb.halfOpenMaxCalls
	}
	return false
}

// RecordSuccess records a successful request.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		// Reset failure count on success
		cb.failures = 0
	case StateHalfOpen:
		cb.consecutiveSuccesses++
		// If we've had enough consecutive successes, close the circuit
		if cb.consecutiveSuccesses >= cb.halfOpenMaxCalls {
			cb.state = StateClosed
			cb.failures = 0
			cb.consecutiveSuccesses = 0
		}
	}
}

// RecordFailure records a failed request.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailureTime = time.Now()

	switch cb.state {
	case StateClosed:
		if cb.failures >= cb.maxFailures {
			cb.state = StateOpen
		}
	case StateHalfOpen:
		// Any failure in half-open immediately reopens
		cb.state = StateOpen
		cb.consecutiveSuccesses = 0
	}
}

// State returns the current state (for monitoring).
func (cb *CircuitBreaker) State() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// CircuitBreakerManager manages circuit breakers for multiple nodes.
type CircuitBreakerManager struct {
	mu        sync.RWMutex
	breakers  map[string]*CircuitBreaker
	defaultCB *CircuitBreaker
}

// NewCircuitBreakerManager creates a new manager.
func NewCircuitBreakerManager() *CircuitBreakerManager {
	return &CircuitBreakerManager{
		breakers:  make(map[string]*CircuitBreaker),
		defaultCB: NewCircuitBreaker(),
	}
}

// Get returns the circuit breaker for a node (creates if needed).
func (m *CircuitBreakerManager) Get(nodeAddr string) *CircuitBreaker {
	m.mu.RLock()
	cb, exists := m.breakers[nodeAddr]
	m.mu.RUnlock()
	if exists {
		return cb
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// Double-check after acquiring write lock
	cb, exists = m.breakers[nodeAddr]
	if exists {
		return cb
	}

	cb = NewCircuitBreaker()
	m.breakers[nodeAddr] = cb
	return cb
}

// Remove removes a node's circuit breaker (e.g., when node is removed from cluster).
func (m *CircuitBreakerManager) Remove(nodeAddr string) {
	m.mu.Lock()
	delete(m.breakers, nodeAddr)
	m.mu.Unlock()
}
