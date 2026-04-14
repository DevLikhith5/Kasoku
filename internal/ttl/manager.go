package ttl

import (
	"container/heap"
	"sync"
	"time"
)

type Entry struct {
	Key         string
	ExpireAt    time.Time
	IsTombstone bool
}

type Manager struct {
	mu       sync.RWMutex
	pq       *PriorityQueue
	stopCh   chan struct{}
	closed   bool
	interval time.Duration
	onExpire func(key string)
}

type Config struct {
	CheckInterval time.Duration

	OnExpire func(key string)
}

func NewManager(cfg Config) *Manager {
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = 1 * time.Minute
	}

	m := &Manager{
		pq:       NewPriorityQueue(),
		stopCh:   make(chan struct{}),
		interval: cfg.CheckInterval,
		onExpire: cfg.OnExpire,
	}

	heap.Init(m.pq)

	return m
}

func (m *Manager) Add(key string, ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return
	}

	m.pq.Remove(key)

	entry := &Item{
		Key:      key,
		ExpireAt: time.Now().Add(ttl),
	}
	heap.Push(m.pq, entry)
}

func (m *Manager) AddWithTombstone(key string, ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return
	}

	m.pq.Remove(key)

	entry := &Item{
		Key:         key,
		ExpireAt:    time.Now().Add(ttl),
		IsTombstone: true,
	}
	heap.Push(m.pq, entry)
}

func (m *Manager) Remove(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return
	}

	m.pq.Remove(key)
}

func (m *Manager) GetExpiration(key string) (time.Time, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	item := m.pq.items[key]
	if item == nil {
		return time.Time{}, false
	}
	return item.ExpireAt, true
}

func (m *Manager) IsExpired(key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	item := m.pq.items[key]
	if item == nil {
		return true
	}
	return time.Now().After(item.ExpireAt)
}

func (m *Manager) Start() {
	go m.run()
}

func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.closed {
		m.closed = true
		close(m.stopCh)
	}
}

func (m *Manager) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pq.Len()
}

func (m *Manager) run() {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.expireKeys()
		case <-m.stopCh:
			return
		}
	}
}

func (m *Manager) expireKeys() {
	now := time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return
	}

	var expiredKeys []string

	for m.pq.Len() > 0 {
		earliest := m.pq.order[0]
		if earliest == nil || !earliest.ExpireAt.Before(now) {
			break
		}

		item := heap.Pop(m.pq).(*Item)
		expiredKeys = append(expiredKeys, item.Key)
	}

	if len(expiredKeys) > 0 && m.onExpire != nil {
		go func(keys []string) {
			for _, key := range keys {
				m.onExpire(key)
			}
		}(expiredKeys)
	}
}

type Item struct {
	Key         string
	ExpireAt    time.Time
	IsTombstone bool
	index       int
}

type PriorityQueue struct {
	items map[string]*Item
	order []*Item
}

func NewPriorityQueue() *PriorityQueue {
	return &PriorityQueue{
		items: make(map[string]*Item),
		order: make([]*Item, 0),
	}
}

func (pq *PriorityQueue) Len() int {
	return len(pq.order)
}

func (pq *PriorityQueue) Less(i, j int) bool {
	return pq.order[i].ExpireAt.Before(pq.order[j].ExpireAt)
}

func (pq *PriorityQueue) Swap(i, j int) {
	pq.order[i], pq.order[j] = pq.order[j], pq.order[i]
	pq.order[i].index = i
	pq.order[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*Item)
	item.index = len(pq.order)
	pq.order = append(pq.order, item)
	pq.items[item.Key] = item
}


func (pq *PriorityQueue) Pop() interface{} {
	n := len(pq.order)
	item := pq.order[n-1]
	pq.order[n-1] = nil 
	item.index = -1
	pq.order = pq.order[:n-1]
	delete(pq.items, item.Key)
	return item
}


func (pq *PriorityQueue) Remove(key string) {
	item, exists := pq.items[key]
	if !exists {
		return
	}
	heap.Remove(pq, item.index)
}
