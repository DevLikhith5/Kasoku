package grpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/DevLikhith5/kasoku/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type BatchWriteEntry struct {
	Key   string
	Value []byte
}

type ReplicatedClient struct {
	addr   string
	conn   *grpc.ClientConn
	client api.KasokuServiceClient
	mu     sync.Mutex
}

func NewReplicatedClient(addr string) (*ReplicatedClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithReturnConnectionError(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	return &ReplicatedClient{
		addr:    addr,
		conn:    conn,
		client:  api.NewKasokuServiceClient(conn),
	}, nil
}

func (c *ReplicatedClient) Close() error {
	return c.conn.Close()
}

func (c *ReplicatedClient) ReplicatedPut(ctx context.Context, key string, value []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := c.client.Put(ctx, &api.PutRequest{
		Key:   key,
		Value: value,
	})
	return err
}

func (c *ReplicatedClient) ReplicatedPutBinary(ctx context.Context, key string, value []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := c.client.Put(ctx, &api.PutRequest{
		Key:   key,
		Value: value,
	})
	return err
}

func (c *ReplicatedClient) ReplicatedGet(ctx context.Context, key string) ([]byte, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := c.client.Get(ctx, &api.GetRequest{Key: key})
	if err != nil {
		return nil, false, err
	}

	if resp.Entry == nil {
		return nil, false, nil
	}

	if resp.Entry.Tombstone {
		return nil, false, nil
	}

	return resp.Entry.Value, true, nil
}

func (c *ReplicatedClient) ReplicatedDelete(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := c.client.Delete(ctx, &api.DeleteRequest{Key: key})
	return err
}

func (c *ReplicatedClient) BatchReplicatedPut(ctx context.Context, entries []BatchWriteEntry) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req := &api.BatchPutRequest{
		Entries: make([]*api.Entry, len(entries)),
	}

	for i, e := range entries {
		req.Entries[i] = &api.Entry{
			Key:   e.Key,
			Value: e.Value,
		}
	}

	resp, err := c.client.BatchPut(ctx, req)
	if err != nil {
		return 0, err
	}

	return int(resp.Count), nil
}

func (c *ReplicatedClient) BatchReplicatedGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := c.client.MultiGet(ctx, &api.MultiGetRequest{Keys: keys})
	if err != nil {
		return nil, err
	}

	result := make(map[string][]byte)
	for key, entry := range resp.Entries {
		if entry == nil || entry.Tombstone {
			continue
		}
		result[key] = entry.Value
	}

	return result, nil
}

func (c *ReplicatedClient) Sync(ctx context.Context, version uint64) ([]byte, uint64, error) {
	resp, err := c.client.Sync(ctx, &api.SyncRequest{SinceVersion: version})
	if err != nil {
		return nil, 0, err
	}

	var data []byte
	for _, e := range resp.Entries {
		data = append(data, e.Value...)
	}

	return data, resp.Version, nil
}

type Pool struct {
	mu       sync.RWMutex
	clients  map[string][]*ReplicatedClient
	idx      map[string]int
	minConns int
	maxConns int
}

func NewPool() *Pool {
	return &Pool{
		clients:  make(map[string][]*ReplicatedClient),
		idx:      make(map[string]int),
		minConns: 4,
		maxConns: 16,
	}
}

func (p *Pool) Get(addr string) (*ReplicatedClient, error) {
	p.mu.RLock()
	conns, ok := p.clients[addr]
	p.mu.RUnlock()

	if ok && len(conns) > 0 {
		p.mu.Lock()
		p.idx[addr] = (p.idx[addr] + 1) % len(conns)
		client := conns[p.idx[addr]]
		p.mu.Unlock()
		return client, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if conns, ok := p.clients[addr]; ok && len(conns) > 0 {
		p.idx[addr] = (p.idx[addr] + 1) % len(conns)
		return conns[p.idx[addr]], nil
	}

	if len(conns) >= p.maxConns {
		return conns[0], nil
	}

	client, err := NewReplicatedClient(addr)
	if err != nil {
		return nil, err
	}

	p.clients[addr] = append(p.clients[addr], client)
	p.idx[addr] = 0

	for i := 1; i < p.minConns; i++ {
		c, err := NewReplicatedClient(addr)
		if err != nil {
			break
		}
		p.clients[addr] = append(p.clients[addr], c)
	}

	return client, nil
}

func (p *Pool) GetAll(addr string) ([]*ReplicatedClient, error) {
	p.mu.RLock()
	conns, ok := p.clients[addr]
	p.mu.RUnlock()

	if ok {
		return conns, nil
	}

	client, err := NewReplicatedClient(addr)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	p.clients[addr] = []*ReplicatedClient{client}
	p.mu.Unlock()

	return p.clients[addr], nil
}

func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, conns := range p.clients {
		for _, c := range conns {
			c.Close()
		}
	}
	p.clients = make(map[string][]*ReplicatedClient)
}