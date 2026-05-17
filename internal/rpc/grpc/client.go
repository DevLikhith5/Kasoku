package grpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/DevLikhith5/kasoku/api"
	storage "github.com/DevLikhith5/kasoku/internal/store"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"google.golang.org/grpc/metadata"
)

type ReplicatedClient struct {
	addr   string
	conn   *grpc.ClientConn
	client api.KasokuServiceClient
	mu     sync.Mutex
}

func NewReplicatedClient(addr string) (*ReplicatedClient, error) {
	// Use non-blocking dial so connections are established lazily.
	// This prevents blocking for seconds when peers are unreachable at startup.
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024*32), grpc.MaxCallSendMsgSize(1024*1024*32)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create client for %s: %w", addr, err)
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



// replicationContext creates a context with the x-replication header set.
// The receiving gRPC server checks this header to bypass the cluster layer
// and write directly to local store, preventing cascading replication loops.
func replicationContext(ctx context.Context) context.Context {
	md := metadata.Pairs("x-replication", "true")
	return metadata.NewOutgoingContext(ctx, md)
}

func (c *ReplicatedClient) ReplicatedPut(ctx context.Context, key string, value []byte) error {
	_, err := c.client.Put(ctx, &api.PutRequest{
		Key:   key,
		Value: value,
	})
	return err
}

// ReplicatedPutBinary forwards a write to the coordinator.
// It DOES NOT set x-replication metadata, because the receiving node
// MUST act as the coordinator and replicate the write to its peers.
func (c *ReplicatedClient) ReplicatedPutBinary(ctx context.Context, entry storage.Entry) error {
	req := &api.PutRequest{
		Key:       entry.Key,
		Value:     entry.Value,
		Version:   entry.Version,
		Timestamp: entry.TimeStamp.UnixNano(),
	}
	if entry.VectorClock != nil {
		req.VectorClock = make(map[string]uint32)
		for k, v := range entry.VectorClock {
			req.VectorClock[k] = uint32(v)
		}
	}
	_, err := c.client.Put(ctx, req)
	return err
}

// ReplicatedPutBinaryInternal is used for inter-node replication.
// It sets x-replication metadata so the receiving node stores locally
// without re-entering the cluster coordinator logic.
func (c *ReplicatedClient) ReplicatedPutBinaryInternal(ctx context.Context, entry storage.Entry) error {
	ctx = replicationContext(ctx)
	req := &api.PutRequest{
		Key:       entry.Key,
		Value:     entry.Value,
		Version:   entry.Version,
		Timestamp: entry.TimeStamp.UnixNano(),
	}
	if entry.VectorClock != nil {
		req.VectorClock = make(map[string]uint32)
		for k, v := range entry.VectorClock {
			req.VectorClock[k] = uint32(v)
		}
	}
	_, err := c.client.Put(ctx, req)
	return err
}

func (c *ReplicatedClient) ReplicatedGet(ctx context.Context, key string) ([]byte, bool, error) {
	ctx = replicationContext(ctx)
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

func (c *ReplicatedClient) ReplicatedGetEntry(ctx context.Context, key string) (storage.Entry, bool, error) {
	ctx = replicationContext(ctx)
	resp, err := c.client.Get(ctx, &api.GetRequest{Key: key})
	if err != nil {
		return storage.Entry{}, false, err
	}

	if resp.Entry == nil {
		return storage.Entry{}, false, nil
	}

	entry := storage.Entry{
		Key:       resp.Entry.Key,
		Value:     resp.Entry.Value,
		Version:   resp.Entry.Version,
		Tombstone: resp.Entry.Tombstone,
		TimeStamp: time.Unix(0, resp.Entry.Timestamp),
	}
	return entry, true, nil
}

func (c *ReplicatedClient) ReplicatedDelete(ctx context.Context, entry storage.Entry) error {
	ctx = replicationContext(ctx)
	req := &api.DeleteRequest{
		Key:       entry.Key,
		Version:   entry.Version,
		Timestamp: entry.TimeStamp.UnixNano(),
	}
	if entry.VectorClock != nil {
		req.VectorClock = make(map[string]uint32)
		for k, v := range entry.VectorClock {
			req.VectorClock[k] = uint32(v)
		}
	}
	_, err := c.client.Delete(ctx, req)
	return err
}

// BatchReplicatedPut forwards a batch write to the coordinator.
// It DOES NOT set x-replication metadata.
func (c *ReplicatedClient) BatchReplicatedPut(ctx context.Context, entries []storage.Entry) (int, error) {
	req := &api.BatchPutRequest{
		Entries: make([]*api.Entry, len(entries)),
	}

	for i, e := range entries {
		req.Entries[i] = &api.Entry{
			Key:       e.Key,
			Value:     e.Value,
			Version:   e.Version,
			Timestamp: e.TimeStamp.UnixNano(),
			Tombstone: e.Tombstone,
		}
		if e.VectorClock != nil {
			req.Entries[i].VectorClock = make(map[string]uint32)
			for k, v := range e.VectorClock {
				req.Entries[i].VectorClock[k] = uint32(v)
			}
		}
	}

	resp, err := c.client.BatchPut(ctx, req)
	if err != nil {
		return 0, err
	}

	return int(resp.Count), nil
}

// BatchReplicatedPutInternal is used for inter-node batch replication.
// It sets x-replication metadata so the receiving node stores locally
// without re-entering the cluster coordinator logic.
func (c *ReplicatedClient) BatchReplicatedPutInternal(ctx context.Context, entries []storage.Entry) (int, error) {
	ctx = replicationContext(ctx)
	req := &api.BatchPutRequest{
		Entries: make([]*api.Entry, len(entries)),
	}

	for i, e := range entries {
		req.Entries[i] = &api.Entry{
			Key:       e.Key,
			Value:     e.Value,
			Version:   e.Version,
			Timestamp: e.TimeStamp.UnixNano(),
			Tombstone: e.Tombstone,
		}
		if e.VectorClock != nil {
			req.Entries[i].VectorClock = make(map[string]uint32)
			for k, v := range e.VectorClock {
				req.Entries[i].VectorClock[k] = uint32(v)
			}
		}
	}

	resp, err := c.client.BatchPut(ctx, req)
	if err != nil {
		return 0, err
	}

	return int(resp.Count), nil
}

func (c *ReplicatedClient) BatchReplicatedGet(ctx context.Context, keys []string) (map[string][]byte, error) {
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
		maxConns: 128,
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