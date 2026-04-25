package grpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/DevLikhith5/kasoku/api"
	storage "github.com/DevLikhith5/kasoku/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

var (
	grpcRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kasoku_grpc_requests_total",
			Help: "Total number of gRPC requests.",
		},
		[]string{"method", "status"},
	)
	grpcRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kasoku_grpc_request_duration_seconds",
			Help:    "gRPC request latency.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
		},
		[]string{"method"},
	)
)

type Logger interface {
	Debug(msg string, args ...any)
	Error(msg string, args ...any)
	Warn(msg string, args ...any)
}

type ClusterInterface interface {
	ReplicatedPut(ctx context.Context, key string, value []byte) error
	ReplicatedDelete(ctx context.Context, key string) error
	ReplicatedBatchPut(ctx context.Context, entries map[string][]byte) error
	IsDistributed() bool
}

type Server struct {
	api.UnimplementedKasokuServiceServer
	store    storage.StorageEngine
	nodeID  string
	addr    string
	logger  Logger
	cluster ClusterInterface

	clients   map[string]api.KasokuServiceClient
	clientsMu sync.RWMutex

	grpcServer *grpc.Server
}

func NewServer(store storage.StorageEngine, nodeID, addr string, logger Logger) *Server {
	return &Server{
		store:   store,
		nodeID:  nodeID,
		addr:    addr,
		logger:  logger,
		clients: make(map[string]api.KasokuServiceClient),
	}
}

func (s *Server) SetCluster(c ClusterInterface) {
	s.cluster = c
}

func (s *Server) Put(ctx context.Context, req *api.PutRequest) (*api.PutResponse, error) {
	start := time.Now()

	if err := s.store.Put(req.Key, req.Value); err != nil {
		s.logger.Error("gRPC put failed", "key", req.Key, "error", err, "duration_ms", time.Since(start).Milliseconds())
		return &api.PutResponse{Success: false, Error: err.Error()}, nil
	}

	if s.cluster != nil && s.cluster.IsDistributed() {
		if err := s.cluster.ReplicatedPut(ctx, req.Key, req.Value); err != nil {
			s.logger.Error("gRPC replicated put failed", "key", req.Key, "error", err)
			return &api.PutResponse{Success: false, Error: err.Error()}, nil
		}
	}

	return &api.PutResponse{Success: true}, nil
}

func (s *Server) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	entry, err := s.store.Get(req.Key)
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			return &api.GetResponse{}, nil
		}
		return &api.GetResponse{Error: err.Error()}, nil
	}

	return &api.GetResponse{
		Entry: &api.Entry{
			Key:       entry.Key,
			Value:     entry.Value,
			Version:   entry.Version,
			Timestamp: entry.TimeStamp.UnixNano(),
			Tombstone: entry.Tombstone,
		},
	}, nil
}

func (s *Server) BatchPut(ctx context.Context, req *api.BatchPutRequest) (*api.BatchPutResponse, error) {
	entries := make([]storage.Entry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = storage.Entry{
			Key:   e.Key,
			Value: e.Value,
		}
	}

	err := s.store.BatchPut(entries)
	if err != nil {
		s.logger.Error("gRPC batch put failed", "error", err)
		return &api.BatchPutResponse{Count: 0, Error: err.Error()}, nil
	}

	count := len(entries)

	// Use background replicator for async batch replication
	if s.cluster != nil && s.cluster.IsDistributed() {
		entriesMap := make(map[string][]byte)
		for _, e := range req.Entries {
			entriesMap[e.Key] = e.Value
		}
		if err := s.cluster.ReplicatedBatchPut(ctx, entriesMap); err != nil {
			s.logger.Error("gRPC batch replicate failed", "error", err)
		}
	}

	return &api.BatchPutResponse{Count: int32(count)}, nil
}

func (s *Server) Delete(ctx context.Context, req *api.DeleteRequest) (*api.DeleteResponse, error) {
	if err := s.store.Delete(req.Key); err != nil {
		return &api.DeleteResponse{Success: false, Error: err.Error()}, nil
	}

	if s.cluster != nil && s.cluster.IsDistributed() {
		if err := s.cluster.ReplicatedDelete(ctx, req.Key); err != nil {
			return &api.DeleteResponse{Success: false, Error: err.Error()}, nil
		}
	}

	return &api.DeleteResponse{Success: true}, nil
}

func (s *Server) Scan(ctx context.Context, req *api.ScanRequest) (*api.ScanResponse, error) {
	entries, err := s.store.Scan(req.Prefix)
	if err != nil {
		return &api.ScanResponse{Error: err.Error()}, nil
	}

	apiEntries := make([]*api.Entry, len(entries))
	for i, e := range entries {
		apiEntries[i] = &api.Entry{
			Key:       e.Key,
			Value:     e.Value,
			Version:   e.Version,
			Timestamp: e.TimeStamp.UnixNano(),
			Tombstone: e.Tombstone,
		}
	}

	return &api.ScanResponse{Entries: apiEntries}, nil
}

func (s *Server) MultiGet(ctx context.Context, req *api.MultiGetRequest) (*api.MultiGetResponse, error) {
	result := make(map[string]*api.Entry)

	for _, key := range req.Keys {
		entry, err := s.store.Get(key)
		if err != nil {
			continue
		}

		result[key] = &api.Entry{
			Key:       entry.Key,
			Value:     entry.Value,
			Version:   entry.Version,
			Timestamp: entry.TimeStamp.UnixNano(),
			Tombstone: entry.Tombstone,
		}
	}

	return &api.MultiGetResponse{Entries: result}, nil
}

func (s *Server) Replicate(stream api.KasokuService_ReplicateServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			break
		}

		if req.Entry == nil {
			continue
		}

		if err := s.store.Put(req.Entry.Key, req.Entry.Value); err != nil {
			s.logger.Error("gRPC replicate failed", "key", req.Entry.Key, "error", err)
		}

		if err := stream.Send(&api.ReplicateResponse{Success: true}); err != nil {
			break
		}
	}

	return nil
}

func (s *Server) Sync(ctx context.Context, req *api.SyncRequest) (*api.SyncResponse, error) {
	entries, err := s.store.Scan("")
	if err != nil {
		return &api.SyncResponse{}, err
	}

	apiEntries := make([]*api.Entry, len(entries))
	for i, e := range entries {
		apiEntries[i] = &api.Entry{
			Key:       e.Key,
			Value:     e.Value,
			Version:   e.Version,
			Timestamp: e.TimeStamp.UnixNano(),
			Tombstone: e.Tombstone,
		}
	}

	return &api.SyncResponse{Entries: apiEntries, Version: uint64(s.store.Stats().KeyCount)}, nil
}

func (s *Server) getOrCreateClient(addr string) (api.KasokuServiceClient, error) {
	s.clientsMu.RLock()
	client, ok := s.clients[addr]
	s.clientsMu.RUnlock()

	if ok {
		return client, nil
	}

	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()

	if client, ok := s.clients[addr]; ok {
		return client, nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	client = api.NewKasokuServiceClient(conn)
	s.clients[addr] = client

	return client, nil
}

func (s *Server) Start(port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}

	s.grpcServer = grpc.NewServer()
	api.RegisterKasokuServiceServer(s.grpcServer, s)
	grpc_health_v1.RegisterHealthServer(s.grpcServer, health.NewServer())

	go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			s.logger.Error("gRPC server error", "error", err)
		}
	}()

	s.logger.Debug("gRPC server started", "port", port)
	return nil
}

func (s *Server) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
}

func GetPeerAddress(ctx context.Context) (string, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}

	values := md.Get("peer-addr")
	if len(values) == 0 {
		return "", false
	}

	return values[0], true
}