package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClient_NewClient(t *testing.T) {
	client := NewClient("http://localhost:8080")
	if client == nil {
		t.Fatal("expected non-nil client")
	}
	if client.baseURL != "http://localhost:8080" {
		t.Errorf("expected http://localhost:8080, got %s", client.baseURL)
	}
	if client.httpClient == nil {
		t.Fatal("expected non-nil http client")
	}
	if client.httpClient.Timeout != 10*time.Second {
		t.Errorf("expected 10s timeout, got %v", client.httpClient.Timeout)
	}
}

func TestClient_ReplicatedPut(t *testing.T) {
	var receivedKey string
	var receivedValue []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT, got %s", r.Method)
		}
		if r.URL.Path != "/internal/replicate" {
			t.Errorf("expected /internal/replicate, got %s", r.URL.Path)
		}

		var req ReplicatedWriteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("failed to decode request: %v", err)
		}

		receivedKey = req.Key
		receivedValue = req.Value

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ReplicatedWriteResponse{Success: true})
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	err := client.ReplicatedPut(ctx, "test-key", []byte("test-value"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if receivedKey != "test-key" {
		t.Errorf("expected test-key, got %s", receivedKey)
	}
	if string(receivedValue) != "test-value" {
		t.Errorf("expected test-value, got %s", string(receivedValue))
	}
}

func TestClient_ReplicatedGet(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}

		var req ReplicatedReadRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("failed to decode request: %v", err)
		}

		if req.Key != "test-key" {
			t.Errorf("expected test-key, got %s", req.Key)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ReplicatedReadResponse{
			Found: true,
			Value: []byte("test-value"),
		})
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	value, found, err := client.ReplicatedGet(ctx, "test-key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Fatal("expected key to be found")
	}
	if string(value) != "test-value" {
		t.Errorf("expected test-value, got %s", string(value))
	}
}

func TestClient_ReplicatedGetNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ReplicatedReadResponse{Found: false})
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	value, found, err := client.ReplicatedGet(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Fatal("expected key not to be found")
	}
	if value != nil {
		t.Errorf("expected nil value, got %v", value)
	}
}

func TestClient_ReplicatedDelete(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ReplicatedDeleteResponse{
			Success: true,
			Deleted: true,
		})
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	deleted, err := client.ReplicatedDelete(ctx, "test-key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !deleted {
		t.Fatal("expected key to be deleted")
	}
}

func TestClient_ReplicatedDeleteNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ReplicatedDeleteResponse{
			Success: true,
			Deleted: false,
		})
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	deleted, err := client.ReplicatedDelete(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if deleted {
		t.Fatal("expected key not to be deleted")
	}
}

func TestClient_HealthCheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Errorf("expected /health, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	err := client.HealthCheck(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClient_HealthCheckFailing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	err := client.HealthCheck(ctx)
	if err == nil {
		t.Fatal("expected error for unhealthy node")
	}
}

func TestClient_ContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate slow response
		time.Sleep(100 * time.Millisecond)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := client.ReplicatedPut(ctx, "test", []byte("value"))
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestClient_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{invalid json}`))
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	var resp ReplicatedReadResponse
	err := client.doRequest(ctx, http.MethodPost, server.URL+"/internal/replicate/get",
		ReplicatedReadRequest{Key: "test"}, &resp)

	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestClient_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"internal error"}`))
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	err := client.ReplicatedPut(ctx, "test", []byte("value"))
	if err == nil {
		t.Fatal("expected error for server error")
	}
}

func TestClient_ConnectionRefused(t *testing.T) {
	client := NewClient("http://localhost:1") // Invalid port
	ctx := context.Background()

	err := client.ReplicatedPut(ctx, "test", []byte("value"))
	if err == nil {
		t.Fatal("expected error for connection refused")
	}
}

func TestClient_DoRequestWithNilBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	err := client.doRequest(ctx, http.MethodGet, server.URL+"/health", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClient_DoRequestWithNilResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	err := client.doRequest(ctx, http.MethodPost, server.URL,
		ReplicatedWriteRequest{Key: "test", Value: []byte("value")}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClient_MarshalingError(t *testing.T) {
	client := NewClient("http://localhost:8080")
	ctx := context.Background()

	// Create a request with unmarshalable data (channel)
	type BadRequest struct {
		Chan chan int `json:"chan"`
	}

	err := client.doRequest(ctx, http.MethodPost, "http://localhost:8080",
		BadRequest{Chan: make(chan int)}, nil)

	if err == nil {
		t.Fatal("expected error for unmarshalable data")
	}
}

func BenchmarkClient_ReplicatedPut(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ReplicatedWriteResponse{Success: true})
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		client.ReplicatedPut(ctx, key, value)
	}
}

func BenchmarkClient_ReplicatedGet(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ReplicatedReadResponse{
			Found: true,
			Value: []byte("test-value"),
		})
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		client.ReplicatedGet(ctx, key)
	}
}

func BenchmarkClient_HealthCheck(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.HealthCheck(ctx)
	}
}
