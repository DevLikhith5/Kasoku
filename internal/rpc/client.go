package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client handles RPC communication between nodes
type Client struct {
	httpClient *http.Client
	baseURL    string
}

// NewClient creates a new RPC client for communicating with a node
func NewClient(nodeAddr string) *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		baseURL: nodeAddr,
	}
}

// ReplicatedWriteRequest represents a write request to be replicated
type ReplicatedWriteRequest struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// ReplicatedWriteResponse is the response from a replicated write
type ReplicatedWriteResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// ReplicatedReadRequest represents a read request
type ReplicatedReadRequest struct {
	Key string `json:"key"`
}

// ReplicatedReadResponse is the response from a replicated read
type ReplicatedReadResponse struct {
	Found   bool   `json:"found"`
	Value   []byte `json:"value,omitempty"`
	Error   string `json:"error,omitempty"`
}

// ReplicatedDeleteRequest represents a delete request to be replicated
type ReplicatedDeleteRequest struct {
	Key string `json:"key"`
}

// ReplicatedDeleteResponse is the response from a replicated delete
type ReplicatedDeleteResponse struct {
	Success bool   `json:"success"`
	Deleted bool   `json:"deleted"`
	Error   string `json:"error,omitempty"`
}

// ReplicatedPut sends a write request to a remote node for replication
func (c *Client) ReplicatedPut(ctx context.Context, key string, value []byte) error {
	reqBody := ReplicatedWriteRequest{
		Key:   key,
		Value: value,
	}

	url := fmt.Sprintf("%s/internal/replicate/put", c.baseURL)
	return c.doRequest(ctx, http.MethodPut, url, reqBody, nil)
}

// ReplicatedGet fetches a value from a remote node
func (c *Client) ReplicatedGet(ctx context.Context, key string) ([]byte, bool, error) {
	reqBody := ReplicatedReadRequest{
		Key: key,
	}

	url := fmt.Sprintf("%s/internal/replicate/get", c.baseURL)

	var resp ReplicatedReadResponse
	err := c.doRequest(ctx, http.MethodPost, url, reqBody, &resp)
	if err != nil {
		return nil, false, err
	}

	if !resp.Found {
		return nil, false, nil
	}

	return resp.Value, true, nil
}

// ReplicatedDelete sends a delete request to a remote node
func (c *Client) ReplicatedDelete(ctx context.Context, key string) (bool, error) {
	reqBody := ReplicatedDeleteRequest{
		Key: key,
	}

	url := fmt.Sprintf("%s/internal/replicate/delete", c.baseURL)

	var resp ReplicatedDeleteResponse
	err := c.doRequest(ctx, http.MethodDelete, url, reqBody, &resp)
	if err != nil {
		return false, err
	}

	return resp.Deleted, nil
}

// doRequest performs an HTTP request with JSON payload
func (c *Client) doRequest(ctx context.Context, method, url string, body, result interface{}) error {
	var reqBody io.Reader

	if body != nil {
		jsonData, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to marshal request: %w", err)
		}
		reqBody = bytes.NewReader(jsonData)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 300 {
		return fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	if result != nil {
		if err := json.Unmarshal(respBody, result); err != nil {
			return fmt.Errorf("failed to unmarshal response: %w", err)
		}
	}

	return nil
}

// HealthCheck checks if a remote node is healthy
func (c *Client) HealthCheck(ctx context.Context) error {
	url := fmt.Sprintf("%s/health", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create health check request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unhealthy status: %d", resp.StatusCode)
	}

	return nil
}
