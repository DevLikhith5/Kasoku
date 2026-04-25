package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	storage "github.com/DevLikhith5/kasoku/internal/store"
)

type Client struct {
	httpClient *http.Client
	baseURL    string
}

func NewClient(nodeAddr string) *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: 2 * time.Second,
			Transport: &http.Transport{
				MaxIdleConnsPerHost:   500,
				MaxConnsPerHost:     500,
				IdleConnTimeout:     90 * time.Second,
				DisableKeepAlives:   false,
				ResponseHeaderTimeout: 1 * time.Second,
			},
		},
		baseURL: nodeAddr,
	}
}

type ReplicatedWriteRequest struct {
	Key         string            `json:"key"`
	Value       []byte            `json:"value"`
	VectorClock map[string]uint64 `json:"vector_clock,omitempty"`
}

type ReplicatedWriteResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

type ReplicatedReadRequest struct {
	Key string `json:"key"`
}

type ReplicatedReadResponse struct {
	Found     bool   `json:"found"`
	Value     []byte `json:"value,omitempty"`
	Tombstone bool   `json:"tombstone,omitempty"`
	Error     string `json:"error,omitempty"`
}

type ReplicatedDeleteRequest struct {
	Key string `json:"key"`
}

type ReplicatedDeleteResponse struct {
	Success bool   `json:"success"`
	Deleted bool   `json:"deleted"`
	Error   string `json:"error,omitempty"`
}

func (c *Client) ReplicatedPut(ctx context.Context, key string, value []byte) error {
	reqBody := ReplicatedWriteRequest{
		Key:   key,
		Value: value,
	}

	url := fmt.Sprintf("%s/internal/replicate", c.baseURL)
	return c.doRequest(ctx, http.MethodPut, url, reqBody, nil)
}

func (c *Client) ReplicatedPutBinary(ctx context.Context, key string, value []byte) error {
	reqBody := ReplicatedWriteRequest{
		Key:   key,
		Value: value,
	}

	url := fmt.Sprintf("%s/internal/replicate", c.baseURL)
	return c.doInternalRequest(ctx, http.MethodPut, url, reqBody, nil)
}

func (c *Client) ReplicatedGet(ctx context.Context, key string) ([]byte, bool, error) {
	reqBody := ReplicatedReadRequest{
		Key: key,
	}

	url := fmt.Sprintf("%s/internal/replicate", c.baseURL)

	var resp ReplicatedReadResponse
	err := c.doRequest(ctx, http.MethodGet, url, reqBody, &resp)
	if err != nil {
		return nil, false, err
	}

	if !resp.Found {
		return nil, false, nil
	}

	if resp.Tombstone {
		return nil, false, nil
	}

	return resp.Value, true, nil
}

func (c *Client) ReplicatedGetForDebug(ctx context.Context, key string) (*ReplicatedReadResponse, error) {
	reqBody := ReplicatedReadRequest{
		Key: key,
	}

	url := fmt.Sprintf("%s/internal/replicate", c.baseURL)

	var resp ReplicatedReadResponse
	err := c.doRequest(ctx, http.MethodGet, url, reqBody, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (c *Client) DebugKey(ctx context.Context, key string) (map[string]any, error) {
	url := fmt.Sprintf("%s/internal/debug/key/%s", c.baseURL, key)

	var resp map[string]any
	err := c.doRequest(ctx, http.MethodGet, url, nil, &resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *Client) ReplicatedGetEntry(ctx context.Context, key string) (storage.Entry, bool, error) {
	reqBody := ReplicatedReadRequest{
		Key: key,
	}

	url := fmt.Sprintf("%s/internal/replicate/get", c.baseURL)

	var resp map[string]any
	err := c.doRequest(ctx, http.MethodPost, url, reqBody, &resp)
	if err != nil {
		return storage.Entry{}, false, err
	}

	found, ok := resp["found"].(bool)
	if !ok || !found {
		return storage.Entry{}, false, nil
	}

	// Safe type assertions to prevent panic on malformed response
	var keyStr string
	var valStr string
	var versionNum float64
	var tombstoneBool bool
	var isOK bool

	keyStr, isOK = resp["key"].(string)
	if !isOK {
		return storage.Entry{}, false, fmt.Errorf("invalid key type in response")
	}
	valStr, isOK = resp["value"].(string)
	if !isOK {
		return storage.Entry{}, false, fmt.Errorf("invalid value type in response")
	}
	versionNum, isOK = resp["version"].(float64)
	if !isOK {
		return storage.Entry{}, false, fmt.Errorf("invalid version type in response")
	}
	tombstoneBool, isOK = resp["tombstone"].(bool)
	if !isOK {
		return storage.Entry{}, false, fmt.Errorf("invalid tombstone type in response")
	}

	entry := storage.Entry{
		Key:       keyStr,
		Value:     []byte(valStr),
		Version:   uint64(versionNum),
		Tombstone: tombstoneBool,
	}

	return entry, true, nil
}

func (c *Client) ReplicatedDelete(ctx context.Context, key string) (bool, error) {
	reqBody := ReplicatedDeleteRequest{
		Key: key,
	}

	url := fmt.Sprintf("%s/internal/replicate", c.baseURL)

	var resp ReplicatedDeleteResponse
	err := c.doRequest(ctx, http.MethodDelete, url, reqBody, &resp)
	if err != nil {
		return false, err
	}

	return resp.Deleted, nil
}

func (c *Client) doRequest(ctx context.Context, method, url string, body, result interface{}) error {
	var reqBody io.Reader

	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to marshal request: %w", err)
		}
		reqBody = bytes.NewReader(data)
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

// doInternalRequest performs an inter-node RPC using binary gob encoding.
// This is significantly faster than JSON-based doRequest and is used for
// high-throughput replication traffic.
func (c *Client) doInternalRequest(ctx context.Context, method, url string, body, result interface{}) error {
	var reqBody io.Reader

	if body != nil {
		data, err := GobEncode(body)
		if err != nil {
			return fmt.Errorf("failed to gob encode request: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", GobContentType)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("internal request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("internal request failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	if result != nil {
		if err := GobDecodeStream(resp.Body, result); err != nil {
			return fmt.Errorf("failed to gob decode response: %w", err)
		}
	}

	return nil
}

type GossipRequest struct {
	Members []string `json:"members"`
}

type GossipResponse struct {
	Members []string `json:"members"`
}

func (c *Client) Gossip(ctx context.Context, ourMembers []string) ([]string, error) {
	reqBody := GossipRequest{
		Members: ourMembers,
	}

	url := fmt.Sprintf("%s/internal/gossip", c.baseURL)

	var resp GossipResponse
	err := c.doRequest(ctx, http.MethodPost, url, reqBody, &resp)
	if err != nil {
		return nil, err
	}

	return resp.Members, nil
}

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

type GossipStateRequest struct {
	NodeID        string            `json:"node_id"`
	NodeAddr      string            `json:"node_addr"`
	Version       uint64            `json:"version"`
	LastHeartbeat int64             `json:"last_heartbeat"`
	Membership    map[string]string `json:"membership"`
}

type GossipStateResponse struct {
	NodeID        string            `json:"node_id"`
	NodeAddr      string            `json:"node_addr"`
	Version       uint64            `json:"version"`
	LastHeartbeat int64             `json:"last_heartbeat"`
	Membership    map[string]string `json:"membership"`
}

func (c *Client) ExchangeGossip(ctx context.Context, state *GossipStateRequest) (*GossipStateResponse, error) {
	url := fmt.Sprintf("%s/internal/gossip/state", c.baseURL)

	var resp GossipStateResponse
	err := c.doRequest(ctx, http.MethodPost, url, state, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

type MerkleRootRequest struct {
	Keys []string `json:"keys"`
}

type MerkleRootResponse struct {
	RootHash []byte `json:"root_hash"`
}

func (c *Client) GetMerkleRoot(ctx context.Context) ([]byte, error) {
	url := fmt.Sprintf("%s/internal/merkle/root", c.baseURL)

	var resp MerkleRootResponse
	err := c.doRequest(ctx, http.MethodGet, url, nil, &resp)
	if err != nil {
		return nil, err
	}

	return resp.RootHash, nil
}

type KeyDiffRequest struct {
	Keys []string `json:"keys"`
}

type KeyDiffResponse struct {
	Keys []string `json:"keys"`
}

func (c *Client) GetKeyDifferences(ctx context.Context, keys []string) ([]string, error) {
	url := fmt.Sprintf("%s/internal/merkle/diff", c.baseURL)

	reqBody := KeyDiffRequest{Keys: keys}
	var resp KeyDiffResponse
	err := c.doRequest(ctx, http.MethodPost, url, reqBody, &resp)
	if err != nil {
		return nil, err
	}

	return resp.Keys, nil
}

type NodeInfoResponse struct {
	NodeID string    `json:"node_id"`
	Addr   string    `json:"addr"`
	Stats  NodeStats `json:"stats"`
}

type NodeStats struct {
	KeyCount  int64 `json:"key_count"`
	DiskBytes int64 `json:"disk_bytes"`
	MemBytes  int64 `json:"mem_bytes"`
}

func (c *Client) GetNodeInfo(ctx context.Context) (*NodeInfoResponse, error) {
	url := fmt.Sprintf("%s/api/v1/node", c.baseURL)

	var resp NodeInfoResponse
	err := c.doRequest(ctx, http.MethodGet, url, nil, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}
