package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	storage "github.com/DevLikhith5/kasoku/internal/store"
)

type APIResponse struct {
	Success bool            `json:"success"`
	Data    json.RawMessage `json:"data,omitempty"`
	Error   string          `json:"error,omitempty"`
}

type GetResponse struct {
	Key       string `json:"key"`
	Value     []byte `json:"value"`
	Version   uint64 `json:"version"`
	Timestamp int64  `json:"timestamp"`
}

type BatchGetResponse struct {
	Entries []GetResponse `json:"entries"`
}

type ScanResponse struct {
	Keys []string `json:"keys"`
}

type RemoteEngine struct {
	baseURL string
	client  *http.Client
}

func NewRemoteEngine(url string) *RemoteEngine {
	return &RemoteEngine{
		baseURL: url,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (r *RemoteEngine) doRequest(method, path string, body interface{}, out interface{}) error {
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reqBody = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, r.baseURL+path, reqBody)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return storage.ErrKeyNotFound
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 {
		var apiErr APIResponse
		if err := json.Unmarshal(respBody, &apiErr); err == nil && apiErr.Error != "" {
			return fmt.Errorf("remote error: %s", apiErr.Error)
		}
		return fmt.Errorf("remote error (status %d): %s", resp.StatusCode, string(respBody))
	}

	if out != nil {
		var apiResp APIResponse
		if err := json.Unmarshal(respBody, &apiResp); err != nil {
			return err
		}
		if !apiResp.Success {
			return fmt.Errorf("api error: %s", apiResp.Error)
		}
		if len(apiResp.Data) > 0 {
			if err := json.Unmarshal(apiResp.Data, out); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *RemoteEngine) Get(key string) (storage.Entry, error) {
	var g GetResponse
	err := r.doRequest("GET", "/api/v1/get/"+url.PathEscape(key), nil, &g)
	if err != nil {
		return storage.Entry{}, err
	}
	return storage.Entry{
		Key:       g.Key,
		Value:     g.Value,
		Version:   g.Version,
		TimeStamp: time.Unix(0, g.Timestamp),
	}, nil
}

func (r *RemoteEngine) MultiGet(keys []string) (map[string]storage.Entry, error) {
	var batchReq struct {
		Keys []string `json:"keys"`
	}
	batchReq.Keys = keys

	var batchResp BatchGetResponse
	err := r.doRequest("POST", "/api/v1/batch", batchReq, &batchResp)
	if err != nil {
		return nil, err
	}

	result := make(map[string]storage.Entry, len(batchResp.Entries))
	for _, g := range batchResp.Entries {
		result[g.Key] = storage.Entry{
			Key:       g.Key,
			Value:     g.Value,
			Version:   g.Version,
			TimeStamp: time.Unix(0, g.Timestamp),
		}
	}
	return result, nil
}

func (r *RemoteEngine) Put(key string, value []byte) error {
	type putReq struct {
		Value string `json:"value"`
	}
	return r.doRequest("PUT", "/api/v1/put/"+url.PathEscape(key), putReq{string(value)}, nil)
}

func (r *RemoteEngine) PutAsync(key string, value []byte) error {
	return r.Put(key, value)
}

func (r *RemoteEngine) PutWithVectorClock(key string, value []byte, vc storage.VectorClock) error {
	type putReq struct {
		Value       string            `json:"value"`
		VectorClock map[string]uint64 `json:"vector_clock"`
	}
	vcMap := map[string]uint64(vc)
	return r.doRequest("PUT", "/api/v1/put/"+url.PathEscape(key), putReq{string(value), vcMap}, nil)
}

// BatchPut falls back to sequential Puts for the remote CLI engine.
func (r *RemoteEngine) BatchPut(pairs []storage.Entry) error {
	for _, e := range pairs {
		if err := r.Put(e.Key, e.Value); err != nil {
			return err
		}
	}
	return nil
}

func (r *RemoteEngine) BatchPutAsync(pairs []storage.Entry) error {
	return r.BatchPut(pairs)
}

func (r *RemoteEngine) Delete(key string) error {
	return r.doRequest("DELETE", "/api/v1/delete/"+url.PathEscape(key), nil, nil)
}

func (r *RemoteEngine) Keys() ([]string, error) {
	var s ScanResponse
	err := r.doRequest("GET", "/api/v1/keys", nil, &s)
	if err != nil {
		return nil, err
	}
	return s.Keys, nil
}

func (r *RemoteEngine) Scan(prefix string) ([]storage.Entry, error) {
	var s ScanResponse
	path := "/api/v1/scan"
	if prefix != "" {
		path += "?prefix=" + url.QueryEscape(prefix)
	}
	err := r.doRequest("GET", path, nil, &s)
	if err != nil {
		return nil, err
	}

	entries := make([]storage.Entry, len(s.Keys))
	for i, k := range s.Keys {
		entries[i] = storage.Entry{Key: k}
		// Notice: Engine Scan returns full values remotely, but the server /api/v1/scan only returns keys right now.
		// For CLI matching keys:
	}
	return entries, nil
}

func (r *RemoteEngine) Stats() storage.EngineStats {
	var out struct {
		NodeId string              `json:"node_id"`
		Stats  storage.EngineStats `json:"stats"`
	}
	_ = r.doRequest("GET", "/api/v1/node", nil, &out)
	return out.Stats
}

func (r *RemoteEngine) Close() error {
	return nil
}

func (r *RemoteEngine) Flush() error {
	return fmt.Errorf("administrative command Flush() not supported over remote proxy")
}

func (r *RemoteEngine) TriggerCompaction() {
	// Not supported over remote
	fmt.Println("Administrative command TriggerCompaction() not supported over remote proxy")
}
