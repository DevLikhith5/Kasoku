package rpc

import (
	"context"
	"fmt"
	"net/http"
)

// BatchWriteEntry is a single key-value pair within a batch.
type BatchWriteEntry struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// BatchWriteRequest is the body for the batch replication endpoint.
type BatchWriteRequest struct {
	Entries []BatchWriteEntry `json:"entries"`
}

// BatchWriteResponse is the response from the batch replication endpoint.
type BatchWriteResponse struct {
	Success  bool   `json:"success"`
	Applied  int    `json:"applied"`
	Error    string `json:"error,omitempty"`
}

// BatchReplicatedPut sends a batch of key-value pairs to a peer node in a
// single HTTP round-trip using gob binary encoding. This dramatically reduces
// per-key RPC overhead compared to calling ReplicatedPut in a loop.
func (c *Client) BatchReplicatedPut(ctx context.Context, entries []BatchWriteEntry) (int, error) {
	if len(entries) == 0 {
		return 0, nil
	}

	req := BatchWriteRequest{Entries: entries}
	url := fmt.Sprintf("%s/internal/replicate/batch", c.baseURL)

	var resp BatchWriteResponse
	if err := c.doInternalRequest(ctx, http.MethodPost, url, req, &resp); err != nil {
		return 0, err
	}

	if !resp.Success {
		return resp.Applied, fmt.Errorf("batch replicate failed: %s", resp.Error)
	}

	return resp.Applied, nil
}
