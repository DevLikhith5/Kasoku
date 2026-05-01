package handler

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/DevLikhith5/kasoku/internal/rpc"
)

// decodeInternal decodes a replication request body, dispatching to gob or JSON
// based on the Content-Type header. This allows rolling upgrades where some
// nodes send gob and others send JSON.
func decodeInternal(r *http.Request, v interface{}) error {
	ct := r.Header.Get("Content-Type")
	if strings.Contains(ct, rpc.GobContentType) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return err
		}
		if len(body) == 0 {
			return nil
		}
		return rpc.GobDecode(body, v)
	}
	// Default: JSON (backwards-compatible with older nodes / CLI tools)
	return json.NewDecoder(r.Body).Decode(v)
}
