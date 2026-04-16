// Package rpc provides binary encoding helpers for inter-node replication messages.
// Using gob instead of JSON for internal RPC gives 2-3x serialization throughput
// since gob skips key name encoding and uses Go native binary types.
package rpc

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
)

func init() {
	// Register all types that will flow over gob encoding
	gob.Register(ReplicatedWriteRequest{})
	gob.Register(ReplicatedWriteResponse{})
	gob.Register(ReplicatedReadRequest{})
	gob.Register(ReplicatedReadResponse{})
	gob.Register(ReplicatedDeleteRequest{})
	gob.Register(ReplicatedDeleteResponse{})
	gob.Register(BatchWriteRequest{})
	gob.Register(BatchWriteResponse{})
	gob.Register(BatchWriteEntry{})
}

// GobContentType is the MIME type used for internal gob-encoded messages.
const GobContentType = "application/octet-stream+gob"

// GobEncode serializes a value to gob bytes.
func GobEncode(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("gob encode: %w", err)
	}
	return buf.Bytes(), nil
}

// GobDecode deserializes gob bytes into a pointer value.
func GobDecode(data []byte, v interface{}) error {
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("gob decode: %w", err)
	}
	return nil
}

// GobDecodeStream reads and deserializes a gob message from a reader.
func GobDecodeStream(r io.Reader, v interface{}) error {
	dec := gob.NewDecoder(r)
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("gob decode stream: %w", err)
	}
	return nil
}
