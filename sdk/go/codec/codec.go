// Package codec provides payload encoding and decoding for workflow data.
//
// Built-in implementations:
//   - JSONCodec: JSON serialization
//   - BinaryCodec: raw byte passthrough
//
// Usage:
//
//	codec := codec.NewJSONCodec()
//	data, err := codec.Encode(map[string]string{"key": "value"})
//	result, err := codec.Decode(data)
package codec

import (
	"encoding/json"
	"fmt"
)

// PayloadCodec defines the interface for payload encoding/decoding.
type PayloadCodec interface {
	// Encode serializes data to bytes.
	Encode(data interface{}) ([]byte, error)

	// Decode deserializes bytes to a Go value.
	Decode(data []byte) (interface{}, error)
}

// JSONCodec encodes/decodes payloads as JSON.
type JSONCodec struct{}

// NewJSONCodec creates a new JSON codec.
func NewJSONCodec() *JSONCodec {
	return &JSONCodec{}
}

// Encode serializes data to JSON bytes.
func (c *JSONCodec) Encode(data interface{}) ([]byte, error) {
	if data == nil {
		return []byte{}, nil
	}
	if b, ok := data.([]byte); ok {
		return b, nil
	}
	return json.Marshal(data)
}

// Decode deserializes JSON bytes to a Go value.
func (c *JSONCodec) Decode(data []byte) (interface{}, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var result interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("codec: JSON decode failed: %w", err)
	}
	return result, nil
}

// DecodeInto deserializes JSON bytes into a specific Go type.
func (c *JSONCodec) DecodeInto(data []byte, v interface{}) error {
	if len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("codec: JSON decode failed: %w", err)
	}
	return nil
}

// BinaryCodec passes bytes through unchanged.
type BinaryCodec struct{}

// NewBinaryCodec creates a new binary codec.
func NewBinaryCodec() *BinaryCodec {
	return &BinaryCodec{}
}

// Encode returns the data as-is (must be []byte).
func (c *BinaryCodec) Encode(data interface{}) ([]byte, error) {
	if data == nil {
		return []byte{}, nil
	}
	b, ok := data.([]byte)
	if !ok {
		return nil, fmt.Errorf("codec: BinaryCodec expects []byte, got %T", data)
	}
	return b, nil
}

// Decode returns the data as-is.
func (c *BinaryCodec) Decode(data []byte) (interface{}, error) {
	return data, nil
}

// NullCodec encodes everything as empty bytes.
type NullCodec struct{}

// NewNullCodec creates a new null codec.
func NewNullCodec() *NullCodec {
	return &NullCodec{}
}

// Encode returns empty bytes.
func (c *NullCodec) Encode(_ interface{}) ([]byte, error) {
	return []byte{}, nil
}

// Decode returns nil.
func (c *NullCodec) Decode(_ []byte) (interface{}, error) {
	return nil, nil
}

// ChainCodec chains multiple codecs together.
// Encoding applies codecs left-to-right; decoding applies right-to-left.
type ChainCodec struct {
	codecs []PayloadCodec
}

// NewChainCodec creates a new chain codec.
func NewChainCodec(codecs ...PayloadCodec) (*ChainCodec, error) {
	if len(codecs) == 0 {
		return nil, fmt.Errorf("codec: ChainCodec requires at least one codec")
	}
	return &ChainCodec{codecs: codecs}, nil
}

// Encode applies all codecs in order.
func (c *ChainCodec) Encode(data interface{}) ([]byte, error) {
	var current interface{} = data
	for _, codec := range c.codecs {
		encoded, err := codec.Encode(current)
		if err != nil {
			return nil, err
		}
		current = encoded
	}
	if b, ok := current.([]byte); ok {
		return b, nil
	}
	return c.codecs[len(c.codecs)-1].Encode(current)
}

// Decode applies all codecs in reverse order.
func (c *ChainCodec) Decode(data []byte) (interface{}, error) {
	var current interface{} = data
	for i := len(c.codecs) - 1; i >= 0; i-- {
		decoded, err := c.codecs[i].Decode(current.([]byte))
		if err != nil {
			return nil, err
		}
		current = decoded
	}
	return current, nil
}
