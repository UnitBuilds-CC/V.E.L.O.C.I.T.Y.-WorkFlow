// Package vctp provides a VCTP (Velocity Transfer Protocol) client for Go.
//
// VctpClient communicates with a Velocity VCTP server over UDP using
// the binary VCTP protocol with 28-byte header + JSON payload + CRC32.
//
// Features:
//   - Frame building with sequence correlation
//   - Auth token injection (JWT / API key)
//   - Idempotency key generation
//   - Timeout handling
//
// Usage:
//
//	client, err := vctp.NewClient(vctp.Config{
//	    ServerAddr: "127.0.0.1:9090",
//	    AuthToken:  "my-jwt-token",
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
//	result, err := client.StartWorkflow(ctx, vctp.StartWorkflowOptions{
//	    WorkflowType: "MyWorkflow",
//	    TotalSteps:   5,
//	})
package vctp

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// ─── Constants ────────────────────────────────────────────────────────────────

const (
	VCTPMagic       uint32 = 0x50544356 // "VCTP"
	VCTPHeaderSize         = 28
	MaxVCTPPayload         = 65479
	DefaultTimeout         = 5 * time.Second
)

// Method IDs
const (
	MethodStartWorkflow    uint64 = 100
	MethodSignalWorkflow   uint64 = 101
	MethodQueryWorkflow    uint64 = 102
	MethodCancelWorkflow   uint64 = 103
	MethodTerminateWorkflow uint64 = 104
	MethodDescribeWorkflow uint64 = 105
	MethodListWorkflows    uint64 = 106
	MethodResetWorkflow    uint64 = 107
	MethodUpdateWorkflow   uint64 = 108
	MethodCompleteWorkflow uint64 = 109
	MethodHealthCheck      uint64 = 500
	MethodCountWorkflows   uint64 = 502
	MethodBatchSignal      uint64 = 503
	MethodBatchTerminate   uint64 = 504
	MethodSignalWithStart  uint64 = 606
)

// ─── Config ───────────────────────────────────────────────────────────────────

// Config holds VCTP client configuration.
type Config struct {
	ServerAddr string        // VCTP server address (host:port)
	AuthToken  string        // JWT bearer token
	APIKey     string        // API key (alternative auth)
	Timeout    time.Duration // Request timeout (default: 5s)
}

// ─── Types ────────────────────────────────────────────────────────────────────

// RpcRequest is the JSON envelope sent as VCTP payload.
type RpcRequest struct {
	Method         uint64            `json:"method"`
	Namespace      string            `json:"namespace,omitempty"`
	WorkflowID     string            `json:"workflow_id,omitempty"`
	Payload        []byte            `json:"payload,omitempty"`
	WorkflowType   string            `json:"workflow_type,omitempty"`
	SignalName     string            `json:"signal_name,omitempty"`
	QueryType      string            `json:"query_type,omitempty"`
	TotalSteps     uint32            `json:"total_steps,omitempty"`
	SignalCount    uint32            `json:"signal_count,omitempty"`
	MaxCount       int64             `json:"max_count,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	AuthToken      string            `json:"auth_token,omitempty"`
	APIKey         string            `json:"api_key,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
}

// RpcResponse is the JSON envelope received from VCTP.
type RpcResponse struct {
	Status     int    `json:"status"`
	Sequence   uint64 `json:"sequence"`
	Error      string `json:"error,omitempty"`
	WorkflowID string `json:"workflow_id,omitempty"`
	RunID      string `json:"run_id,omitempty"`
	RunStatus  string `json:"run_status,omitempty"`
	Count      uint64 `json:"count,omitempty"`
}

// StartWorkflowOptions configures a StartWorkflow call.
type StartWorkflowOptions struct {
	WorkflowType   string
	WorkflowID     string
	Namespace      string
	TotalSteps     uint32
	IdempotencyKey string
}

// StartWorkflowResult holds the result of a StartWorkflow call.
type StartWorkflowResult struct {
	WorkflowID string
	RunID      string
	Status     string
}

// VctpError is returned when a VCTP RPC call returns a non-zero status.
type VctpError struct {
	Status  int
	Message string
}

func (e *VctpError) Error() string {
	return fmt.Sprintf("VCTP error %d: %s", e.Status, e.Message)
}

// ─── Client ───────────────────────────────────────────────────────────────────

// Client is a VCTP UDP client.
type Client struct {
	conn      *net.UDPConn
	config    Config
	sequence  atomic.Uint64
	pending   map[uint64]chan RpcResponse
	pendingMu sync.Mutex
	closed    atomic.Bool
}

// NewClient creates a new VCTP client and connects the UDP socket.
func NewClient(cfg Config) (*Client, error) {
	if cfg.ServerAddr == "" {
		return nil, fmt.Errorf("server address required")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = DefaultTimeout
	}

	addr, err := net.ResolveUDPAddr("udp4", cfg.ServerAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve server addr: %w", err)
	}

	conn, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		return nil, fmt.Errorf("dial UDP: %w", err)
	}

	c := &Client{
		conn:    conn,
		config:  cfg,
		pending: make(map[uint64]chan RpcResponse),
	}

	// Start receiver goroutine
	go c.receiveLoop()

	return c, nil
}

// Close shuts down the client.
func (c *Client) Close() error {
	c.closed.Store(true)
	c.pendingMu.Lock()
	for seq, ch := range c.pending {
		close(ch)
		delete(c.pending, seq)
	}
	c.pendingMu.Unlock()
	return c.conn.Close()
}

// StartWorkflow starts a new workflow execution.
func (c *Client) StartWorkflow(ctx context.Context, opts StartWorkflowOptions) (*StartWorkflowResult, error) {
	ns := opts.Namespace
	if ns == "" {
		ns = "default"
	}
	steps := opts.TotalSteps
	if steps == 0 {
		steps = 10
	}

	req := RpcRequest{
		Method:     MethodStartWorkflow,
		Namespace:  ns,
		WorkflowID: opts.WorkflowID,
		WorkflowType: opts.WorkflowType,
		TotalSteps: steps,
	}
	if opts.IdempotencyKey != "" {
		req.IdempotencyKey = opts.IdempotencyKey
	}

	resp, err := c.sendRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.Status != 0 {
		return nil, &VctpError{Status: resp.Status, Message: resp.Error}
	}

	return &StartWorkflowResult{
		WorkflowID: resp.WorkflowID,
		RunID:      resp.RunID,
		Status:     resp.RunStatus,
	}, nil
}

// SignalWorkflow sends a signal to a running workflow.
func (c *Client) SignalWorkflow(ctx context.Context, workflowID, signalName string, payload []byte) error {
	req := RpcRequest{
		Method:     MethodSignalWorkflow,
		Namespace:  "default",
		WorkflowID: workflowID,
		SignalName: signalName,
		Payload:    payload,
	}
	resp, err := c.sendRequest(ctx, req)
	if err != nil {
		return err
	}
	if resp.Status != 0 {
		return &VctpError{Status: resp.Status, Message: resp.Error}
	}
	return nil
}

// QueryWorkflow queries a workflow's status.
func (c *Client) QueryWorkflow(ctx context.Context, workflowID string) (string, error) {
	req := RpcRequest{
		Method:     MethodQueryWorkflow,
		Namespace:  "default",
		WorkflowID: workflowID,
	}
	resp, err := c.sendRequest(ctx, req)
	if err != nil {
		return "", err
	}
	if resp.Status != 0 {
		return "", &VctpError{Status: resp.Status, Message: resp.Error}
	}
	return resp.RunStatus, nil
}

// DescribeWorkflow gets detailed information about a workflow.
func (c *Client) DescribeWorkflow(ctx context.Context, workflowID string) (*StartWorkflowResult, error) {
	req := RpcRequest{
		Method:     MethodDescribeWorkflow,
		Namespace:  "default",
		WorkflowID: workflowID,
	}
	resp, err := c.sendRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.Status != 0 {
		return nil, &VctpError{Status: resp.Status, Message: resp.Error}
	}
	return &StartWorkflowResult{
		WorkflowID: resp.WorkflowID,
		RunID:      resp.RunID,
		Status:     resp.RunStatus,
	}, nil
}

// CancelWorkflow cancels a running workflow.
func (c *Client) CancelWorkflow(ctx context.Context, workflowID string) error {
	req := RpcRequest{
		Method:     MethodCancelWorkflow,
		Namespace:  "default",
		WorkflowID: workflowID,
	}
	resp, err := c.sendRequest(ctx, req)
	if err != nil {
		return err
	}
	if resp.Status != 0 {
		return &VctpError{Status: resp.Status, Message: resp.Error}
	}
	return nil
}

// TerminateWorkflow forcefully terminates a workflow.
func (c *Client) TerminateWorkflow(ctx context.Context, workflowID string) error {
	req := RpcRequest{
		Method:     MethodTerminateWorkflow,
		Namespace:  "default",
		WorkflowID: workflowID,
	}
	resp, err := c.sendRequest(ctx, req)
	if err != nil {
		return err
	}
	if resp.Status != 0 {
		return &VctpError{Status: resp.Status, Message: resp.Error}
	}
	return nil
}

// HealthCheck checks server health.
func (c *Client) HealthCheck(ctx context.Context) (string, error) {
	req := RpcRequest{Method: MethodHealthCheck}
	resp, err := c.sendRequest(ctx, req)
	if err != nil {
		return "", err
	}
	return resp.RunStatus, nil
}

// CountWorkflows counts workflow executions.
func (c *Client) CountWorkflows(ctx context.Context, namespace string) (uint64, error) {
	if namespace == "" {
		namespace = "default"
	}
	req := RpcRequest{
		Method:    MethodCountWorkflows,
		Namespace: namespace,
	}
	resp, err := c.sendRequest(ctx, req)
	if err != nil {
		return 0, err
	}
	return resp.Count, nil
}

// GenerateIdempotencyKey generates a random idempotency key.
func GenerateIdempotencyKey() string {
	return uuid.New().String()
}

// ─── Internal ─────────────────────────────────────────────────────────────────

func (c *Client) sendRequest(ctx context.Context, req RpcRequest) (RpcResponse, error) {
	if c.closed.Load() {
		return RpcResponse{}, fmt.Errorf("client closed")
	}

	// Inject auth
	if c.config.AuthToken != "" && req.AuthToken == "" {
		req.AuthToken = c.config.AuthToken
	}
	if c.config.APIKey != "" && req.APIKey == "" {
		req.APIKey = c.config.APIKey
	}

	seq := c.sequence.Add(1)
	reqJSON, err := json.Marshal(req)
	if err != nil {
		return RpcResponse{}, fmt.Errorf("marshal request: %w", err)
	}

	packet := buildPacket(seq, req.Method, reqJSON)

	// Register pending response
	ch := make(chan RpcResponse, 1)
	c.pendingMu.Lock()
	c.pending[seq] = ch
	c.pendingMu.Unlock()

	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, seq)
		c.pendingMu.Unlock()
	}()

	// Send
	if _, err := c.conn.Write(packet); err != nil {
		return RpcResponse{}, fmt.Errorf("send: %w", err)
	}

	// Wait for response
	select {
	case resp, ok := <-ch:
		if !ok {
			return RpcResponse{}, fmt.Errorf("connection closed")
		}
		return resp, nil
	case <-ctx.Done():
		return RpcResponse{}, ctx.Err()
	case <-time.After(c.config.Timeout):
		return RpcResponse{}, &VctpError{Status: 504, Message: "request timeout"}
	}
}

func (c *Client) receiveLoop() {
	buf := make([]byte, 65536)
	for !c.closed.Load() {
		n, err := c.conn.Read(buf)
		if err != nil {
			if c.closed.Load() {
				return
			}
			continue
		}
		if n < VCTPHeaderSize+4 {
			continue
		}

		resp, err := parseResponse(buf[:n])
		if err != nil {
			continue
		}

		c.pendingMu.Lock()
		ch, ok := c.pending[resp.Sequence]
		c.pendingMu.Unlock()

		if ok {
			select {
			case ch <- resp:
			default:
			}
		}
	}
}

func buildPacket(sequence uint64, methodID uint64, payload []byte) []byte {
	header := make([]byte, VCTPHeaderSize)
	// magic (4)
	header[0] = byte(VCTPMagic)
	header[1] = byte(VCTPMagic >> 8)
	header[2] = byte(VCTPMagic >> 16)
	header[3] = byte(VCTPMagic >> 24)
	// sequence (8)
	for i := 0; i < 8; i++ {
		header[4+i] = byte(sequence >> (8 * i))
	}
	// method/workflow_id (8)
	for i := 0; i < 8; i++ {
		header[12+i] = byte(methodID >> (8 * i))
	}
	// slab_offset (4) — all zeros
	// payload_length (4)
	pl := uint32(len(payload))
	header[24] = byte(pl)
	header[25] = byte(pl >> 8)
	header[26] = byte(pl >> 16)
	header[27] = byte(pl >> 24)

	withoutCRC := append(header, payload...)
	checksum := crc32.ChecksumIEEE(withoutCRC)
	crcBytes := []byte{
		byte(checksum),
		byte(checksum >> 8),
		byte(checksum >> 16),
		byte(checksum >> 24),
	}
	return append(withoutCRC, crcBytes...)
}

func parseResponse(data []byte) (RpcResponse, error) {
	if len(data) < VCTPHeaderSize+4 {
		return RpcResponse{}, fmt.Errorf("response too small")
	}

	magic := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
	if magic != VCTPMagic {
		return RpcResponse{}, fmt.Errorf("invalid magic: 0x%08X", magic)
	}

	seq := uint64(0)
	for i := 0; i < 8; i++ {
		seq |= uint64(data[4+i]) << (8 * i)
	}

	payloadLen := uint32(data[24]) | uint32(data[25])<<8 | uint32(data[26])<<16 | uint32(data[27])<<24

	if len(data) < int(VCTPHeaderSize+payloadLen+4) {
		return RpcResponse{}, fmt.Errorf("response truncated")
	}

	// Verify CRC32
	packetData := data[:VCTPHeaderSize+payloadLen]
	expectedCRC := crc32.ChecksumIEEE(packetData)
	off := int(VCTPHeaderSize + payloadLen)
	actualCRC := uint32(data[off]) | uint32(data[off+1])<<8 | uint32(data[off+2])<<16 | uint32(data[off+3])<<24
	if expectedCRC != actualCRC {
		return RpcResponse{}, fmt.Errorf("CRC32 mismatch")
	}

	payload := data[VCTPHeaderSize : VCTPHeaderSize+payloadLen]
	var resp RpcResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		return RpcResponse{}, fmt.Errorf("unmarshal response: %w", err)
	}
	resp.Sequence = seq
	return resp, nil
}
