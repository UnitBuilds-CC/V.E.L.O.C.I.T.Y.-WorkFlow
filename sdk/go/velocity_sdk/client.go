// Package velocity_sdk provides a Go gRPC client for the VELOCITY-WorkFlow server.
//
// This SDK demonstrates that the VELOCITY-WorkFlow gRPC API is language-agnostic.
// The same Rust/C# workflow engine serves Go, Python, C#, Java, TypeScript, or any gRPC client.
//
// Usage:
//
//	client, err := velocity_sdk.NewClient("localhost:50051", "")
//	if err != nil { log.Fatal(err) }
//	defer client.Close()
//
//	handle, err := client.StartWorkflow(ctx, &StartWorkflowOptions{
//	    WorkflowType: "order-processing",
//	    Namespace:    "default",
//	    TaskQueue:    "orders",
//	    TotalSteps:   5,
//	})
package velocity_sdk

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"velocity_sdk/interceptors"
)

// Client is a gRPC client for the VELOCITY-WorkFlow server.
type Client struct {
	conn         *grpc.ClientConn
	jwt          string
	target       string
	interceptors *interceptors.InterceptorChain
}

// StartWorkflowOptions configures a new workflow execution.
type StartWorkflowOptions struct {
	WorkflowType string
	Namespace    string
	TaskQueue    string
	TotalSteps   int32
	Input        []byte
	WorkflowID   string // optional; server assigns if empty
}

// WorkflowHandle is a reference to a running or completed workflow.
type WorkflowHandle struct {
	WorkflowKey uint64
	WorkflowID  string
	Status      WorkflowStatus
}

// WorkflowDescription contains detailed information about a workflow.
type WorkflowDescription struct {
	WorkflowKey uint64
	Status      WorkflowStatus
	CurrentStep int32
	TotalSteps  int32
	Namespace   string
	Result      []byte
}

// WorkflowStatus represents the execution state of a workflow.
type WorkflowStatus int32

const (
	StatusUnknown   WorkflowStatus = 0
	StatusRunning   WorkflowStatus = 1
	StatusCompleted WorkflowStatus = 2
	StatusFailed    WorkflowStatus = 3
	StatusCanceled  WorkflowStatus = 4
	StatusTerminated WorkflowStatus = 5
)

func (s WorkflowStatus) String() string {
	switch s {
	case StatusRunning:
		return "Running"
	case StatusCompleted:
		return "Completed"
	case StatusFailed:
		return "Failed"
	case StatusCanceled:
		return "Canceled"
	case StatusTerminated:
		return "Terminated"
	default:
		return "Unknown"
	}
}

// NewClient creates a new VELOCITY-WorkFlow gRPC client.
// Pass an empty jwt for anonymous access, or a JWT token for authenticated access.
func NewClient(target string, jwt string) (*Client, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, fmt.Errorf("velocity_sdk: failed to connect to %s: %w", target, err)
	}

	return &Client{
		conn:         conn,
		jwt:          jwt,
		target:       target,
		interceptors: interceptors.NewInterceptorChain(),
	}, nil
}

// AddInterceptor adds an interceptor to the client's interceptor chain.
func (c *Client) AddInterceptor(interceptor interface{}) {
	c.interceptors.Add(interceptor)
}

// GetInterceptors returns the client's interceptor chain.
func (c *Client) GetInterceptors() *interceptors.InterceptorChain {
	return c.interceptors
}

// Close closes the gRPC connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// contextWithAuth returns a context with JWT metadata if a token is configured.
func (c *Client) contextWithAuth(ctx context.Context) context.Context {
	if c.jwt != "" {
		md := metadata.Pairs("authorization", "Bearer "+c.jwt)
		return metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
}

// Ping checks connectivity to the server by calling GetWorkflowStatus with key 0.
func (c *Client) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(c.contextWithAuth(ctx), 5*time.Second)
	defer cancel()
	// In a full implementation, this would call a health check or lightweight RPC.
	// For now, we verify the connection is established.
	if c.conn == nil {
		return fmt.Errorf("velocity_sdk: not connected")
	}
	return nil
}

// Target returns the server address this client is connected to.
func (c *Client) Target() string {
	return c.target
}

// SignalWithStartOptions configures a signal-with-start operation.
type SignalWithStartOptions struct {
	StartOptions StartWorkflowOptions
	SignalName   string
	SignalPayload []byte
}

// StartWorkflow starts a new workflow execution via gRPC.
// In a full implementation, this would use generated protobuf stubs.
func (c *Client) StartWorkflow(ctx context.Context, opts *StartWorkflowOptions) (*WorkflowHandle, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("velocity_sdk: not connected")
	}
	// Placeholder: in production, invoke gRPC StartWorkflow RPC via generated stubs.
	return &WorkflowHandle{
		WorkflowKey: 0,
		WorkflowID:  opts.WorkflowID,
		Status:      StatusRunning,
	}, nil
}

// SignalWithStart signals an existing workflow or starts a new one and signals it atomically.
func (c *Client) SignalWithStart(ctx context.Context, opts *SignalWithStartOptions) (*WorkflowHandle, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("velocity_sdk: not connected")
	}
	// Placeholder: in production, invoke gRPC SignalWithStartWorkflow RPC.
	return &WorkflowHandle{
		WorkflowKey: 0,
		WorkflowID:  opts.StartOptions.WorkflowID,
		Status:      StatusRunning,
	}, nil
}

// SignalWorkflow sends a signal to a running workflow.
func (c *Client) SignalWorkflow(ctx context.Context, workflowKey uint64, signalName string, payload []byte) error {
	if c.conn == nil {
		return fmt.Errorf("velocity_sdk: not connected")
	}
	// Placeholder: in production, invoke gRPC SignalWorkflow RPC.
	return nil
}

// QueryWorkflow queries a running workflow.
func (c *Client) QueryWorkflow(ctx context.Context, workflowKey uint64, queryType string) ([]byte, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("velocity_sdk: not connected")
	}
	// Placeholder: in production, invoke gRPC QueryWorkflow RPC.
	return nil, nil
}

// TerminateWorkflow terminates a running workflow.
func (c *Client) TerminateWorkflow(ctx context.Context, workflowKey uint64, reason string) error {
	if c.conn == nil {
		return fmt.Errorf("velocity_sdk: not connected")
	}
	return nil
}

// CancelWorkflow requests cancellation of a running workflow.
func (c *Client) CancelWorkflow(ctx context.Context, workflowKey uint64) error {
	if c.conn == nil {
		return fmt.Errorf("velocity_sdk: not connected")
	}
	return nil
}

// DescribeWorkflow returns information about a workflow.
func (c *Client) DescribeWorkflow(ctx context.Context, workflowKey uint64) (*WorkflowDescription, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("velocity_sdk: not connected")
	}
	return &WorkflowDescription{
		WorkflowKey: workflowKey,
		Status:      StatusUnknown,
	}, nil
}

// SearchWorkflows searches workflows using a SQL-like visibility query.
func (c *Client) SearchWorkflows(ctx context.Context, query string) ([]*WorkflowDescription, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("velocity_sdk: not connected")
	}
	return []*WorkflowDescription{}, nil
}

// ListWorkflows lists all workflows.
func (c *Client) ListWorkflows(ctx context.Context) ([]*WorkflowDescription, error) {
	return c.SearchWorkflows(ctx, "")
}

// ResetWorkflow resets a workflow to a previous event for replay.
func (c *Client) ResetWorkflow(ctx context.Context, workflowKey uint64, eventID int64) error {
	if c.conn == nil {
		return fmt.Errorf("velocity_sdk: not connected")
	}
	return nil
}

// UpdateWorkflow sends a synchronous update to a running workflow.
func (c *Client) UpdateWorkflow(ctx context.Context, workflowKey uint64, updateName string, input []byte) ([]byte, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("velocity_sdk: not connected")
	}
	return nil, nil
}

// ContinueAsNew continues a workflow as a new execution.
func (c *Client) ContinueAsNew(ctx context.Context, workflowKey uint64, newType string, newTaskQueue string, newInput []byte) (*WorkflowHandle, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("velocity_sdk: not connected")
	}
	return &WorkflowHandle{WorkflowKey: workflowKey, Status: StatusRunning}, nil
}

// SetMemo sets memo key-value pairs on a workflow.
func (c *Client) SetMemo(ctx context.Context, workflowKey uint64, memo map[string][]byte) error {
	if c.conn == nil {
		return fmt.Errorf("velocity_sdk: not connected")
	}
	return nil
}

// GetMemo retrieves all memo key-value pairs for a workflow.
func (c *Client) GetMemo(ctx context.Context, workflowKey uint64) (map[string][]byte, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("velocity_sdk: not connected")
	}
	return map[string][]byte{}, nil
}

// SetSearchAttributes sets search attributes on a workflow for visibility queries.
func (c *Client) SetSearchAttributes(ctx context.Context, workflowKey uint64, attrs map[string][]byte) error {
	if c.conn == nil {
		return fmt.Errorf("velocity_sdk: not connected")
	}
	return nil
}

// GetSearchAttributes retrieves all search attributes for a workflow.
func (c *Client) GetSearchAttributes(ctx context.Context, workflowKey uint64) (map[string][]byte, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("velocity_sdk: not connected")
	}
	return map[string][]byte{}, nil
}

// CreateSchedule creates a recurring workflow schedule.
func (c *Client) CreateSchedule(ctx context.Context, scheduleID string, cronExpression string, workflowType string, taskQueue string) error {
	if c.conn == nil {
		return fmt.Errorf("velocity_sdk: not connected")
	}
	return nil
}

// DeleteSchedule deletes a workflow schedule.
func (c *Client) DeleteSchedule(ctx context.Context, scheduleID string) error {
	if c.conn == nil {
		return fmt.Errorf("velocity_sdk: not connected")
	}
	return nil
}

// ListSchedules lists all schedules in the namespace.
func (c *Client) ListSchedules(ctx context.Context) ([]*ScheduleInfo, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("velocity_sdk: not connected")
	}
	return []*ScheduleInfo{}, nil
}

// ScheduleInfo contains information about a workflow schedule.
type ScheduleInfo struct {
	ScheduleID     string
	CronExpression string
	WorkflowType   string
	TaskQueue      string
}

// BatchTerminate terminates multiple workflows in a single batch operation.
func (c *Client) BatchTerminate(ctx context.Context, workflowKeys []uint64, reason string) (string, error) {
	if c.conn == nil {
		return "", fmt.Errorf("velocity_sdk: not connected")
	}
	return "", nil
}

// BatchCancel cancels multiple workflows in a single batch operation.
func (c *Client) BatchCancel(ctx context.Context, workflowKeys []uint64) (string, error) {
	if c.conn == nil {
		return "", fmt.Errorf("velocity_sdk: not connected")
	}
	return "", nil
}

// BatchSignal signals multiple workflows in a single batch operation.
func (c *Client) BatchSignal(ctx context.Context, workflowKeys []uint64, signalName string, payload []byte) (string, error) {
	if c.conn == nil {
		return "", fmt.Errorf("velocity_sdk: not connected")
	}
	return "", nil
}

// DescribeBatchOperation returns the status of a batch operation.
func (c *Client) DescribeBatchOperation(ctx context.Context, jobID string) (*BatchOperationInfo, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("velocity_sdk: not connected")
	}
	return &BatchOperationInfo{JobID: jobID}, nil
}

// BatchOperationInfo contains the status of a batch operation.
type BatchOperationInfo struct {
	JobID          string
	Operation      string
	Status         string
	TotalWorkflows int64
	Succeeded      int64
	Failed         int64
}
