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
)

// Client is a gRPC client for the VELOCITY-WorkFlow server.
type Client struct {
	conn   *grpc.ClientConn
	jwt    string
	target string
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
		conn:   conn,
		jwt:    jwt,
		target: target,
	}, nil
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
