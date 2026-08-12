// Package stub provides a typed workflow execution stub.
//
// Usage:
//
//	s := stub.New(client, "order-processing").
//	    WithNamespace("default").
//	    WithTaskQueue("orders")
//
//	handle, err := s.Start(ctx, map[string]string{"orderId": "12345"})
//	err = s.Signal(ctx, "approve", map[string]bool{"approved": true})
//	result, err := s.Result(ctx)
package stub

import (
	"context"
	"fmt"

	"velocity_sdk/codec"
	"velocity_sdk/velocity_sdk"
)

// WorkflowStub provides a high-level interface for workflow execution.
type WorkflowStub struct {
	client       *velocity_sdk.Client
	workflowType string
	namespace    string
	taskQueue    string
	codec        codec.PayloadCodec
	handle       *velocity_sdk.WorkflowHandle
}

// New creates a new WorkflowStub.
func New(client *velocity_sdk.Client, workflowType string) *WorkflowStub {
	return &WorkflowStub{
		client:       client,
		workflowType: workflowType,
		namespace:    "default",
		taskQueue:    "default",
		codec:        codec.NewJSONCodec(),
	}
}

// WithNamespace sets the namespace. Returns the stub for chaining.
func (s *WorkflowStub) WithNamespace(namespace string) *WorkflowStub {
	s.namespace = namespace
	return s
}

// WithTaskQueue sets the task queue. Returns the stub for chaining.
func (s *WorkflowStub) WithTaskQueue(taskQueue string) *WorkflowStub {
	s.taskQueue = taskQueue
	return s
}

// WithCodec sets the payload codec. Returns the stub for chaining.
func (s *WorkflowStub) WithCodec(c codec.PayloadCodec) *WorkflowStub {
	s.codec = c
	return s
}

// Start begins workflow execution.
func (s *WorkflowStub) Start(ctx context.Context, input interface{}) (*velocity_sdk.WorkflowHandle, error) {
	var payload []byte
	if input != nil {
		var err error
		payload, err = s.codec.Encode(input)
		if err != nil {
			return nil, fmt.Errorf("stub: failed to encode input: %w", err)
		}
	}

	handle, err := s.client.StartWorkflow(ctx, &velocity_sdk.StartWorkflowOptions{
		WorkflowType: s.workflowType,
		Namespace:    s.namespace,
		TaskQueue:    s.taskQueue,
		Input:        payload,
	})
	if err != nil {
		return nil, err
	}

	s.handle = handle
	return handle, nil
}

// Signal sends a signal to the workflow.
func (s *WorkflowStub) Signal(ctx context.Context, signalName string, data interface{}) error {
	if s.handle == nil {
		return fmt.Errorf("stub: workflow not started, call Start() first")
	}

	var payload []byte
	if data != nil {
		var err error
		payload, err = s.codec.Encode(data)
		if err != nil {
			return fmt.Errorf("stub: failed to encode signal data: %w", err)
		}
	}

	return s.client.SignalWorkflow(ctx, s.handle.WorkflowKey, signalName, payload)
}

// Query queries the workflow state.
func (s *WorkflowStub) Query(ctx context.Context, queryType string, args interface{}) (interface{}, error) {
	if s.handle == nil {
		return nil, fmt.Errorf("stub: workflow not started, call Start() first")
	}

	var payload []byte
	if args != nil {
		var err error
		payload, err = s.codec.Encode(args)
		if err != nil {
			return nil, fmt.Errorf("stub: failed to encode query args: %w", err)
		}
	}

	result, err := s.client.QueryWorkflow(ctx, s.handle.WorkflowKey, queryType, payload)
	if err != nil {
		return nil, err
	}

	if result == nil || len(result) == 0 {
		return nil, nil
	}

	return s.codec.Decode(result)
}

// Result waits for workflow completion and returns the decoded result.
func (s *WorkflowStub) Result(ctx context.Context) (interface{}, error) {
	if s.handle == nil {
		return nil, fmt.Errorf("stub: workflow not started, call Start() first")
	}

	desc, err := s.client.WaitForCompletion(ctx, s.handle.WorkflowKey)
	if err != nil {
		return nil, err
	}

	if desc == nil || desc.Result == nil || len(desc.Result) == 0 {
		return nil, nil
	}

	return s.codec.Decode(desc.Result)
}

// Cancel cancels the workflow.
func (s *WorkflowStub) Cancel(ctx context.Context) error {
	if s.handle == nil {
		return fmt.Errorf("stub: workflow not started, call Start() first")
	}
	return s.client.CancelWorkflow(ctx, s.handle.WorkflowKey)
}

// Terminate terminates the workflow.
func (s *WorkflowStub) Terminate(ctx context.Context, reason string) error {
	if s.handle == nil {
		return fmt.Errorf("stub: workflow not started, call Start() first")
	}
	return s.client.TerminateWorkflow(ctx, s.handle.WorkflowKey, reason)
}

// Handle returns the underlying workflow handle.
func (s *WorkflowStub) Handle() *velocity_sdk.WorkflowHandle {
	return s.handle
}

// WorkflowKey returns the workflow key (0 if not started).
func (s *WorkflowStub) WorkflowKey() uint64 {
	if s.handle == nil {
		return 0
	}
	return s.handle.WorkflowKey
}
