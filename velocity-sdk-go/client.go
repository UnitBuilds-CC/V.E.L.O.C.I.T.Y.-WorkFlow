package velocity

import (
	"context"
	"fmt"
	"time"
)

// Client provides a high-level API for managing workflows.
type Client struct {
	conn      *Connection
	namespace string
}

// ClientOptions contains options for creating a Client.
type ClientOptions struct {
	HostPort  string
	Namespace string
	TLS       bool
}

// NewClient creates a new Velocity client.
func NewClient(options ClientOptions) (*Client, error) {
	if options.HostPort == "" {
		options.HostPort = "localhost:7233"
	}
	if options.Namespace == "" {
		options.Namespace = "default"
	}

	conn, err := NewConnection(options.HostPort, options.TLS)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	return &Client{
		conn:      conn,
		namespace: options.Namespace,
	}, nil
}

// Start starts a new workflow execution.
func (c *Client) Start(ctx context.Context, options WorkflowOptions) (*WorkflowExecution, error) {
	exec, err := c.conn.StartWorkflow(ctx, &StartWorkflowRequest{
		Namespace:      c.namespace,
		WorkflowID:     options.WorkflowID,
		WorkflowType:   options.WorkflowType,
		TaskQueue:      options.TaskQueue,
		Input:          options.Input,
		ExecutionTimeout: options.ExecutionTimeout,
		RunTimeout:     options.RunTimeout,
		TaskTimeout:    options.TaskTimeout,
		RetryPolicy:    options.RetryPolicy,
	})
	if err != nil {
		return nil, err
	}

	return &WorkflowExecution{
		WorkflowID: exec.WorkflowID,
		RunID:      exec.RunID,
	}, nil
}

// Execute starts a workflow and waits for its result.
func (c *Client) Execute(ctx context.Context, options WorkflowOptions) (interface{}, error) {
	exec, err := c.Start(ctx, options)
	if err != nil {
		return nil, err
	}

	// Poll for workflow completion
	for {
		desc, err := c.Describe(ctx, exec.WorkflowID)
		if err != nil {
			return nil, err
		}

		switch desc.Status {
		case WorkflowStatusCompleted:
			return desc.Result, nil
		case WorkflowStatusFailed:
			return nil, fmt.Errorf("workflow failed: %v", desc.Failure)
		case WorkflowStatusCancelled:
			return nil, fmt.Errorf("workflow cancelled")
		case WorkflowStatusTerminated:
			return nil, fmt.Errorf("workflow terminated")
		}

		// Wait before polling again
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}
}

// Signal sends a signal to a running workflow.
func (c *Client) Signal(ctx context.Context, workflowID string, options SignalOptions) error {
	return c.conn.SignalWorkflow(ctx, &SignalWorkflowRequest{
		Namespace:  c.namespace,
		WorkflowID: workflowID,
		SignalName: options.SignalName,
		Input:      options.Args,
	})
}

// Query queries a workflow.
func (c *Client) Query(ctx context.Context, workflowID string, options QueryOptions) (interface{}, error) {
	return c.conn.QueryWorkflow(ctx, &QueryWorkflowRequest{
		Namespace:  c.namespace,
		WorkflowID: workflowID,
		QueryType:  options.QueryType,
		Input:      options.Args,
	})
}

// Terminate terminates a running workflow.
func (c *Client) Terminate(ctx context.Context, workflowID string, reason string) error {
	return c.conn.TerminateWorkflow(ctx, &TerminateWorkflowRequest{
		Namespace:  c.namespace,
		WorkflowID: workflowID,
		Reason:     reason,
	})
}

// Cancel cancels a running workflow.
func (c *Client) Cancel(ctx context.Context, workflowID string) error {
	return c.conn.CancelWorkflow(ctx, &CancelWorkflowRequest{
		Namespace:  c.namespace,
		WorkflowID: workflowID,
	})
}

// WorkflowDescription contains information about a workflow execution.
type WorkflowDescription struct {
	WorkflowExecution WorkflowExecution
	WorkflowType      string
	Status            WorkflowStatus
	TaskQueue         string
	StartTime         int64
	CloseTime         int64
	Result            interface{}
	Failure           string
}

// Describe returns information about a workflow execution.
func (c *Client) Describe(ctx context.Context, workflowID string) (*WorkflowDescription, error) {
	return c.conn.DescribeWorkflow(ctx, &DescribeWorkflowRequest{
		Namespace:  c.namespace,
		WorkflowID: workflowID,
	})
}

// GetHistory returns the history of a workflow execution.
func (c *Client) GetHistory(ctx context.Context, workflowID string) ([]*HistoryEvent, error) {
	return c.conn.GetWorkflowHistory(ctx, &GetWorkflowHistoryRequest{
		Namespace:  c.namespace,
		WorkflowID: workflowID,
	})
}

// GetWorkflow returns a handle to an existing workflow.
func (c *Client) GetWorkflow(workflowID string) *WorkflowHandle {
	return &WorkflowHandle{
		client:     c,
		workflowID: workflowID,
	}
}

// SignalWithStart signals an existing workflow or starts a new one and signals it atomically.
func (c *Client) SignalWithStart(ctx context.Context, workflowType string, signalName string, signalArgs interface{}, options WorkflowOptions) (*WorkflowExecution, error) {
	resp, err := c.conn.SignalWithStartWorkflow(ctx, &SignalWithStartRequest{
		Namespace:    c.namespace,
		WorkflowType: workflowType,
		WorkflowID:   options.WorkflowID,
		TaskQueue:    options.TaskQueue,
		Input:        options.Input,
		SignalName:   signalName,
		SignalArgs:   signalArgs,
	})
	if err != nil {
		return nil, err
	}
	return &WorkflowExecution{
		WorkflowID: resp.WorkflowID,
		RunID:      resp.RunID,
	}, nil
}

// SearchWorkflows searches workflows using a SQL-like visibility query.
func (c *Client) SearchWorkflows(ctx context.Context, query string) ([]map[string]interface{}, error) {
	return c.conn.SearchWorkflows(ctx, &SearchWorkflowsRequest{
		Namespace: c.namespace,
		Query:     query,
	})
}

// ListWorkflows lists all workflows in the namespace.
func (c *Client) ListWorkflows(ctx context.Context) ([]map[string]interface{}, error) {
	return c.conn.ListWorkflows(ctx, c.namespace)
}

// ResetWorkflow resets a workflow to a previous event for replay.
func (c *Client) ResetWorkflow(ctx context.Context, workflowID string, eventID int64) error {
	return c.conn.ResetWorkflow(ctx, &ResetWorkflowRequest{
		Namespace:  c.namespace,
		WorkflowID: workflowID,
		EventID:    eventID,
	})
}

// UpdateWorkflow sends a synchronous update to a running workflow and returns the result.
func (c *Client) UpdateWorkflow(ctx context.Context, workflowID string, updateName string, input interface{}) (interface{}, error) {
	return c.conn.UpdateWorkflow(ctx, &UpdateWorkflowRequest{
		Namespace:  c.namespace,
		WorkflowID: workflowID,
		UpdateName: updateName,
		Input:      input,
	})
}

// ContinueAsNew continues a workflow as a new execution.
func (c *Client) ContinueAsNew(ctx context.Context, workflowID string, newType string, newTaskQueue string, newInput interface{}) (*WorkflowExecution, error) {
	resp, err := c.conn.ContinueAsNew(ctx, &ContinueAsNewRequest{
		Namespace:       c.namespace,
		WorkflowID:      workflowID,
		NewWorkflowType: newType,
		NewTaskQueue:    newTaskQueue,
		NewInput:        newInput,
	})
	if err != nil {
		return nil, err
	}
	return &WorkflowExecution{
		WorkflowID: resp.WorkflowID,
		RunID:      resp.RunID,
	}, nil
}

// SetMemo sets memo key-value pairs on a workflow.
func (c *Client) SetMemo(ctx context.Context, workflowID string, memo map[string]interface{}) error {
	return c.conn.SetMemo(ctx, &SetMemoRequest{
		Namespace:  c.namespace,
		WorkflowID: workflowID,
		Memo:       memo,
	})
}

// SetSearchAttributes sets search attributes on a workflow for visibility queries.
func (c *Client) SetSearchAttributes(ctx context.Context, workflowID string, attrs map[string]interface{}) error {
	return c.conn.SetSearchAttributes(ctx, &SetSearchAttributesRequest{
		Namespace:  c.namespace,
		WorkflowID: workflowID,
		Attributes: attrs,
	})
}

// CreateSchedule creates a recurring workflow schedule.
func (c *Client) CreateSchedule(ctx context.Context, scheduleID string, cronExpression string, workflowType string, options WorkflowOptions) error {
	return c.conn.CreateSchedule(ctx, &CreateScheduleRequest{
		Namespace:      c.namespace,
		ScheduleID:     scheduleID,
		CronExpression: cronExpression,
		WorkflowType:   workflowType,
		TaskQueue:      options.TaskQueue,
		Input:          options.Input,
	})
}

// DeleteSchedule deletes a workflow schedule.
func (c *Client) DeleteSchedule(ctx context.Context, scheduleID string) error {
	return c.conn.DeleteSchedule(ctx, c.namespace, scheduleID)
}

// ListSchedules lists all schedules in the namespace.
func (c *Client) ListSchedules(ctx context.Context) ([]map[string]interface{}, error) {
	return c.conn.ListSchedules(ctx, c.namespace)
}

// BatchTerminate terminates multiple workflows in a single batch operation.
func (c *Client) BatchTerminate(ctx context.Context, workflowIDs []string, reason string) (string, error) {
	return c.conn.StartBatchOperation(ctx, &BatchOperationRequest{
		Namespace:   c.namespace,
		Operation:   "terminate",
		WorkflowIDs: workflowIDs,
		Reason:      reason,
	})
}

// BatchCancel cancels multiple workflows in a single batch operation.
func (c *Client) BatchCancel(ctx context.Context, workflowIDs []string) (string, error) {
	return c.conn.StartBatchOperation(ctx, &BatchOperationRequest{
		Namespace:   c.namespace,
		Operation:   "cancel",
		WorkflowIDs: workflowIDs,
	})
}

// BatchSignal signals multiple workflows in a single batch operation.
func (c *Client) BatchSignal(ctx context.Context, workflowIDs []string, signalName string, args interface{}) (string, error) {
	return c.conn.StartBatchOperation(ctx, &BatchOperationRequest{
		Namespace:   c.namespace,
		Operation:   "signal",
		WorkflowIDs: workflowIDs,
		SignalName:  signalName,
		SignalArgs:  args,
	})
}

// DescribeBatchOperation returns the status of a batch operation.
func (c *Client) DescribeBatchOperation(ctx context.Context, jobID string) (map[string]interface{}, error) {
	return c.conn.DescribeBatchOperation(ctx, c.namespace, jobID)
}

// Close closes the client connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// WorkflowHandle provides a handle to an existing workflow execution.
type WorkflowHandle struct {
	client     *Client
	workflowID string
}

// GetWorkflowID returns the workflow ID.
func (h *WorkflowHandle) GetWorkflowID() string {
	return h.workflowID
}

// Signal sends a signal to the workflow.
func (h *WorkflowHandle) Signal(ctx context.Context, signalName string, args ...interface{}) error {
	return h.client.Signal(ctx, h.workflowID, SignalOptions{
		SignalName: signalName,
		Args:       args,
	})
}

// Query queries the workflow.
func (h *WorkflowHandle) Query(ctx context.Context, queryType string, args ...interface{}) (interface{}, error) {
	return h.client.Query(ctx, h.workflowID, QueryOptions{
		QueryType: queryType,
		Args:      args,
	})
}

// Terminate terminates the workflow.
func (h *WorkflowHandle) Terminate(ctx context.Context, reason string) error {
	return h.client.Terminate(ctx, h.workflowID, reason)
}

// Cancel cancels the workflow.
func (h *WorkflowHandle) Cancel(ctx context.Context) error {
	return h.client.Cancel(ctx, h.workflowID)
}

// Describe returns information about the workflow.
func (h *WorkflowHandle) Describe(ctx context.Context) (*WorkflowDescription, error) {
	return h.client.Describe(ctx, h.workflowID)
}

// GetHistory returns the workflow history.
func (h *WorkflowHandle) GetHistory(ctx context.Context) ([]*HistoryEvent, error) {
	return h.client.GetHistory(ctx, h.workflowID)
}

// Result waits for the workflow result.
func (h *WorkflowHandle) Result(ctx context.Context) (interface{}, error) {
	for {
		desc, err := h.Describe(ctx)
		if err != nil {
			return nil, err
		}

		switch desc.Status {
		case WorkflowStatusCompleted:
			return desc.Result, nil
		case WorkflowStatusFailed:
			return nil, fmt.Errorf("workflow failed: %v", desc.Failure)
		case WorkflowStatusCancelled:
			return nil, fmt.Errorf("workflow cancelled")
		case WorkflowStatusTerminated:
			return nil, fmt.Errorf("workflow terminated")
		}

		// Wait before polling again
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}
}
