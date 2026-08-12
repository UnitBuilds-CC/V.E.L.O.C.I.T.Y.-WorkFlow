package velocity

import (
	"context"
	"fmt"
)

// ─── Workflow Update ────────────────────────────────────────────────────────────

// UpdateOptions contains options for updating a workflow.
type UpdateOptions struct {
	UpdateName string
	Args       interface{}
	WaitPolicy string // "ACCEPTED", "COMPLETED"
}

// UpdateResult represents the result of a workflow update.
type UpdateResult struct {
	UpdateID string
	Status   string // "ACCEPTED", "COMPLETED", "REJECTED"
	Result   interface{}
}

// Update sends an update to a running workflow.
func (c *Client) Update(ctx context.Context, workflowID string, options UpdateOptions) (*UpdateResult, error) {
	return c.conn.UpdateWorkflow(ctx, &UpdateWorkflowRequest{
		Namespace:  c.namespace,
		WorkflowID: workflowID,
		UpdateName: options.UpdateName,
		Args:       options.Args,
		WaitPolicy: options.WaitPolicy,
	})
}

// Update sends an update to the workflow via handle.
func (h *WorkflowHandle) Update(ctx context.Context, updateName string, args ...interface{}) (*UpdateResult, error) {
	return h.client.Update(ctx, h.workflowID, UpdateOptions{
		UpdateName: updateName,
		Args:       args,
	})
}

// ─── Workflow Reset ─────────────────────────────────────────────────────────────

// ResetOptions contains options for resetting a workflow.
type ResetOptions struct {
	ResetEventID int64
	Reason       string
}

// Reset resets a workflow to a specific event ID.
func (c *Client) Reset(ctx context.Context, workflowID string, options ResetOptions) (string, error) {
	return c.conn.ResetWorkflow(ctx, &ResetWorkflowRequest{
		Namespace:  c.namespace,
		WorkflowID: workflowID,
		ResetEventID: options.ResetEventID,
		Reason:     options.Reason,
	})
}

// Reset resets the workflow via handle.
func (h *WorkflowHandle) Reset(ctx context.Context, resetEventID int64, reason string) (string, error) {
	return h.client.Reset(ctx, h.workflowID, ResetOptions{
		ResetEventID: resetEventID,
		Reason:       reason,
	})
}

// ─── Schedule Client ────────────────────────────────────────────────────────────

// ScheduleOptions contains options for creating a schedule.
type ScheduleOptions struct {
	ScheduleID   string
	WorkflowType string
	TaskQueue    string
	CronSchedule string
	Input        interface{}
	Enabled      bool
}

// ScheduleClient provides schedule management operations.
type ScheduleClient struct {
	client *Client
}

// NewScheduleClient creates a new ScheduleClient from an existing Client.
func NewScheduleClient(client *Client) *ScheduleClient {
	return &ScheduleClient{client: client}
}

// Create creates a new schedule.
func (sc *ScheduleClient) Create(ctx context.Context, options ScheduleOptions) (string, error) {
	return sc.client.conn.CreateSchedule(ctx, &CreateScheduleRequest{
		Namespace:    sc.client.namespace,
		ScheduleID:   options.ScheduleID,
		WorkflowType: options.WorkflowType,
		TaskQueue:    options.TaskQueue,
		CronSchedule: options.CronSchedule,
		Input:        options.Input,
		Enabled:      options.Enabled,
	})
}

// Describe returns schedule details.
func (sc *ScheduleClient) Describe(ctx context.Context, scheduleID string) (*Schedule, error) {
	return sc.client.conn.DescribeSchedule(ctx, &DescribeScheduleRequest{
		Namespace:  sc.client.namespace,
		ScheduleID: scheduleID,
	})
}

// List lists all schedules.
func (sc *ScheduleClient) List(ctx context.Context) ([]*Schedule, error) {
	return sc.client.conn.ListSchedules(ctx, &ListSchedulesRequest{
		Namespace: sc.client.namespace,
	})
}

// Update updates a schedule.
func (sc *ScheduleClient) Update(ctx context.Context, scheduleID string, options ScheduleOptions) error {
	return sc.client.conn.UpdateSchedule(ctx, &UpdateScheduleRequest{
		Namespace:    sc.client.namespace,
		ScheduleID:   scheduleID,
		WorkflowType: options.WorkflowType,
		CronSchedule: options.CronSchedule,
		Enabled:      options.Enabled,
	})
}

// Delete deletes a schedule.
func (sc *ScheduleClient) Delete(ctx context.Context, scheduleID string) error {
	return sc.client.conn.DeleteSchedule(ctx, &DeleteScheduleRequest{
		Namespace:  sc.client.namespace,
		ScheduleID: scheduleID,
	})
}

// Pause pauses a schedule.
func (sc *ScheduleClient) Pause(ctx context.Context, scheduleID string) error {
	return sc.client.conn.UpdateSchedule(ctx, &UpdateScheduleRequest{
		Namespace:  sc.client.namespace,
		ScheduleID: scheduleID,
		Enabled:    false,
	})
}

// Unpause resumes a schedule.
func (sc *ScheduleClient) Unpause(ctx context.Context, scheduleID string) error {
	return sc.client.conn.UpdateSchedule(ctx, &UpdateScheduleRequest{
		Namespace:  sc.client.namespace,
		ScheduleID: scheduleID,
		Enabled:    true,
	})
}

// ─── Search Attributes ──────────────────────────────────────────────────────────

// SearchAttributesClient provides search attribute operations.
type SearchAttributesClient struct {
	client *Client
}

// NewSearchAttributesClient creates a new SearchAttributesClient.
func NewSearchAttributesClient(client *Client) *SearchAttributesClient {
	return &SearchAttributesClient{client: client}
}

// UpsertSearchAttributes upserts search attributes for a workflow execution.
func (sac *SearchAttributesClient) Upsert(ctx context.Context, workflowID string, attributes map[string]interface{}) error {
	return sac.client.conn.UpsertSearchAttributes(ctx, &UpsertSearchAttributesRequest{
		Namespace:  sac.client.namespace,
		WorkflowID: workflowID,
		Attributes: attributes,
	})
}

// ListWorkflowExecutions lists workflows matching a search query.
func (sac *SearchAttributesClient) ListWorkflows(ctx context.Context, query string) ([]*WorkflowExecution, error) {
	return sac.client.conn.ListWorkflowExecutions(ctx, &ListWorkflowExecutionsRequest{
		Namespace: sac.client.namespace,
		Query:     query,
	})
}

// CountWorkflowExecutions counts workflows matching a search query.
func (sac *SearchAttributesClient) CountWorkflows(ctx context.Context, query string) (int64, error) {
	return sac.client.conn.CountWorkflowExecutions(ctx, &CountWorkflowExecutionsRequest{
		Namespace: sac.client.namespace,
		Query:     query,
	})
}

// ─── Continue-as-New ────────────────────────────────────────────────────────────

// ContinueAsNewOptions contains options for continuing a workflow as new.
type ContinueAsNewOptions struct {
	WorkflowType string
	TaskQueue    string
	Input        interface{}
	RunTimeout   int64 // milliseconds
	TaskTimeout  int64 // milliseconds
	RetryPolicy  *RetryPolicy
	Memo         map[string]interface{}
}

// ContinueAsNewError is a special error used to signal the worker to continue-as-new.
type ContinueAsNewError struct {
	Options ContinueAsNewOptions
}

func (e *ContinueAsNewError) Error() string {
	return fmt.Sprintf("continue-as-new: %s", e.Options.WorkflowType)
}

// NewContinueAsNewError creates a ContinueAsNewError that instructs the worker
// to continue the workflow as a new execution. This is the Temporal pattern for
// long-running workflows that need to reset their history.
func NewContinueAsNewError(options ContinueAsNewOptions) *ContinueAsNewError {
	return &ContinueAsNewError{Options: options}
}

// ─── Batch Operations ───────────────────────────────────────────────────────────

// BatchOperationOptions contains options for starting a batch operation.
type BatchOperationOptions struct {
	Operation   string // "terminate", "cancel", "signal", "delete"
	Query       string // Visibility query to select workflows
	SignalName  string // Required if Operation is "signal"
	SignalInput interface{}
	Reason      string
}

// BatchOperationClient provides batch operation management.
type BatchOperationClient struct {
	client *Client
}

// NewBatchOperationClient creates a new BatchOperationClient.
func NewBatchOperationClient(client *Client) *BatchOperationClient {
	return &BatchOperationClient{client: client}
}

// Start starts a batch operation.
func (bc *BatchOperationClient) Start(ctx context.Context, options BatchOperationOptions) (string, error) {
	return bc.client.conn.StartBatchOperation(ctx, &StartBatchOperationRequest{
		Namespace:  bc.client.namespace,
		Operation:  options.Operation,
		Query:      options.Query,
		SignalName: options.SignalName,
		Reason:     options.Reason,
	})
}

// Describe returns batch operation details.
func (bc *BatchOperationClient) Describe(ctx context.Context, jobID string) (*BatchOperation, error) {
	return bc.client.conn.DescribeBatchOperation(ctx, &DescribeBatchOperationRequest{
		Namespace: bc.client.namespace,
		JobID:     jobID,
	})
}

// List lists batch operations.
func (bc *BatchOperationClient) List(ctx context.Context) ([]*BatchOperation, error) {
	return bc.client.conn.ListBatchOperations(ctx, &ListBatchOperationsRequest{
		Namespace: bc.client.namespace,
	})
}

// ─── Saga Orchestration ─────────────────────────────────────────────────────────

// SagaStep represents a single step in a saga with an execution function and
// a compensating function for rollback.
type SagaStep struct {
	Execute     func(ctx context.Context) (interface{}, error)
	Compensate  func(ctx context.Context) error
	Name        string
}

// Saga provides saga orchestration for multi-step workflows with compensating
// transactions. If any step fails, previously completed steps are rolled back
// in reverse order (Temporal Saga pattern).
type Saga struct {
	steps       []SagaStep
	completed   []SagaStep
	results     []interface{}
}

// NewSaga creates a new Saga orchestrator.
func NewSaga() *Saga {
	return &Saga{}
}

// AddStep adds a step to the saga.
func (s *Saga) AddStep(name string, execute func(ctx context.Context) (interface{}, error), compensate func(ctx context.Context) error) {
	s.steps = append(s.steps, SagaStep{
		Execute:    execute,
		Compensate: compensate,
		Name:       name,
	})
}

// Execute runs all saga steps in order. If any step fails, previously completed
// steps are compensated in reverse order. Returns the results of successful steps
// and the error from the failing step (if any).
func (s *Saga) Execute(ctx context.Context) ([]interface{}, error) {
	for _, step := range s.steps {
		result, err := step.Execute(ctx)
		if err != nil {
			// Compensate in reverse order
			s.compensate(ctx)
			return s.results, fmt.Errorf("saga step %q failed: %w", step.Name, err)
		}
		s.completed = append(s.completed, step)
		s.results = append(s.results, result)
	}
	return s.results, nil
}

// compensate runs compensating transactions in reverse order.
func (s *Saga) compensate(ctx context.Context) {
	for i := len(s.completed) - 1; i >= 0; i-- {
		_ = s.completed[i].Compensate(ctx)
	}
}

// ─── Connection Request/Response Types ──────────────────────────────────────────

type UpdateWorkflowRequest struct {
	Namespace  string
	WorkflowID string
	UpdateName string
	Args       interface{}
	WaitPolicy string
}

type ResetWorkflowRequest struct {
	Namespace    string
	WorkflowID   string
	ResetEventID int64
	Reason       string
}

type CreateScheduleRequest struct {
	Namespace    string
	ScheduleID   string
	WorkflowType string
	TaskQueue    string
	CronSchedule string
	Input        interface{}
	Enabled      bool
}

type DescribeScheduleRequest struct {
	Namespace  string
	ScheduleID string
}

type ListSchedulesRequest struct {
	Namespace string
}

type UpdateScheduleRequest struct {
	Namespace    string
	ScheduleID   string
	WorkflowType string
	CronSchedule string
	Enabled      bool
}

type DeleteScheduleRequest struct {
	Namespace  string
	ScheduleID string
}

type UpsertSearchAttributesRequest struct {
	Namespace  string
	WorkflowID string
	Attributes map[string]interface{}
}

type ListWorkflowExecutionsRequest struct {
	Namespace string
	Query     string
}

type CountWorkflowExecutionsRequest struct {
	Namespace string
	Query     string
}

type StartBatchOperationRequest struct {
	Namespace  string
	Operation  string
	Query      string
	SignalName string
	Reason     string
}

type DescribeBatchOperationRequest struct {
	Namespace string
	JobID     string
}

type ListBatchOperationsRequest struct {
	Namespace string
}

// ─── Connection Method Implementations (HTTP-based) ────────────────────────────

func (c *Connection) UpdateWorkflow(ctx context.Context, req *UpdateWorkflowRequest) (*UpdateResult, error) {
	var result interface{}
	err := c.doJSON(ctx, "POST", fmt.Sprintf("/api/workflows/%s/update", req.WorkflowID), map[string]interface{}{
		"updateName": req.UpdateName,
		"args":       req.Args,
		"waitPolicy": req.WaitPolicy,
	}, &result)
	if err != nil {
		return nil, err
	}
	return &UpdateResult{
		UpdateID: fmt.Sprintf("update-%s", req.WorkflowID),
		Status:   "ACCEPTED",
		Result:   result,
	}, nil
}

func (c *Connection) ResetWorkflow(ctx context.Context, req *ResetWorkflowRequest) (string, error) {
	var result map[string]interface{}
	err := c.doJSON(ctx, "POST", fmt.Sprintf("/api/workflows/%s/reset", req.WorkflowID), map[string]interface{}{
		"resetEventId": req.ResetEventID,
		"reason":       req.Reason,
	}, &result)
	if err != nil {
		return "", err
	}
	if newRunID, ok := result["runId"].(string); ok {
		return newRunID, nil
	}
	return fmt.Sprintf("run-reset-%s", req.WorkflowID), nil
}

func (c *Connection) CreateSchedule(ctx context.Context, req *CreateScheduleRequest) (string, error) {
	var result map[string]interface{}
	err := c.doJSON(ctx, "POST", "/api/schedules", req, &result)
	if err != nil {
		return "", err
	}
	if id, ok := result["scheduleId"].(string); ok {
		return id, nil
	}
	return req.ScheduleID, nil
}

func (c *Connection) DescribeSchedule(ctx context.Context, req *DescribeScheduleRequest) (*Schedule, error) {
	var sched Schedule
	err := c.doJSON(ctx, "GET", fmt.Sprintf("/api/schedules/%s", req.ScheduleID), nil, &sched)
	if err != nil {
		return nil, err
	}
	return &sched, nil
}

func (c *Connection) ListSchedules(ctx context.Context, req *ListSchedulesRequest) ([]*Schedule, error) {
	var schedules []*Schedule
	err := c.doJSON(ctx, "GET", "/api/schedules", nil, &schedules)
	if err != nil {
		return []*Schedule{}, nil
	}
	return schedules, nil
}

func (c *Connection) UpdateSchedule(ctx context.Context, req *UpdateScheduleRequest) error {
	return c.doJSON(ctx, "PUT", fmt.Sprintf("/api/schedules/%s", req.ScheduleID), req, nil)
}

func (c *Connection) DeleteSchedule(ctx context.Context, req *DeleteScheduleRequest) error {
	return c.doJSON(ctx, "DELETE", fmt.Sprintf("/api/schedules/%s", req.ScheduleID), nil, nil)
}

func (c *Connection) UpsertSearchAttributes(ctx context.Context, req *UpsertSearchAttributesRequest) error {
	return c.doJSON(ctx, "POST", fmt.Sprintf("/api/workflows/%s/search-attributes", req.WorkflowID), map[string]interface{}{
		"attributes": req.Attributes,
	}, nil)
}

func (c *Connection) ListWorkflowExecutions(ctx context.Context, req *ListWorkflowExecutionsRequest) ([]*WorkflowExecution, error) {
	var result struct {
		Workflows []*WorkflowExecution `json:"workflows"`
	}
	err := c.doJSON(ctx, "GET", fmt.Sprintf("/api/workflows?query=%s", req.Query), nil, &result)
	if err != nil {
		return []*WorkflowExecution{}, nil
	}
	return result.Workflows, nil
}

func (c *Connection) CountWorkflowExecutions(ctx context.Context, req *CountWorkflowExecutionsRequest) (int64, error) {
	var result struct {
		Count int64 `json:"count"`
	}
	err := c.doJSON(ctx, "GET", fmt.Sprintf("/api/workflows/count?query=%s", req.Query), nil, &result)
	if err != nil {
		return 0, nil
	}
	return result.Count, nil
}

func (c *Connection) StartBatchOperation(ctx context.Context, req *StartBatchOperationRequest) (string, error) {
	var result struct {
		JobID string `json:"jobId"`
	}
	err := c.doJSON(ctx, "POST", "/api/batch", req, &result)
	if err != nil {
		return "", err
	}
	return result.JobID, nil
}

func (c *Connection) DescribeBatchOperation(ctx context.Context, req *DescribeBatchOperationRequest) (*BatchOperation, error) {
	var op BatchOperation
	err := c.doJSON(ctx, "GET", fmt.Sprintf("/api/batch/%s", req.JobID), nil, &op)
	if err != nil {
		return nil, err
	}
	return &op, nil
}

func (c *Connection) ListBatchOperations(ctx context.Context, req *ListBatchOperationsRequest) ([]*BatchOperation, error) {
	var ops []*BatchOperation
	err := c.doJSON(ctx, "GET", "/api/batch", nil, &ops)
	if err != nil {
		return []*BatchOperation{}, nil
	}
	return ops, nil
}
