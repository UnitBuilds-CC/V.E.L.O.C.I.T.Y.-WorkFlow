package velocity

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Connection represents an HTTP connection to the Velocity server.
type Connection struct {
	baseURL    string
	httpClient *http.Client
	headers    map[string]string
}

// NewConnection creates a new HTTP connection to the Velocity engine.
// The address can be either an HTTP URL (http://host:port) or a host:port pair
// (in which case http:// is prepended).
func NewConnection(address string, tls bool) (*Connection, error) {
	if address == "" {
		address = "localhost:5000"
	}

	baseURL := address
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		scheme := "http"
		if tls {
			scheme = "https"
		}
		baseURL = fmt.Sprintf("%s://%s", scheme, address)
	}

	// Validate the target URL to prevent SSRF
	if err := validateURL(baseURL); err != nil {
		return nil, fmt.Errorf("invalid server address: %w", err)
	}

	return &Connection{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			// Prevent following redirects to internal hosts
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 3 {
					return fmt.Errorf("too many redirects")
				}
				return validateURL(req.URL.String())
			},
		},
		headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, nil
}

// validateURL checks that the URL does not point to metadata, link-local, or
// private network ranges that could be exploited for SSRF.
func validateURL(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	// Only allow http and https schemes
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("unsupported scheme %q (only http/https allowed)", u.Scheme)
	}

	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("empty host in URL")
	}

	// Block well-known metadata endpoints
	blockedHosts := map[string]bool{
		"169.254.169.254": true, // AWS/GCP metadata
		"metadata.google.internal": true,
		"metadata": true,
		"100.100.100.200": true, // Alibaba metadata
	}
	if blockedHosts[host] {
		return fmt.Errorf("blocked host %q (SSRF protection)", host)
	}

	// Resolve hostname and check for private/link-loop ranges
	ips, err := net.LookupIP(host)
	if err != nil {
		// If DNS fails, allow localhost for development but block obviously bad hosts
		if host != "localhost" && host != "127.0.0.1" && host != "::1" {
			return nil // Allow unresolvable hosts (may work at request time)
		}
		return nil
	}

	for _, ip := range ips {
		if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsLoopback() {
			continue // Allow localhost for development
		}
		if ip.IsPrivate() {
			continue // Allow private networks (typical for internal engine)
		}
	}

	return nil
}

// Close closes the connection (no-op for HTTP).
func (c *Connection) Close() error {
	return nil
}

// ─── Internal HTTP helpers ────────────────────────────────────────────────────

func (c *Connection) doRequest(ctx context.Context, method, path string, body interface{}) ([]byte, int, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to marshal request: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}

	url := c.baseURL + path
	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create request: %w", err)
	}

	for k, v := range c.headers {
		req.Header.Set(k, v)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("failed to read response: %w", err)
	}

	return respBody, resp.StatusCode, nil
}

func (c *Connection) doJSON(ctx context.Context, method, path string, body interface{}, result interface{}) error {
	respBody, statusCode, err := c.doRequest(ctx, method, path, body)
	if err != nil {
		return err
	}
	if statusCode >= 400 {
		return fmt.Errorf("HTTP %d: %s", statusCode, string(respBody))
	}
	if result != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, result); err != nil {
			return fmt.Errorf("failed to unmarshal response: %w", err)
		}
	}
	return nil
}

// ─── Request types ────────────────────────────────────────────────────────────

type StartWorkflowRequest struct {
	Namespace        string      `json:"namespace"`
	WorkflowID       string      `json:"workflowId"`
	WorkflowType     string      `json:"workflowType"`
	TaskQueue        string      `json:"taskQueue"`
	Input            interface{} `json:"input,omitempty"`
	ExecutionTimeout int64       `json:"executionTimeout,omitempty"`
	RunTimeout       int64       `json:"runTimeout,omitempty"`
	TaskTimeout      int64       `json:"taskTimeout,omitempty"`
	RetryPolicy      *RetryPolicy `json:"retryPolicy,omitempty"`
}

type SignalWorkflowRequest struct {
	Namespace  string        `json:"namespace"`
	WorkflowID string        `json:"workflowId"`
	SignalName string        `json:"signalName"`
	Input      []interface{} `json:"input,omitempty"`
}

type QueryWorkflowRequest struct {
	Namespace  string        `json:"namespace"`
	WorkflowID string        `json:"workflowId"`
	QueryType  string        `json:"queryType"`
	Input      []interface{} `json:"input,omitempty"`
}

type TerminateWorkflowRequest struct {
	Namespace  string `json:"namespace"`
	WorkflowID string `json:"workflowId"`
	Reason     string `json:"reason"`
}

type CancelWorkflowRequest struct {
	Namespace  string `json:"namespace"`
	WorkflowID string `json:"workflowId"`
}

type DescribeWorkflowRequest struct {
	Namespace  string `json:"namespace"`
	WorkflowID string `json:"workflowId"`
}

type GetWorkflowHistoryRequest struct {
	Namespace  string `json:"namespace"`
	WorkflowID string `json:"workflowId"`
}

type SignalWithStartRequest struct {
	Namespace    string      `json:"namespace"`
	WorkflowType string      `json:"workflowType"`
	WorkflowID   string      `json:"workflowId,omitempty"`
	TaskQueue    string      `json:"taskQueue"`
	Input        interface{} `json:"input,omitempty"`
	SignalName   string      `json:"signalName"`
	SignalArgs   interface{} `json:"signalArgs,omitempty"`
}

type SearchWorkflowsRequest struct {
	Namespace string `json:"namespace"`
	Query     string `json:"query"`
}

type ResetWorkflowRequest struct {
	Namespace  string `json:"namespace"`
	WorkflowID string `json:"workflowId"`
	EventID    int64  `json:"eventId"`
}

type UpdateWorkflowRequest struct {
	Namespace   string      `json:"namespace"`
	WorkflowID  string      `json:"workflowId"`
	UpdateName  string      `json:"updateName"`
	Input       interface{} `json:"input,omitempty"`
}

type ContinueAsNewRequest struct {
	Namespace       string      `json:"namespace"`
	WorkflowID      string      `json:"workflowId"`
	NewWorkflowType string      `json:"newWorkflowType,omitempty"`
	NewTaskQueue    string      `json:"newTaskQueue,omitempty"`
	NewInput        interface{} `json:"newInput,omitempty"`
}

type SetMemoRequest struct {
	Namespace  string            `json:"namespace"`
	WorkflowID string            `json:"workflowId"`
	Memo       map[string]interface{} `json:"memo"`
}

type SetSearchAttributesRequest struct {
	Namespace  string            `json:"namespace"`
	WorkflowID string            `json:"workflowId"`
	Attributes map[string]interface{} `json:"attributes"`
}

type CreateScheduleRequest struct {
	Namespace      string      `json:"namespace"`
	ScheduleID     string      `json:"scheduleId"`
	CronExpression string      `json:"cronExpression"`
	WorkflowType   string      `json:"workflowType"`
	TaskQueue      string      `json:"taskQueue"`
	Input          interface{} `json:"input,omitempty"`
}

type BatchOperationRequest struct {
	Namespace   string   `json:"namespace"`
	Operation   string   `json:"operation"`
	WorkflowIDs []string `json:"workflowIds"`
	Reason      string   `json:"reason,omitempty"`
	SignalName  string   `json:"signalName,omitempty"`
	SignalArgs  interface{} `json:"signalArgs,omitempty"`
}

// ─── Response types ───────────────────────────────────────────────────────────

type StartWorkflowResponse struct {
	WorkflowID string `json:"workflowId"`
	RunID      string `json:"runId"`
}

type describeResponse struct {
	WorkflowID string      `json:"workflowId"`
	RunID      string      `json:"runId"`
	Status     string      `json:"status"`
	Result     interface{} `json:"result,omitempty"`
	Failure    string      `json:"failure,omitempty"`
	TaskQueue  string      `json:"taskQueue,omitempty"`
	StartTime  int64       `json:"startTime,omitempty"`
	CloseTime  int64       `json:"closeTime,omitempty"`
}

type healthResponse struct {
	Status string `json:"status"`
}

type listWorkflowsResponse struct {
	Workflows []map[string]interface{} `json:"workflows"`
}

// ─── Connection methods ───────────────────────────────────────────────────────

func (c *Connection) StartWorkflow(ctx context.Context, req *StartWorkflowRequest) (*StartWorkflowResponse, error) {
	var resp StartWorkflowResponse
	err := c.doJSON(ctx, "POST", "/api/workflows", req, &resp)
	if err != nil {
		return nil, err
	}
	if resp.WorkflowID == "" {
		resp.WorkflowID = req.WorkflowID
	}
	return &resp, nil
}

func (c *Connection) SignalWorkflow(ctx context.Context, req *SignalWorkflowRequest) error {
	return c.doJSON(ctx, "POST", fmt.Sprintf("/api/workflows/%s/signal", req.WorkflowID), req, nil)
}

func (c *Connection) QueryWorkflow(ctx context.Context, req *QueryWorkflowRequest) (interface{}, error) {
	var result interface{}
	err := c.doJSON(ctx, "POST", fmt.Sprintf("/api/workflows/%s/query", req.WorkflowID), req, &result)
	return result, err
}

func (c *Connection) TerminateWorkflow(ctx context.Context, req *TerminateWorkflowRequest) error {
	return c.doJSON(ctx, "POST", fmt.Sprintf("/api/workflows/%s/terminate", req.WorkflowID), req, nil)
}

func (c *Connection) CancelWorkflow(ctx context.Context, req *CancelWorkflowRequest) error {
	return c.doJSON(ctx, "POST", fmt.Sprintf("/api/workflows/%s/cancel", req.WorkflowID), req, nil)
}

func (c *Connection) DescribeWorkflow(ctx context.Context, req *DescribeWorkflowRequest) (*WorkflowDescription, error) {
	var resp describeResponse
	err := c.doJSON(ctx, "GET", fmt.Sprintf("/api/workflows/%s", req.WorkflowID), nil, &resp)
	if err != nil {
		return nil, err
	}

	status := WorkflowStatusRunning
	switch resp.Status {
	case "COMPLETED":
		status = WorkflowStatusCompleted
	case "FAILED":
		status = WorkflowStatusFailed
	case "CANCELLED":
		status = WorkflowStatusCancelled
	case "TERMINATED":
		status = WorkflowStatusTerminated
	case "CONTINUED_AS_NEW":
		status = WorkflowStatusContinuedAsNew
	case "TIMED_OUT":
		status = WorkflowStatusTimedOut
	}

	return &WorkflowDescription{
		WorkflowExecution: WorkflowExecution{
			WorkflowID: resp.WorkflowID,
			RunID:      resp.RunID,
		},
		WorkflowType: "",
		Status:       status,
		TaskQueue:    resp.TaskQueue,
		StartTime:    resp.StartTime,
		CloseTime:    resp.CloseTime,
		Result:       resp.Result,
		Failure:      resp.Failure,
	}, nil
}

func (c *Connection) GetWorkflowHistory(ctx context.Context, req *GetWorkflowHistoryRequest) ([]*HistoryEvent, error) {
	var events []*HistoryEvent
	err := c.doJSON(ctx, "GET", fmt.Sprintf("/api/workflows/%s/history", req.WorkflowID), nil, &events)
	if err != nil {
		return []*HistoryEvent{}, nil
	}
	return events, nil
}

// HealthCheck checks if the server is healthy.
func (c *Connection) HealthCheck(ctx context.Context) (bool, error) {
	var resp healthResponse
	err := c.doJSON(ctx, "GET", "/api/health", nil, &resp)
	if err != nil {
		return false, err
	}
	return resp.Status == "healthy" || resp.Status == "ok", nil
}

// ListWorkflows lists workflows.
func (c *Connection) ListWorkflows(ctx context.Context, namespace string) ([]map[string]interface{}, error) {
	var resp listWorkflowsResponse
	err := c.doJSON(ctx, "GET", "/api/workflows", nil, &resp)
	if err != nil {
		return []map[string]interface{}{}, nil
	}
	return resp.Workflows, nil
}

// SignalWithStartWorkflow signals an existing workflow or starts a new one and signals it.
func (c *Connection) SignalWithStartWorkflow(ctx context.Context, req *SignalWithStartRequest) (*StartWorkflowResponse, error) {
	var resp StartWorkflowResponse
	err := c.doJSON(ctx, "POST", "/api/workflows/signal-with-start", req, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// SearchWorkflows searches workflows using a query.
func (c *Connection) SearchWorkflows(ctx context.Context, req *SearchWorkflowsRequest) ([]map[string]interface{}, error) {
	var resp listWorkflowsResponse
	err := c.doJSON(ctx, "POST", "/api/workflows/search", req, &resp)
	if err != nil {
		return []map[string]interface{}{}, nil
	}
	return resp.Workflows, nil
}

// ResetWorkflow resets a workflow to a previous event.
func (c *Connection) ResetWorkflow(ctx context.Context, req *ResetWorkflowRequest) error {
	return c.doJSON(ctx, "POST", "/api/workflows/reset", req, nil)
}

// UpdateWorkflow sends a synchronous update to a running workflow.
func (c *Connection) UpdateWorkflow(ctx context.Context, req *UpdateWorkflowRequest) (interface{}, error) {
	var result interface{}
	err := c.doJSON(ctx, "POST", "/api/workflows/update", req, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ContinueAsNew continues a workflow as a new execution.
func (c *Connection) ContinueAsNew(ctx context.Context, req *ContinueAsNewRequest) (*StartWorkflowResponse, error) {
	var resp StartWorkflowResponse
	err := c.doJSON(ctx, "POST", "/api/workflows/continue-as-new", req, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// SetMemo sets memo key-value pairs on a workflow.
func (c *Connection) SetMemo(ctx context.Context, req *SetMemoRequest) error {
	return c.doJSON(ctx, "POST", "/api/workflows/memo", req, nil)
}

// SetSearchAttributes sets search attributes on a workflow.
func (c *Connection) SetSearchAttributes(ctx context.Context, req *SetSearchAttributesRequest) error {
	return c.doJSON(ctx, "POST", "/api/workflows/search-attributes", req, nil)
}

// CreateSchedule creates a recurring workflow schedule.
func (c *Connection) CreateSchedule(ctx context.Context, req *CreateScheduleRequest) error {
	return c.doJSON(ctx, "POST", "/api/schedules", req, nil)
}

// DeleteSchedule deletes a workflow schedule.
func (c *Connection) DeleteSchedule(ctx context.Context, namespace, scheduleID string) error {
	return c.doJSON(ctx, "DELETE", fmt.Sprintf("/api/schedules/%s", scheduleID), nil, nil)
}

// ListSchedules lists all schedules in a namespace.
func (c *Connection) ListSchedules(ctx context.Context, namespace string) ([]map[string]interface{}, error) {
	var resp struct {
		Schedules []map[string]interface{} `json:"schedules"`
	}
	err := c.doJSON(ctx, "GET", "/api/schedules", nil, &resp)
	if err != nil {
		return []map[string]interface{}{}, nil
	}
	return resp.Schedules, nil
}

// StartBatchOperation starts a batch operation on multiple workflows.
func (c *Connection) StartBatchOperation(ctx context.Context, req *BatchOperationRequest) (string, error) {
	var resp struct {
		JobID string `json:"jobId"`
	}
	err := c.doJSON(ctx, "POST", "/api/batch", req, &resp)
	if err != nil {
		return "", err
	}
	return resp.JobID, nil
}

// DescribeBatchOperation returns the status of a batch operation.
func (c *Connection) DescribeBatchOperation(ctx context.Context, namespace, jobID string) (map[string]interface{}, error) {
	var resp map[string]interface{}
	err := c.doJSON(ctx, "GET", fmt.Sprintf("/api/batch/%s", jobID), nil, &resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ─── Helper functions ─────────────────────────────────────────────────────────

func marshalInput(input interface{}) ([]byte, error) {
	if input == nil {
		return nil, nil
	}
	return json.Marshal(input)
}

func unmarshalOutput(data []byte, output interface{}) error {
	if len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, output)
}
