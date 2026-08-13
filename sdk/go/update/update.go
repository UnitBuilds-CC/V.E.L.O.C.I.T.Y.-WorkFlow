// Package update provides the Workflow Update API — synchronous workflow mutation.
//
// Unlike signals (fire-and-forget), updates provide:
//   - Synchronous request/response semantics
//   - Wait policies (Accepted, Completed, Admitted)
//   - Validation before execution
//   - Named update handlers registered by workflows
//
// Usage:
//
//	client := update.NewUpdateClient("localhost:7234")
//	client.RegisterHandler("setAmount", func(args interface{}) interface{} {
//	    return args
//	}, nil)
//	result, err := client.executeUpdate(context.Background(), update.UpdateRequest{
//	    WorkflowKey: 42,
//	    UpdateName:  "setAmount",
//	    Args:        map[string]int{"amount": 100},
//	})
package update

import (
	"fmt"
	"sync"
	"time"
)

// UpdateStatus represents the status of a workflow update.
type UpdateStatus int

const (
	Admitted  UpdateStatus = 0
	Accepted  UpdateStatus = 1
	Completed UpdateStatus = 2
	Rejected  UpdateStatus = 3
)

// UpdateWaitPolicy controls how long to wait for an update.
type UpdateWaitPolicy int

const (
	WaitAdmitted  UpdateWaitPolicy = 0
	WaitAccepted  UpdateWaitPolicy = 1
	WaitCompleted UpdateWaitPolicy = 2
)

// UpdateRequest represents a request to execute a workflow update.
type UpdateRequest struct {
	WorkflowKey uint64
	UpdateID    string
	UpdateName  string
	Args        interface{}
	WaitPolicy  UpdateWaitPolicy
}

// UpdateResult represents the result of a workflow update.
type UpdateResult struct {
	UpdateID   string
	Status     UpdateStatus
	Result     interface{}
	Error      string
	DurationMs float64
}

// UpdateHandler is a named handler for workflow updates.
type UpdateHandler struct {
	Name      string
	Handler   func(args interface{}) interface{}
	Validator func(args interface{}) bool
}

// UpdateClient provides workflow update operations.
type UpdateClient struct {
	serverAddress string
	handlers      map[string]*UpdateHandler
	pending       map[string]*UpdateResult
	mu            sync.RWMutex
}

// NewUpdateClient creates a new update client.
func NewUpdateClient(serverAddress string) *UpdateClient {
	return &UpdateClient{
		serverAddress: serverAddress,
		handlers:      make(map[string]*UpdateHandler),
		pending:       make(map[string]*UpdateResult),
	}
}

// RegisterHandler registers a named update handler.
func (c *UpdateClient) RegisterHandler(name string, handler func(args interface{}) interface{}, validator func(args interface{}) bool) {
	c.handlers[name] = &UpdateHandler{
		Name:      name,
		Handler:   handler,
		Validator: validator,
	}
}

// ExecuteUpdate executes a workflow update synchronously.
func (c *UpdateClient) ExecuteUpdate(req UpdateRequest) (*UpdateResult, error) {
	uid := req.UpdateID
	if uid == "" {
		uid = fmt.Sprintf("update-%d-%d", req.WorkflowKey, time.Now().UnixMilli())
	}
	start := time.Now()

	handler, ok := c.handlers[req.UpdateName]
	if !ok {
		result := &UpdateResult{
			UpdateID:   uid,
			Status:     Rejected,
			Error:      fmt.Sprintf("no handler registered for update '%s'", req.UpdateName),
			DurationMs: float64(time.Since(start).Milliseconds()),
		}
		c.mu.Lock()
		c.pending[uid] = result
		c.mu.Unlock()
		return result, nil
	}

	if handler.Validator != nil && !handler.Validator(req.Args) {
		result := &UpdateResult{
			UpdateID:   uid,
			Status:     Rejected,
			Error:      "update validation failed",
			DurationMs: float64(time.Since(start).Milliseconds()),
		}
		c.mu.Lock()
		c.pending[uid] = result
		c.mu.Unlock()
		return result, nil
	}

	value := handler.Handler(req.Args)
	result := &UpdateResult{
		UpdateID:   uid,
		Status:     Completed,
		Result:     value,
		DurationMs: float64(time.Since(start).Milliseconds()),
	}
	c.mu.Lock()
	c.pending[uid] = result
	c.mu.Unlock()
	return result, nil
}

// GetUpdateResult retrieves the result of a previously executed update.
func (c *UpdateClient) GetUpdateResult(updateID string) *UpdateResult {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.pending[updateID]
}

// ListHandlers returns the names of registered update handlers.
func (c *UpdateClient) ListHandlers() []string {
	names := make([]string, 0, len(c.handlers))
	for name := range c.handlers {
		names = append(names, name)
	}
	return names
}

// ListPending returns the IDs of pending updates.
func (c *UpdateClient) ListPending() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ids := make([]string, 0, len(c.pending))
	for id := range c.pending {
		ids = append(ids, id)
	}
	return ids
}
