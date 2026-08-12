package velocity

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// WorkflowFunction represents a workflow function.
type WorkflowFunction func(ctx WorkflowContext, input interface{}) (interface{}, error)

// WorkflowContext contains information about the current workflow execution.
type WorkflowContext struct {
	WorkflowID       string
	RunID            string
	TaskQueue        string
	Memo             map[string]interface{}
	SearchAttributes map[string]interface{}

	// Internal fields used by the worker to support in-workflow operations
	_worker    *Worker
	_runID     string
	_cancelFn  context.CancelFunc
}

// workflowRegistry stores registered workflows.
var (
	workflowRegistry = make(map[string]WorkflowFunction)
	workflowMutex    sync.RWMutex
)

// RegisterWorkflow registers a workflow function.
func RegisterWorkflow(name string, fn WorkflowFunction) {
	workflowMutex.Lock()
	defer workflowMutex.Unlock()
	workflowRegistry[name] = fn
}

// GetWorkflow retrieves a registered workflow function.
func GetWorkflow(name string) (WorkflowFunction, bool) {
	workflowMutex.RLock()
	defer workflowMutex.RUnlock()
	fn, ok := workflowRegistry[name]
	return fn, ok
}

// HasWorkflow checks if a workflow is registered.
func HasWorkflow(name string) bool {
	workflowMutex.RLock()
	defer workflowMutex.RUnlock()
	_, ok := workflowRegistry[name]
	return ok
}

// ClearWorkflows removes all registered workflows (useful for testing).
func ClearWorkflows() {
	workflowMutex.Lock()
	defer workflowMutex.Unlock()
	workflowRegistry = make(map[string]WorkflowFunction)
}

// ─── In-Workflow Operations ───────────────────────────────────────────────────
// These functions are called from within workflow code and delegate to the
// worker that is executing the workflow.

// ExecuteActivity executes an activity from within a workflow context.
// The activity runs on the same worker and returns its result.
func ExecuteActivity(wfCtx WorkflowContext, activityType string, input interface{}) (interface{}, error) {
	if wfCtx._worker == nil {
		return nil, fmt.Errorf("ExecuteActivity: no worker bound to workflow context")
	}
	return wfCtx._worker.executeActivity(activityType, input)
}

// Sleep pauses the workflow for the specified duration. In a connected worker
// this uses a real timer; in local mode it uses time.Sleep.
func Sleep(wfCtx WorkflowContext, duration time.Duration) error {
	if wfCtx._worker == nil {
		time.Sleep(duration)
		return nil
	}
	return wfCtx._worker.sleep(wfCtx.WorkflowID, duration)
}

// ExecuteChildWorkflow starts a child workflow from within a parent workflow.
// The child runs on the same worker and returns its result.
func ExecuteChildWorkflow(wfCtx WorkflowContext, workflowType string, workflowID string, input interface{}) (interface{}, error) {
	if wfCtx._worker == nil {
		return nil, fmt.Errorf("ExecuteChildWorkflow: no worker bound to workflow context")
	}
	return wfCtx._worker.executeChildWorkflow(workflowType, workflowID, input)
}

// GetWorkflowInfo returns the current workflow context information.
func GetWorkflowInfo(wfCtx WorkflowContext) *WorkflowContext {
	return &wfCtx
}

// SignalExternal sends a signal to another running workflow via the engine.
func SignalExternal(wfCtx WorkflowContext, workflowID string, signalName string, input interface{}) error {
	if wfCtx._worker == nil {
		return fmt.Errorf("SignalExternal: no worker bound to workflow context")
	}
	return wfCtx._worker.conn.SignalWorkflow(context.Background(), &SignalWorkflowRequest{
		WorkflowID: workflowID,
		SignalName: signalName,
		Input:      []interface{}{input},
	})
}

// ─── Workflow Helper (legacy API, kept for backward compatibility) ────────────

// Workflow provides helper methods for workflow execution.
type Workflow struct{}

// ExecuteActivity executes an activity from within a workflow.
func (w *Workflow) ExecuteActivity(ctx context.Context, options ActivityOptions) (interface{}, error) {
	return nil, fmt.Errorf("use package-level ExecuteActivity(WorkflowContext, activityType, input) instead")
}

// Sleep sleeps for the specified duration.
func (w *Workflow) Sleep(ctx context.Context, duration int64) error {
	return fmt.Errorf("use package-level Sleep(WorkflowContext, duration) instead")
}

// ExecuteChildWorkflow starts a child workflow.
func (w *Workflow) ExecuteChildWorkflow(ctx context.Context, options ChildWorkflowOptions) (interface{}, error) {
	return nil, fmt.Errorf("use package-level ExecuteChildWorkflow(WorkflowContext, type, id, input) instead")
}

// GetInfo returns the current workflow context.
func (w *Workflow) GetInfo() (*WorkflowContext, error) {
	return nil, fmt.Errorf("use package-level GetWorkflowInfo(WorkflowContext) instead")
}
