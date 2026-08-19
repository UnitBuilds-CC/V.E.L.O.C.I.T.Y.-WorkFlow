package velocity

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/unitbuilds/velocity-workflow/velocity-sdk-go/autoapply"
)

// WorkerOptions contains options for creating a Worker.
type WorkerOptions struct {
	HostPort   string
	Namespace  string
	TaskQueue  string
	BuildID    string
	Workflows  map[string]WorkflowFunction
	Activities map[string]ActivityFunction
}

// Worker executes workflows and activities.
type Worker struct {
	conn       *Connection
	namespace  string
	taskQueue  string
	buildID    string
	identity   string
	running    bool
	stopCh     chan struct{}
	wg         sync.WaitGroup
	mu         sync.RWMutex

	// Stats
	workflowsStarted   int64
	workflowsCompleted int64
	workflowsFailed    int64
	activitiesStarted  int64
	activitiesCompleted int64
	activitiesFailed   int64
	tasksPolled        int64

	// Active workflow executions
	executions map[string]*workflowExecution
	execMu     sync.RWMutex

	// Activity execution tracking
	activityResults map[string]chan activityResult
	activityMu      sync.Mutex
}

type workflowExecution struct {
	workflowID string
	runID      string
	cancelFn   context.CancelFunc
	doneCh     chan struct{}
	result     interface{}
	err        error
}

type activityResult struct {
	value interface{}
	err   error
}

// NewWorker creates a new Worker.
func NewWorker(options WorkerOptions) (*Worker, error) {
	if options.HostPort == "" {
		options.HostPort = "localhost:5000"
	}
	if options.Namespace == "" {
		options.Namespace = "default"
	}
	if options.TaskQueue == "" {
		return nil, fmt.Errorf("task queue is required")
	}
	if options.BuildID == "" {
		options.BuildID = "1.0"
	}

	conn, err := NewConnection(options.HostPort, false)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	hostname, _ := os.Hostname()
	identity := fmt.Sprintf("go-worker-%s@%s", options.BuildID, hostname)

	// Register workflows and activities from options
	for name, fn := range options.Workflows {
		RegisterWorkflow(name, fn)
	}
	for name, fn := range options.Activities {
		RegisterActivity(name, fn)
	}

	// Also merge autoapply registry entries
	autoWorkflows := autoapply.GetRegisteredWorkflows()
	for name, factory := range autoWorkflows {
		handler, err := factory()
		if err != nil {
			log.Printf("Warning: failed to create workflow instance for %s: %v", name, err)
			continue
		}
		// Wrap the autoapply handler as a WorkflowFunction
		_ = handler
		// The autoapply registry is available via GetWorkflow() fallback
	}

	return &Worker{
		conn:            conn,
		namespace:       options.Namespace,
		taskQueue:       options.TaskQueue,
		buildID:         options.BuildID,
		identity:        identity,
		stopCh:          make(chan struct{}),
		executions:      make(map[string]*workflowExecution),
		activityResults: make(map[string]chan activityResult),
	}, nil
}

// Run starts the worker and blocks until it's stopped.
func (w *Worker) Run() error {
	if w.running {
		return fmt.Errorf("worker is already running")
	}

	w.running = true
	log.Printf("Worker started for task queue: %s", w.taskQueue)

	// Start polling for workflow tasks
	w.wg.Add(1)
	go w.pollWorkflowTasks()

	// Start polling for activity tasks
	w.wg.Add(1)
	go w.pollActivityTasks()

	// Wait for stop signal
	<-w.stopCh

	// Wait for goroutines to finish
	w.wg.Wait()

	w.conn.Close()
	log.Println("Worker stopped")

	return nil
}

// Stop stops the worker.
func (w *Worker) Stop() {
	if !w.running {
		return
	}

	log.Println("Worker stopping...")
	close(w.stopCh)
	w.running = false
}

// IsRunning returns true if the worker is running.
func (w *Worker) IsRunning() bool {
	return w.running
}

// GetTaskQueue returns the task queue name.
func (w *Worker) GetTaskQueue() string {
	return w.taskQueue
}

// ─── Workflow Execution ───────────────────────────────────────────────────────

// ExecuteWorkflow runs a workflow locally (for testing or embedded mode).
func (w *Worker) ExecuteWorkflow(workflowID string, workflowType string, input interface{}) (interface{}, error) {
	fn, ok := GetWorkflow(workflowType)
	if !ok {
		return nil, fmt.Errorf("workflow %q not registered", workflowType)
	}

	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	wfCtx := WorkflowContext{
		WorkflowID: workflowID,
		RunID:      fmt.Sprintf("run-%s-%d", workflowID, time.Now().UnixNano()),
		TaskQueue:  w.taskQueue,
		_worker:    w,
		_cancelFn:  cancel,
	}

	// Track the execution
	exec := &workflowExecution{
		workflowID: workflowID,
		runID:      wfCtx.RunID,
		cancelFn:   cancel,
		doneCh:     make(chan struct{}),
	}
	w.execMu.Lock()
	w.executions[workflowID] = exec
	w.execMu.Unlock()

	defer func() {
		w.execMu.Lock()
		delete(w.executions, workflowID)
		w.execMu.Unlock()
		close(exec.doneCh)
	}()

	// Execute the workflow function
	result, err := fn(wfCtx, input)
	exec.result = result
	exec.err = err

	if err != nil {
		return nil, err
	}
	return result, nil
}

// ─── In-Workflow Operations (called by workflow.go functions) ─────────────────

// executeActivity runs an activity locally on this worker.
func (w *Worker) executeActivity(activityType string, input interface{}) (interface{}, error) {
	fn, ok := GetActivity(activityType)
	if !ok {
		return nil, fmt.Errorf("activity %q not registered", activityType)
	}

	actCtx := &ActivityContext{
		ActivityType: activityType,
		ActivityID:   fmt.Sprintf("act-%d", time.Now().UnixNano()),
		Attempt:      1,
	}

	return fn(actCtx, input)
}

// sleep pauses workflow execution for the given duration.
func (w *Worker) sleep(workflowID string, duration time.Duration) error {
	select {
	case <-time.After(duration):
		return nil
	case <-w.stopCh:
		return fmt.Errorf("worker stopped during sleep")
	}
}

// executeChildWorkflow runs a child workflow on this worker.
func (w *Worker) executeChildWorkflow(workflowType string, workflowID string, input interface{}) (interface{}, error) {
	childID := fmt.Sprintf("child-%s-%s", workflowID, workflowType)
	return w.ExecuteWorkflow(childID, workflowType, input)
}

// ─── Task Polling ─────────────────────────────────────────────────────────────

// WorkflowTask represents a pending workflow task from the server.
type WorkflowTask struct {
	TaskToken    uint64 `json:"task_token"`
	WorkflowKey  uint64 `json:"workflow_key"`
	WorkflowType string `json:"workflow_type"`
	WorkflowID   string `json:"workflow_id"`
	StepIndex    uint32 `json:"step_index"`
	Attempt      int32  `json:"attempt"`
	Input        []byte `json:"input"`
}

// ActivityTask represents a pending activity task from the server.
type ActivityTask struct {
	TaskToken    uint64 `json:"task_token"`
	WorkflowKey  uint64 `json:"workflow_key"`
	ActivityType string `json:"activity_type"`
	Input        []byte `json:"input"`
	StepIndex    uint32 `json:"step_index"`
	Attempt      int32  `json:"attempt"`
}

// pollWorkflowTasks polls for workflow tasks from the server.
func (w *Worker) pollWorkflowTasks() {
	defer w.wg.Done()

	for {
		select {
		case <-w.stopCh:
			return
		default:
			w.tasksPolled++

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			task, err := w.conn.PollWorkflowTask(ctx, w.namespace, w.taskQueue)
			cancel()

			if err != nil {
				log.Printf("Error polling workflow tasks: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			if task == nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			w.executeWorkflowTask(task)
		}
	}
}

// executeWorkflowTask dispatches a workflow task to the registered handler.
func (w *Worker) executeWorkflowTask(task *WorkflowTask) {
	fn, ok := GetWorkflow(task.WorkflowType)
	if !ok {
		handler, err := autoapply.CreateWorkflow(task.WorkflowType)
		if err != nil {
			log.Printf("No workflow registered for type: %s", task.WorkflowType)
			commands := []map[string]interface{}{
				{"fail_workflow": map[string]interface{}{
					"reason": fmt.Sprintf("no workflow registered for type: %s", task.WorkflowType),
				}},
			}
			w.conn.RespondWorkflowTaskCompleted(context.Background(), task.TaskToken, commands, w.identity, w.namespace)
			return
		}
		w.workflowsStarted++
		result, err := handler.Run(nil, json.RawMessage(task.Input))
		w.reportWorkflowResult(task.TaskToken, result, err)
		return
	}

	w.workflowsStarted++
	var input interface{}
	if len(task.Input) > 0 {
		json.Unmarshal(task.Input, &input)
	}

	wfCtx := WorkflowContext{
		WorkflowID: task.WorkflowID,
		RunID:      fmt.Sprintf("run-%s-%d", task.WorkflowID, time.Now().UnixNano()),
		TaskQueue:  w.taskQueue,
		_worker:    w,
	}

	result, err := fn(wfCtx, input)
	w.reportWorkflowResult(task.TaskToken, result, err)
}

// reportWorkflowResult sends the completion/failure response to the server.
func (w *Worker) reportWorkflowResult(taskToken uint64, result interface{}, err error) {
	var commands []map[string]interface{}
	if err != nil {
		w.workflowsFailed++
		commands = []map[string]interface{}{
			{"fail_workflow": map[string]interface{}{"reason": err.Error()}},
		}
	} else {
		w.workflowsCompleted++
		resultBytes, _ := json.Marshal(result)
		commands = []map[string]interface{}{
			{"complete_workflow": map[string]interface{}{"result": resultBytes}},
		}
	}
	w.conn.RespondWorkflowTaskCompleted(context.Background(), taskToken, commands, w.identity, w.namespace)
}

// pollActivityTasks polls for activity tasks from the server.
func (w *Worker) pollActivityTasks() {
	defer w.wg.Done()

	for {
		select {
		case <-w.stopCh:
			return
		default:
			w.tasksPolled++

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			task, err := w.conn.PollActivityTask(ctx, w.namespace, w.taskQueue)
			cancel()

			if err != nil {
				log.Printf("Error polling activity tasks: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			if task == nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			w.executeActivityTaskFromServer(task)
		}
	}
}

// executeActivityTaskFromServer dispatches an activity task to the registered handler.
func (w *Worker) executeActivityTaskFromServer(task *ActivityTask) {
	fn, ok := GetActivity(task.ActivityType)
	if !ok {
		handler, err := autoapply.GetActivity(task.ActivityType)
		if err != nil {
			log.Printf("No activity registered for type: %s", task.ActivityType)
			w.conn.RespondActivityTaskFailed(
				context.Background(), task.TaskToken,
				fmt.Sprintf("no activity registered for type: %s", task.ActivityType),
				w.identity, w.namespace,
			)
			return
		}
		w.activitiesStarted++
		actCtx := &ActivityContext{
			ActivityType: task.ActivityType,
			ActivityID:   fmt.Sprintf("act-%d", task.TaskToken),
			Attempt:      int(task.Attempt),
		}
		result, execErr := handler(actCtx, json.RawMessage(task.Input))
		w.reportActivityResult(task.TaskToken, result, execErr)
		return
	}

	w.activitiesStarted++
	var input interface{}
	if len(task.Input) > 0 {
		json.Unmarshal(task.Input, &input)
	}

	actCtx := &ActivityContext{
		ActivityType: task.ActivityType,
		ActivityID:   fmt.Sprintf("act-%d", task.TaskToken),
		Attempt:      int(task.Attempt),
	}

	result, err := fn(actCtx, input)
	w.reportActivityResult(task.TaskToken, result, err)
}

// reportActivityResult sends the completion/failure response to the server.
func (w *Worker) reportActivityResult(taskToken uint64, result interface{}, err error) {
	if err != nil {
		w.activitiesFailed++
		w.conn.RespondActivityTaskFailed(context.Background(), taskToken, err.Error(), w.identity, w.namespace)
		return
	}
	w.activitiesCompleted++
	resultBytes, _ := json.Marshal(result)
	w.conn.RespondActivityTaskCompleted(context.Background(), taskToken, string(resultBytes), w.identity, w.namespace)
}

// PollWorkflowTask polls the server for a workflow task (long-poll).
func (c *Connection) PollWorkflowTask(ctx context.Context, namespace, taskQueue string) (*WorkflowTask, error) {
	var result struct {
		Task *WorkflowTask `json:"task"`
	}
	err := c.doJSON(ctx, "POST", "/api/workers/poll-workflow", map[string]interface{}{
		"namespace": namespace,
		"taskQueue": taskQueue,
	}, &result)
	if err != nil {
		return nil, err
	}
	return result.Task, nil
}

// RespondWorkflowTaskCompleted reports a workflow task as completed with commands.
func (c *Connection) RespondWorkflowTaskCompleted(ctx context.Context, taskToken uint64, commands []map[string]interface{}, identity, namespace string) error {
	return c.doJSON(ctx, "POST", "/api/workers/respond-workflow", map[string]interface{}{
		"taskToken": taskToken,
		"commands":  commands,
		"identity":  identity,
		"namespace": namespace,
	}, nil)
}

// PollActivityTask polls the server for an activity task (long-poll).
func (c *Connection) PollActivityTask(ctx context.Context, namespace, taskQueue string) (*ActivityTask, error) {
	var result struct {
		Task *ActivityTask `json:"task"`
	}
	err := c.doJSON(ctx, "POST", "/api/workers/poll-activity", map[string]interface{}{
		"namespace": namespace,
		"taskQueue": taskQueue,
	}, &result)
	if err != nil {
		return nil, err
	}
	return result.Task, nil
}

// RespondActivityTaskCompleted reports an activity task as completed.
func (c *Connection) RespondActivityTaskCompleted(ctx context.Context, taskToken uint64, result string, identity, namespace string) error {
	return c.doJSON(ctx, "POST", "/api/workers/respond-activity", map[string]interface{}{
		"taskToken": taskToken,
		"result":    result,
		"identity":  identity,
		"namespace": namespace,
	}, nil)
}

// RespondActivityTaskFailed reports an activity task as failed.
func (c *Connection) RespondActivityTaskFailed(ctx context.Context, taskToken uint64, failure string, identity, namespace string) error {
	return c.doJSON(ctx, "POST", "/api/workers/respond-activity-failed", map[string]interface{}{
		"taskToken": taskToken,
		"failure":   failure,
		"identity":  identity,
		"namespace": namespace,
	}, nil)
}
