package velocity

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// WorkerOptions contains options for creating a Worker.
type WorkerOptions struct {
	HostPort   string
	Namespace  string
	TaskQueue  string
	Workflows  map[string]WorkflowFunction
	Activities map[string]ActivityFunction
}

// Worker executes workflows and activities.
type Worker struct {
	conn       *Connection
	namespace  string
	taskQueue  string
	running    bool
	stopCh     chan struct{}
	wg         sync.WaitGroup
	mu         sync.RWMutex

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

	conn, err := NewConnection(options.HostPort, false)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	// Register workflows and activities
	for name, fn := range options.Workflows {
		RegisterWorkflow(name, fn)
	}
	for name, fn := range options.Activities {
		RegisterActivity(name, fn)
	}

	return &Worker{
		conn:            conn,
		namespace:       options.Namespace,
		taskQueue:       options.TaskQueue,
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

// pollWorkflowTasks polls for workflow tasks from the engine.
func (w *Worker) pollWorkflowTasks() {
	defer w.wg.Done()

	for {
		select {
		case <-w.stopCh:
			return
		default:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			task, err := w.conn.ListWorkflows(ctx, w.namespace)
			cancel()

			if err != nil {
				log.Printf("Error polling workflow tasks: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			// Process any running workflows that need task completion
			_ = task
			time.Sleep(1 * time.Second)
		}
	}
}

// pollActivityTasks polls for activity tasks from the engine.
func (w *Worker) pollActivityTasks() {
	defer w.wg.Done()

	for {
		select {
		case <-w.stopCh:
			return
		default:
			// In HTTP mode, activity tasks are delivered via the engine API
			time.Sleep(1 * time.Second)
		}
	}
}

// ─── Connection methods for polling (HTTP-based) ─────────────────────────────

func (c *Connection) PollWorkflowTaskQueue(ctx context.Context, namespace, taskQueue string) (interface{}, error) {
	// In HTTP mode, workflow tasks come from the REST API
	return nil, nil
}

func (c *Connection) PollActivityTaskQueue(ctx context.Context, namespace, taskQueue string) (interface{}, error) {
	// In HTTP mode, activity tasks come from the REST API
	return nil, nil
}
