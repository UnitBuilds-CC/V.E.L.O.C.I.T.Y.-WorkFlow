// Package velocity provides a Go SDK for V.E.L.O.C.I.T.Y.-WorkFlow.
//
// V.E.L.O.C.I.T.Y.-WorkFlow is a hardware-native zero-allocation durable execution
// engine and Temporal alternative with superior performance.
package velocity

// WorkflowExecution represents a workflow execution.
type WorkflowExecution struct {
	WorkflowID string
	RunID      string
}

// WorkflowOptions contains options for starting a workflow.
type WorkflowOptions struct {
	WorkflowID       string
	TaskQueue        string
	WorkflowType     string
	Input            interface{}
	ExecutionTimeout int64 // milliseconds
	RunTimeout       int64 // milliseconds
	TaskTimeout      int64 // milliseconds
	RetryPolicy      *RetryPolicy
	Memo             map[string]interface{}
	SearchAttributes map[string]interface{}
}

// RetryPolicy defines the retry policy for workflows and activities.
type RetryPolicy struct {
	InitialInterval        int64    // milliseconds
	BackoffCoefficient     float64
	MaximumInterval        int64    // milliseconds
	MaximumAttempts        int32
	NonRetryableErrorTypes []string
}

// SignalOptions contains options for signaling a workflow.
type SignalOptions struct {
	SignalName string
	Args       []interface{}
}

// QueryOptions contains options for querying a workflow.
type QueryOptions struct {
	QueryType string
	Args      []interface{}
}

// WorkflowResult represents the result of a workflow execution.
type WorkflowResult struct {
	WorkflowExecution WorkflowExecution
	Result            interface{}
}

// HistoryEvent represents a single event in workflow history.
type HistoryEvent struct {
	EventID    int64
	EventType  string
	EventTime  int64
	TaskID     int64
	Attributes interface{}
}

// TaskQueue represents a task queue.
type TaskQueue struct {
	Name         string
	TaskType     string // "workflow" or "activity"
	BacklogCount int64
	Pollers      int32
}

// Schedule represents a workflow schedule.
type Schedule struct {
	ScheduleID     string
	WorkflowType   string
	State          string // "ACTIVE", "PAUSED", "COMPLETED"
	CronSchedule   string
	LastActionTime int64
}

// BatchOperation represents a batch operation.
type BatchOperation struct {
	JobID          string
	Operation      string // "terminate", "cancel", "signal", "query"
	Status         string // "RUNNING", "COMPLETED", "FAILED"
	TotalWorkflows int64
	Succeeded      int64
	Failed         int64
}

// WorkflowStatus represents the status of a workflow.
type WorkflowStatus string

const (
	WorkflowStatusRunning         WorkflowStatus = "RUNNING"
	WorkflowStatusCompleted       WorkflowStatus = "COMPLETED"
	WorkflowStatusFailed          WorkflowStatus = "FAILED"
	WorkflowStatusCancelled       WorkflowStatus = "CANCELLED"
	WorkflowStatusTerminated      WorkflowStatus = "TERMINATED"
	WorkflowStatusContinuedAsNew  WorkflowStatus = "CONTINUED_AS_NEW"
	WorkflowStatusTimedOut        WorkflowStatus = "TIMED_OUT"
)

// ActivityContext contains information about the current activity execution.
type ActivityContext struct {
	TaskToken         string
	WorkflowExecution WorkflowExecution
	ActivityID        string
	ActivityType      string
	Input             interface{}
	HeartbeatDetails  interface{}
	HeartbeatTimeout  int64
	ScheduledTime     int64
	StartedTime       int64
	Attempt           int32
}

// ActivityOptions contains options for executing an activity.
type ActivityOptions struct {
	TaskQueue              string
	ActivityType           string
	Input                  interface{}
	ScheduleToCloseTimeout int64 // milliseconds
	ScheduleToStartTimeout int64 // milliseconds
	StartToCloseTimeout    int64 // milliseconds
	HeartbeatTimeout       int64 // milliseconds
	RetryPolicy            *RetryPolicy
}

// TimerOptions contains options for creating a timer.
type TimerOptions struct {
	Duration int64 // milliseconds
}

// ChildWorkflowOptions contains options for starting a child workflow.
type ChildWorkflowOptions struct {
	WorkflowID       string
	WorkflowType     string
	TaskQueue        string
	Input            interface{}
	ExecutionTimeout int64 // milliseconds
	RunTimeout       int64 // milliseconds
	TaskTimeout      int64 // milliseconds
	RetryPolicy      *RetryPolicy
}
