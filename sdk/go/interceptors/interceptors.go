// Package interceptors provides interceptor framework for the VELOCITY-WorkFlow SDK.
//
// Interceptors implement a middleware pattern for workflow and activity lifecycle hooks.
// They can be chained to compose logging, metrics, tracing, and custom logic.
package interceptors

import (
	"log"
	"time"
)

// WorkflowInterceptor defines hooks for workflow lifecycle events.
type WorkflowInterceptor interface {
	// OnStart is called before a workflow starts.
	OnStart(workflowType string, workflowID uint64)

	// OnComplete is called after a workflow completes successfully.
	OnComplete(workflowID uint64, result []byte)

	// OnFail is called when a workflow fails.
	OnFail(workflowID uint64, err error)

	// OnSignal is called when a workflow receives a signal.
	OnSignal(workflowID uint64, signalName string)
}

// ActivityInterceptor defines hooks for activity lifecycle events.
type ActivityInterceptor interface {
	// OnExecute is called before an activity executes.
	OnExecute(activityType string, activityID string)

	// OnActivityComplete is called after an activity completes.
	OnActivityComplete(activityID string, result []byte)

	// OnActivityFail is called when an activity fails.
	OnActivityFail(activityID string, err error)
}

// LoggingInterceptor logs workflow and activity lifecycle events.
type LoggingInterceptor struct {
	prefix string
	logger *log.Logger
}

// NewLoggingInterceptor creates a new LoggingInterceptor.
func NewLoggingInterceptor(prefix string) *LoggingInterceptor {
	if prefix == "" {
		prefix = "[VELOCITY]"
	}
	return &LoggingInterceptor{
		prefix: prefix,
		logger: log.Default(),
	}
}

// OnStart logs workflow start.
func (l *LoggingInterceptor) OnStart(workflowType string, workflowID uint64) {
	l.logger.Printf("%s Workflow started: type=%s, id=%d", l.prefix, workflowType, workflowID)
}

// OnComplete logs workflow completion.
func (l *LoggingInterceptor) OnComplete(workflowID uint64, result []byte) {
	l.logger.Printf("%s Workflow completed: id=%d", l.prefix, workflowID)
}

// OnFail logs workflow failure.
func (l *LoggingInterceptor) OnFail(workflowID uint64, err error) {
	l.logger.Printf("%s Workflow failed: id=%d, error=%v", l.prefix, workflowID, err)
}

// OnSignal logs workflow signal.
func (l *LoggingInterceptor) OnSignal(workflowID uint64, signalName string) {
	l.logger.Printf("%s Workflow signal: id=%d, signal=%s", l.prefix, workflowID, signalName)
}

// OnExecute logs activity execution.
func (l *LoggingInterceptor) OnExecute(activityType string, activityID string) {
	l.logger.Printf("%s Activity executing: type=%s, id=%s", l.prefix, activityType, activityID)
}

// OnActivityComplete logs activity completion.
func (l *LoggingInterceptor) OnActivityComplete(activityID string, result []byte) {
	l.logger.Printf("%s Activity completed: id=%s", l.prefix, activityID)
}

// OnActivityFail logs activity failure.
func (l *LoggingInterceptor) OnActivityFail(activityID string, err error) {
	l.logger.Printf("%s Activity failed: id=%s, error=%v", l.prefix, activityID, err)
}

// MetricsInterceptor tracks workflow and activity metrics.
type MetricsInterceptor struct {
	WorkflowStarts      int64
	WorkflowCompletions int64
	WorkflowFailures    int64
	ActivityExecutions  int64
	ActivityCompletions int64
	ActivityFailures    int64
	startTimes          map[uint64]time.Time
}

// NewMetricsInterceptor creates a new MetricsInterceptor.
func NewMetricsInterceptor() *MetricsInterceptor {
	return &MetricsInterceptor{
		startTimes: make(map[uint64]time.Time),
	}
}

// OnStart tracks workflow start.
func (m *MetricsInterceptor) OnStart(workflowType string, workflowID uint64) {
	m.WorkflowStarts++
	m.startTimes[workflowID] = time.Now()
}

// OnComplete tracks workflow completion.
func (m *MetricsInterceptor) OnComplete(workflowID uint64, result []byte) {
	m.WorkflowCompletions++
	delete(m.startTimes, workflowID)
}

// OnFail tracks workflow failure.
func (m *MetricsInterceptor) OnFail(workflowID uint64, err error) {
	m.WorkflowFailures++
	delete(m.startTimes, workflowID)
}

// OnSignal tracks signal received.
func (m *MetricsInterceptor) OnSignal(workflowID uint64, signalName string) {
	// No-op for metrics
}

// OnExecute tracks activity execution.
func (m *MetricsInterceptor) OnExecute(activityType string, activityID string) {
	m.ActivityExecutions++
}

// OnActivityComplete tracks activity completion.
func (m *MetricsInterceptor) OnActivityComplete(activityID string, result []byte) {
	m.ActivityCompletions++
}

// OnActivityFail tracks activity failure.
func (m *MetricsInterceptor) OnActivityFail(activityID string, err error) {
	m.ActivityFailures++
}

// GetMetrics returns a snapshot of current metrics.
func (m *MetricsInterceptor) GetMetrics() map[string]int64 {
	return map[string]int64{
		"workflow_starts":      m.WorkflowStarts,
		"workflow_completions": m.WorkflowCompletions,
		"workflow_failures":    m.WorkflowFailures,
		"activity_executions":  m.ActivityExecutions,
		"activity_completions": m.ActivityCompletions,
		"activity_failures":    m.ActivityFailures,
	}
}

// InterceptorChain manages a chain of interceptors.
type InterceptorChain struct {
	interceptors []interface{}
}

// NewInterceptorChain creates a new InterceptorChain.
func NewInterceptorChain() *InterceptorChain {
	return &InterceptorChain{
		interceptors: make([]interface{}, 0),
	}
}

// Add adds an interceptor to the chain.
func (c *InterceptorChain) Add(interceptor interface{}) {
	c.interceptors = append(c.interceptors, interceptor)
}

// InvokeWorkflowStart invokes all workflow interceptors for start event.
func (c *InterceptorChain) InvokeWorkflowStart(workflowType string, workflowID uint64) {
	for _, interceptor := range c.interceptors {
		if wi, ok := interceptor.(WorkflowInterceptor); ok {
			wi.OnStart(workflowType, workflowID)
		}
	}
}

// InvokeWorkflowComplete invokes all workflow interceptors for complete event.
func (c *InterceptorChain) InvokeWorkflowComplete(workflowID uint64, result []byte) {
	for _, interceptor := range c.interceptors {
		if wi, ok := interceptor.(WorkflowInterceptor); ok {
			wi.OnComplete(workflowID, result)
		}
	}
}

// InvokeWorkflowFail invokes all workflow interceptors for fail event.
func (c *InterceptorChain) InvokeWorkflowFail(workflowID uint64, err error) {
	for _, interceptor := range c.interceptors {
		if wi, ok := interceptor.(WorkflowInterceptor); ok {
			wi.OnFail(workflowID, err)
		}
	}
}

// InvokeActivityExecute invokes all activity interceptors for execute event.
func (c *InterceptorChain) InvokeActivityExecute(activityType string, activityID string) {
	for _, interceptor := range c.interceptors {
		if ai, ok := interceptor.(ActivityInterceptor); ok {
			ai.OnExecute(activityType, activityID)
		}
	}
}
