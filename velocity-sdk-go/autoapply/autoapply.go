// Package autoapply provides auto-registration for workflows and activities in the VELOCITY Go SDK.
//
// This package enables annotation-driven workflow and activity registration using Go's init()
// pattern. When a workflow or activity is registered via RegisterWorkflow or RegisterActivity,
// it is stored in a global registry. The Worker discovers all registered handlers at startup.
//
// Example:
//
//	func init() {
//	    autoapply.RegisterWorkflow("OrderWorkflow", &OrderWorkflow{})
//	    autoapply.RegisterActivity("process_payment", ProcessPayment)
//	}
//
//	type OrderWorkflow struct{}
//	func (w *OrderWorkflow) Run(ctx WorkflowContext, orderID string) (string, error) {
//	    return ctx.ExecuteActivity("process_payment", orderID)
//	}
//
//	func ProcessPayment(orderID string) (string, error) {
//	    return "charged", nil
//	}
package autoapply

import (
	"fmt"
	"sync"
)

// WorkflowHandler is the interface that all workflow implementations must satisfy.
type WorkflowHandler interface {
	// Run executes the workflow with the given context and input.
	Run(ctx interface{}, input interface{}) (interface{}, error)
}

// ActivityHandler is a function that executes an activity.
type ActivityHandler func(ctx interface{}, input interface{}) (interface{}, error)

var (
	workflowRegistry = make(map[string]func() WorkflowHandler)
	activityRegistry = make(map[string]ActivityHandler)
	registryMutex    sync.RWMutex
)

// RegisterWorkflow registers a workflow factory in the global registry.
//
// The factory function should return a new instance of the workflow handler.
// This allows the Worker to create fresh instances for each workflow execution.
//
// Example:
//
//	autoapply.RegisterWorkflow("OrderWorkflow", func() autoapply.WorkflowHandler {
//	    return &OrderWorkflow{}
//	})
func RegisterWorkflow(workflowType string, factory func() WorkflowHandler) {
	registryMutex.Lock()
	defer registryMutex.Unlock()
	workflowRegistry[workflowType] = factory
}

// RegisterActivity registers an activity handler in the global registry.
//
// Example:
//
//	autoapply.RegisterActivity("process_payment", func(ctx interface{}, input interface{}) (interface{}, error) {
//	    return ProcessPayment(input.(string))
//	})
func RegisterActivity(activityName string, handler ActivityHandler) {
	registryMutex.Lock()
	defer registryMutex.Unlock()
	activityRegistry[activityName] = handler
}

// GetRegisteredWorkflows returns a copy of all registered workflow types.
func GetRegisteredWorkflows() map[string]func() WorkflowHandler {
	registryMutex.RLock()
	defer registryMutex.RUnlock()
	result := make(map[string]func() WorkflowHandler, len(workflowRegistry))
	for k, v := range workflowRegistry {
		result[k] = v
	}
	return result
}

// GetRegisteredActivities returns a copy of all registered activity names.
func GetRegisteredActivities() map[string]ActivityHandler {
	registryMutex.RLock()
	defer registryMutex.RUnlock()
	result := make(map[string]ActivityHandler, len(activityRegistry))
	for k, v := range activityRegistry {
		result[k] = v
	}
	return result
}

// CreateWorkflow creates a new instance of a registered workflow by type.
func CreateWorkflow(workflowType string) (WorkflowHandler, error) {
	registryMutex.RLock()
	defer registryMutex.RUnlock()
	factory, ok := workflowRegistry[workflowType]
	if !ok {
		return nil, fmt.Errorf("no workflow registered for type: %s", workflowType)
	}
	return factory(), nil
}

// GetActivity returns a registered activity handler by name.
func GetActivity(activityName string) (ActivityHandler, error) {
	registryMutex.RLock()
	defer registryMutex.RUnlock()
	handler, ok := activityRegistry[activityName]
	if !ok {
		return nil, fmt.Errorf("no activity registered for name: %s", activityName)
	}
	return handler, nil
}

// ClearRegistries clears both workflow and activity registries (useful for testing).
func ClearRegistries() {
	registryMutex.Lock()
	defer registryMutex.Unlock()
	workflowRegistry = make(map[string]func() WorkflowHandler)
	activityRegistry = make(map[string]ActivityHandler)
}

// WorkflowCount returns the number of registered workflows.
func WorkflowCount() int {
	registryMutex.RLock()
	defer registryMutex.RUnlock()
	return len(workflowRegistry)
}

// ActivityCount returns the number of registered activities.
func ActivityCount() int {
	registryMutex.RLock()
	defer registryMutex.RUnlock()
	return len(activityRegistry)
}
