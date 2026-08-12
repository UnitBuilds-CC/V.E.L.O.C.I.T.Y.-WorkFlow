package velocity

import (
	"fmt"
	"sync"
)

// ActivityFunction represents an activity function.
type ActivityFunction func(ctx *ActivityContext, input interface{}) (interface{}, error)

// activityRegistry stores registered activities.
var (
	activityRegistry = make(map[string]ActivityFunction)
	activityMutex    sync.RWMutex
)

// RegisterActivity registers an activity function.
func RegisterActivity(name string, fn ActivityFunction) {
	activityMutex.Lock()
	defer activityMutex.Unlock()
	activityRegistry[name] = fn
}

// GetActivity retrieves a registered activity function.
func GetActivity(name string) (ActivityFunction, bool) {
	activityMutex.RLock()
	defer activityMutex.RUnlock()
	fn, ok := activityRegistry[name]
	return fn, ok
}

// HasActivity checks if an activity is registered.
func HasActivity(name string) bool {
	activityMutex.RLock()
	defer activityMutex.RUnlock()
	_, ok := activityRegistry[name]
	return ok
}

// ClearActivities removes all registered activities (useful for testing).
func ClearActivities() {
	activityMutex.Lock()
	defer activityMutex.Unlock()
	activityRegistry = make(map[string]ActivityFunction)
}

// Activity provides helper methods for activity execution.
type Activity struct{}

// Heartbeat records a heartbeat for the current activity.
func (a *Activity) Heartbeat(details interface{}) {
	// In HTTP mode, heartbeats are sent via the engine API
	fmt.Printf("Activity heartbeat: %v\n", details)
}

// GetInfo returns the current activity context.
func (a *Activity) GetInfo() (*ActivityContext, error) {
	return nil, fmt.Errorf("use ActivityContext passed to activity function instead")
}
