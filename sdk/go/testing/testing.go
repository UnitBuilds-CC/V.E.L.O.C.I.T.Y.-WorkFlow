// Package testing provides test utilities for the VELOCITY-WorkFlow SDK.
//
// Provides test environment and mock client for unit testing workflows
// without requiring a running VELOCITY-WorkFlow server.
package testing

import (
	"fmt"
	"time"

	velocity_sdk "velocity_sdk/velocity_sdk"
	"velocity_sdk/velocity_sdk/errors"
)

// MockClient is a mock client for testing workflows without a server.
type MockClient struct {
	workflows map[uint64]*workflowState
	signals   map[uint64][]signal
	nextKey   uint64
}

type workflowState struct {
	workflowType string
	namespace    string
	taskQueue    string
	totalSteps   int32
	currentStep  int32
	status       velocity_sdk.WorkflowStatus
	result       []byte
}

type signal struct {
	signalName string
	payload    []byte
}

// NewMockClient creates a new MockClient.
func NewMockClient() *MockClient {
	return &MockClient{
		workflows: make(map[uint64]*workflowState),
		signals:   make(map[uint64][]signal),
		nextKey:   1,
	}
}

// StartWorkflow starts a mock workflow.
func (m *MockClient) StartWorkflow(opts *velocity_sdk.StartWorkflowOptions) (*velocity_sdk.WorkflowHandle, error) {
	key := m.nextKey
	m.nextKey++

	m.workflows[key] = &workflowState{
		workflowType: opts.WorkflowType,
		namespace:    opts.Namespace,
		taskQueue:    opts.TaskQueue,
		totalSteps:   opts.TotalSteps,
		currentStep:  0,
		status:       velocity_sdk.StatusRunning,
		result:       nil,
	}
	m.signals[key] = []signal{}

	return &velocity_sdk.WorkflowHandle{
		WorkflowKey: key,
		WorkflowID:  fmt.Sprintf("%d", key),
		Status:      velocity_sdk.StatusRunning,
	}, nil
}

// DescribeWorkflow describes a mock workflow.
func (m *MockClient) DescribeWorkflow(workflowKey uint64) (*velocity_sdk.WorkflowDescription, error) {
	state, ok := m.workflows[workflowKey]
	if !ok {
		return nil, errors.NewWorkflowNotFoundError(workflowKey)
	}

	return &velocity_sdk.WorkflowDescription{
		WorkflowKey: workflowKey,
		Status:      state.status,
		CurrentStep: state.currentStep,
		TotalSteps:  state.totalSteps,
		Namespace:   state.namespace,
		Result:      state.result,
	}, nil
}

// SignalWorkflow sends a signal to a mock workflow.
func (m *MockClient) SignalWorkflow(workflowKey uint64, signalName string, payload []byte) (bool, error) {
	state, ok := m.workflows[workflowKey]
	if !ok {
		return false, errors.NewWorkflowNotFoundError(workflowKey)
	}
	_ = state

	m.signals[workflowKey] = append(m.signals[workflowKey], signal{
		signalName: signalName,
		payload:    payload,
	})
	return true, nil
}

// CompleteWorkflow completes a mock workflow.
func (m *MockClient) CompleteWorkflow(workflowKey uint64, result []byte) (bool, error) {
	state, ok := m.workflows[workflowKey]
	if !ok {
		return false, errors.NewWorkflowNotFoundError(workflowKey)
	}
	if state.status != velocity_sdk.StatusRunning {
		return false, errors.NewWorkflowAlreadyCompletedError(workflowKey)
	}

	state.status = velocity_sdk.StatusCompleted
	state.result = result
	return true, nil
}

// FailWorkflow fails a mock workflow.
func (m *MockClient) FailWorkflow(workflowKey uint64, reason string) (bool, error) {
	state, ok := m.workflows[workflowKey]
	if !ok {
		return false, errors.NewWorkflowNotFoundError(workflowKey)
	}
	if state.status != velocity_sdk.StatusRunning {
		return false, errors.NewWorkflowAlreadyCompletedError(workflowKey)
	}

	state.status = velocity_sdk.StatusFailed
	return true, nil
}

// CancelWorkflow cancels a mock workflow.
func (m *MockClient) CancelWorkflow(workflowKey uint64) (bool, error) {
	state, ok := m.workflows[workflowKey]
	if !ok {
		return false, errors.NewWorkflowNotFoundError(workflowKey)
	}

	state.status = velocity_sdk.StatusCanceled
	return true, nil
}

// GetSignals returns all signals received by a workflow.
func (m *MockClient) GetSignals(workflowKey uint64) []signal {
	return m.signals[workflowKey]
}

// TestWorkflowEnvironment provides a test environment for running workflows in isolation.
type TestWorkflowEnvironment struct {
	Client     *MockClient
	timeOffset int64
}

// NewTestWorkflowEnvironment creates a new TestWorkflowEnvironment.
func NewTestWorkflowEnvironment() *TestWorkflowEnvironment {
	return &TestWorkflowEnvironment{
		Client:     NewMockClient(),
		timeOffset: 0,
	}
}

// StartWorkflow starts a workflow in the test environment.
func (e *TestWorkflowEnvironment) StartWorkflow(opts *velocity_sdk.StartWorkflowOptions) (*velocity_sdk.WorkflowHandle, error) {
	return e.Client.StartWorkflow(opts)
}

// CompleteWorkflow completes a workflow in the test environment.
func (e *TestWorkflowEnvironment) CompleteWorkflow(workflowKey uint64, result []byte) (bool, error) {
	return e.Client.CompleteWorkflow(workflowKey, result)
}

// SignalWorkflow signals a workflow in the test environment.
func (e *TestWorkflowEnvironment) SignalWorkflow(workflowKey uint64, signalName string, payload []byte) (bool, error) {
	return e.Client.SignalWorkflow(workflowKey, signalName, payload)
}

// TimeSkip advances the test environment's clock.
func (e *TestWorkflowEnvironment) TimeSkip(seconds int64) {
	e.timeOffset += seconds
}

// GetCurrentTime returns the current test time (real time + offset).
func (e *TestWorkflowEnvironment) GetCurrentTime() int64 {
	return time.Now().Unix() + e.timeOffset
}

// AssertWorkflowCompleted asserts that a workflow has completed.
func (e *TestWorkflowEnvironment) AssertWorkflowCompleted(workflowKey uint64) error {
	desc, err := e.Client.DescribeWorkflow(workflowKey)
	if err != nil {
		return err
	}
	if desc.Status != velocity_sdk.StatusCompleted {
		return fmt.Errorf("expected workflow %d to be completed, but status is %s", workflowKey, desc.Status)
	}
	return nil
}

// AssertSignalReceived asserts that a workflow received a specific signal.
func (e *TestWorkflowEnvironment) AssertSignalReceived(workflowKey uint64, signalName string) error {
	signals := e.Client.GetSignals(workflowKey)
	for _, s := range signals {
		if s.signalName == signalName {
			return nil
		}
	}
	return fmt.Errorf("expected signal '%s' not found for workflow %d", signalName, workflowKey)
}

// Reset resets the test environment.
func (e *TestWorkflowEnvironment) Reset() {
	e.Client = NewMockClient()
	e.timeOffset = 0
}
