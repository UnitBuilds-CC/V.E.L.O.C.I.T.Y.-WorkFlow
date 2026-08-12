package velocity

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// ─── Registration Tests ───────────────────────────────────────────────────────

func TestWorkflowRegistration(t *testing.T) {
	ClearWorkflows()
	defer ClearWorkflows()

	workflow := func(ctx WorkflowContext, input interface{}) (interface{}, error) {
		return "test", nil
	}

	RegisterWorkflow("test-workflow", workflow)

	if !HasWorkflow("test-workflow") {
		t.Error("Expected workflow to be registered")
	}

	retrieved, ok := GetWorkflow("test-workflow")
	if !ok || retrieved == nil {
		t.Error("Expected to retrieve workflow")
	}

	if HasWorkflow("non-existent") {
		t.Error("Expected non-existent workflow to not be registered")
	}
}

func TestActivityRegistration(t *testing.T) {
	ClearActivities()
	defer ClearActivities()

	activity := func(ctx *ActivityContext, input interface{}) (interface{}, error) {
		return "test", nil
	}

	RegisterActivity("test-activity", activity)

	if !HasActivity("test-activity") {
		t.Error("Expected activity to be registered")
	}

	retrieved, ok := GetActivity("test-activity")
	if !ok || retrieved == nil {
		t.Error("Expected to retrieve activity")
	}

	if HasActivity("non-existent") {
		t.Error("Expected non-existent activity to not be registered")
	}
}

func TestClearRegistries(t *testing.T) {
	RegisterWorkflow("temp-wf", func(ctx WorkflowContext, input interface{}) (interface{}, error) {
		return nil, nil
	})
	RegisterActivity("temp-act", func(ctx *ActivityContext, input interface{}) (interface{}, error) {
		return nil, nil
	})

	ClearWorkflows()
	ClearActivities()

	if HasWorkflow("temp-wf") {
		t.Error("Expected workflows to be cleared")
	}
	if HasActivity("temp-act") {
		t.Error("Expected activities to be cleared")
	}
}

// ─── Context Tests ────────────────────────────────────────────────────────────

func TestWorkflowContext(t *testing.T) {
	ctx := WorkflowContext{
		WorkflowID: "test-workflow-id",
		RunID:      "test-run-id",
		TaskQueue:  "test-queue",
	}

	if ctx.WorkflowID != "test-workflow-id" {
		t.Errorf("Expected WorkflowID 'test-workflow-id', got '%s'", ctx.WorkflowID)
	}
	if ctx.RunID != "test-run-id" {
		t.Errorf("Expected RunID 'test-run-id', got '%s'", ctx.RunID)
	}
}

func TestGetWorkflowInfo(t *testing.T) {
	wfCtx := WorkflowContext{
		WorkflowID: "wf-123",
		RunID:      "run-456",
		TaskQueue:  "my-queue",
		Memo:       map[string]interface{}{"key": "value"},
	}

	info := GetWorkflowInfo(wfCtx)
	if info.WorkflowID != "wf-123" {
		t.Errorf("Expected WorkflowID 'wf-123', got '%s'", info.WorkflowID)
	}
	if info.Memo["key"] != "value" {
		t.Error("Expected memo to be preserved")
	}
}

// ─── Worker Execution Tests ───────────────────────────────────────────────────

func TestWorkerExecuteWorkflow(t *testing.T) {
	ClearWorkflows()
	ClearActivities()
	defer ClearWorkflows()
	defer ClearActivities()

	RegisterWorkflow("simple-wf", func(ctx WorkflowContext, input interface{}) (interface{}, error) {
		return map[string]interface{}{"result": "hello"}, nil
	})

	worker := &Worker{
		taskQueue:       "test-queue",
		executions:      make(map[string]*workflowExecution),
		activityResults: make(map[string]chan activityResult),
		stopCh:          make(chan struct{}),
	}

	result, err := worker.ExecuteWorkflow("wf-1", "simple-wf", nil)
	if err != nil {
		t.Fatalf("ExecuteWorkflow failed: %v", err)
	}

	resultMap, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("Expected map result, got %T", result)
	}
	if resultMap["result"] != "hello" {
		t.Errorf("Expected result 'hello', got '%v'", resultMap["result"])
	}
}

func TestWorkerExecuteWorkflowWithActivity(t *testing.T) {
	ClearWorkflows()
	ClearActivities()
	defer ClearWorkflows()
	defer ClearActivities()

	RegisterActivity("greet", func(ctx *ActivityContext, input interface{}) (interface{}, error) {
		name := input.(string)
		return fmt.Sprintf("Hello, %s!", name), nil
	})

	RegisterWorkflow("activity-wf", func(ctx WorkflowContext, input interface{}) (interface{}, error) {
		result, err := ExecuteActivity(ctx, "greet", "World")
		if err != nil {
			return nil, err
		}
		return result, nil
	})

	worker := &Worker{
		taskQueue:       "test-queue",
		executions:      make(map[string]*workflowExecution),
		activityResults: make(map[string]chan activityResult),
		stopCh:          make(chan struct{}),
	}

	result, err := worker.ExecuteWorkflow("wf-2", "activity-wf", nil)
	if err != nil {
		t.Fatalf("ExecuteWorkflow failed: %v", err)
	}
	if result != "Hello, World!" {
		t.Errorf("Expected 'Hello, World!', got '%v'", result)
	}
}

func TestWorkerExecuteWorkflowWithChildWorkflow(t *testing.T) {
	ClearWorkflows()
	defer ClearWorkflows()

	RegisterWorkflow("child-wf", func(ctx WorkflowContext, input interface{}) (interface{}, error) {
		val := input.(float64)
		return val * 2, nil
	})

	RegisterWorkflow("parent-wf", func(ctx WorkflowContext, input interface{}) (interface{}, error) {
		result, err := ExecuteChildWorkflow(ctx, "child-wf", "child-1", 21.0)
		if err != nil {
			return nil, err
		}
		return result, nil
	})

	worker := &Worker{
		taskQueue:       "test-queue",
		executions:      make(map[string]*workflowExecution),
		activityResults: make(map[string]chan activityResult),
		stopCh:          make(chan struct{}),
	}

	result, err := worker.ExecuteWorkflow("parent-1", "parent-wf", nil)
	if err != nil {
		t.Fatalf("ExecuteWorkflow failed: %v", err)
	}
	if result != 42.0 {
		t.Errorf("Expected 42.0, got '%v'", result)
	}
}

func TestWorkerExecuteWorkflowUnregistered(t *testing.T) {
	ClearWorkflows()
	defer ClearWorkflows()

	worker := &Worker{
		taskQueue:       "test-queue",
		executions:      make(map[string]*workflowExecution),
		activityResults: make(map[string]chan activityResult),
		stopCh:          make(chan struct{}),
	}

	_, err := worker.ExecuteWorkflow("wf-x", "nonexistent", nil)
	if err == nil {
		t.Fatal("Expected error for unregistered workflow")
	}
}

func TestWorkerExecuteWorkflowError(t *testing.T) {
	ClearWorkflows()
	defer ClearWorkflows()

	RegisterWorkflow("failing-wf", func(ctx WorkflowContext, input interface{}) (interface{}, error) {
		return nil, fmt.Errorf("workflow failed intentionally")
	})

	worker := &Worker{
		taskQueue:       "test-queue",
		executions:      make(map[string]*workflowExecution),
		activityResults: make(map[string]chan activityResult),
		stopCh:          make(chan struct{}),
	}

	_, err := worker.ExecuteWorkflow("wf-err", "failing-wf", nil)
	if err == nil {
		t.Fatal("Expected error from failing workflow")
	}
	if err.Error() != "workflow failed intentionally" {
		t.Errorf("Expected 'workflow failed intentionally', got '%v'", err)
	}
}

func TestWorkerSleep(t *testing.T) {
	worker := &Worker{
		stopCh: make(chan struct{}),
	}

	start := time.Now()
	err := worker.sleep("wf-sleep", 50*time.Millisecond)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Sleep failed: %v", err)
	}
	if elapsed < 40*time.Millisecond {
		t.Errorf("Sleep returned too quickly: %v", elapsed)
	}
}

// ─── In-Workflow Operations Without Worker ────────────────────────────────────

func TestExecuteActivityWithoutWorker(t *testing.T) {
	wfCtx := WorkflowContext{WorkflowID: "test"}
	_, err := ExecuteActivity(wfCtx, "some-activity", nil)
	if err == nil {
		t.Fatal("Expected error when no worker is bound")
	}
}

func TestExecuteChildWorkflowWithoutWorker(t *testing.T) {
	wfCtx := WorkflowContext{WorkflowID: "test"}
	_, err := ExecuteChildWorkflow(wfCtx, "child", "child-1", nil)
	if err == nil {
		t.Fatal("Expected error when no worker is bound")
	}
}

// ─── Connection / HTTP Transport Tests ────────────────────────────────────────

func TestNewConnection(t *testing.T) {
	conn, err := NewConnection("localhost:5000", false)
	if err != nil {
		t.Fatalf("NewConnection failed: %v", err)
	}
	defer conn.Close()

	if conn.baseURL != "http://localhost:5000" {
		t.Errorf("Expected baseURL 'http://localhost:5000', got '%s'", conn.baseURL)
	}
}

func TestNewConnectionWithScheme(t *testing.T) {
	conn, err := NewConnection("https://velocity.example.com:8443", false)
	if err != nil {
		t.Fatalf("NewConnection failed: %v", err)
	}
	if conn.baseURL != "https://velocity.example.com:8443" {
		t.Errorf("Expected baseURL preserved, got '%s'", conn.baseURL)
	}
}

func TestNewConnectionTLS(t *testing.T) {
	conn, err := NewConnection("velocity.example.com", true)
	if err != nil {
		t.Fatalf("NewConnection failed: %v", err)
	}
	if conn.baseURL != "https://velocity.example.com" {
		t.Errorf("Expected https:// prefix, got '%s'", conn.baseURL)
	}
}

func TestConnectionSSRFProtection(t *testing.T) {
	_, err := NewConnection("http://169.254.169.254/latest/meta-data", false)
	if err == nil {
		t.Fatal("Expected SSRF protection to block metadata endpoint")
	}
}

func TestConnectionHealthCheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/health" {
			json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	conn, err := NewConnection(server.URL, false)
	if err != nil {
		t.Fatalf("NewConnection failed: %v", err)
	}

	healthy, err := conn.HealthCheck(context.Background())
	if err != nil {
		t.Fatalf("HealthCheck failed: %v", err)
	}
	if !healthy {
		t.Error("Expected healthy=true")
	}
}

func TestConnectionStartWorkflow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && r.URL.Path == "/api/workflows" {
			json.NewEncoder(w).Encode(map[string]string{
				"workflowId": "wf-test",
				"runId":      "run-test-1",
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	conn, err := NewConnection(server.URL, false)
	if err != nil {
		t.Fatalf("NewConnection failed: %v", err)
	}

	resp, err := conn.StartWorkflow(context.Background(), &StartWorkflowRequest{
		WorkflowID:   "wf-test",
		WorkflowType: "TestWorkflow",
		TaskQueue:    "test-queue",
	})
	if err != nil {
		t.Fatalf("StartWorkflow failed: %v", err)
	}
	if resp.WorkflowID != "wf-test" {
		t.Errorf("Expected workflowId 'wf-test', got '%s'", resp.WorkflowID)
	}
	if resp.RunID != "run-test-1" {
		t.Errorf("Expected runId 'run-test-1', got '%s'", resp.RunID)
	}
}

func TestConnectionDescribeWorkflow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Path == "/api/workflows/wf-1" {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"workflowId": "wf-1",
				"runId":      "run-1",
				"status":     "COMPLETED",
				"result":     map[string]interface{}{"value": 42},
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	conn, err := NewConnection(server.URL, false)
	if err != nil {
		t.Fatalf("NewConnection failed: %v", err)
	}

	desc, err := conn.DescribeWorkflow(context.Background(), &DescribeWorkflowRequest{
		WorkflowID: "wf-1",
	})
	if err != nil {
		t.Fatalf("DescribeWorkflow failed: %v", err)
	}
	if desc.Status != WorkflowStatusCompleted {
		t.Errorf("Expected COMPLETED status, got '%s'", desc.Status)
	}
	if desc.WorkflowExecution.WorkflowID != "wf-1" {
		t.Errorf("Expected workflowId 'wf-1', got '%s'", desc.WorkflowExecution.WorkflowID)
	}
}

func TestConnectionSignalWorkflow(t *testing.T) {
	signalReceived := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && r.URL.Path == "/api/workflows/wf-1/signal" {
			signalReceived = true
			w.WriteHeader(200)
			json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	conn, err := NewConnection(server.URL, false)
	if err != nil {
		t.Fatalf("NewConnection failed: %v", err)
	}

	err = conn.SignalWorkflow(context.Background(), &SignalWorkflowRequest{
		WorkflowID: "wf-1",
		SignalName: "my-signal",
		Input:      []interface{}{"data"},
	})
	if err != nil {
		t.Fatalf("SignalWorkflow failed: %v", err)
	}
	if !signalReceived {
		t.Error("Expected signal to be received by server")
	}
}

func TestConnectionHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte(`{"error":"internal server error"}`))
	}))
	defer server.Close()

	conn, err := NewConnection(server.URL, false)
	if err != nil {
		t.Fatalf("NewConnection failed: %v", err)
	}

	_, err = conn.StartWorkflow(context.Background(), &StartWorkflowRequest{
		WorkflowID:   "wf-fail",
		WorkflowType: "FailWorkflow",
	})
	if err == nil {
		t.Fatal("Expected error from HTTP 500")
	}
}

// ─── Client Tests (with mock server) ─────────────────────────────────────────

func newMockServer() *httptest.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	})

	mux.HandleFunc("/api/workflows", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			var req StartWorkflowRequest
			json.NewDecoder(r.Body).Decode(&req)
			json.NewEncoder(w).Encode(map[string]string{
				"workflowId": req.WorkflowID,
				"runId":      fmt.Sprintf("run-%s", req.WorkflowID),
			})
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"workflows": []map[string]interface{}{},
		})
	})

	mux.HandleFunc("/api/workflows/wf-1", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"workflowId": "wf-1",
			"runId":      "run-1",
			"status":     "COMPLETED",
			"result":     map[string]interface{}{"value": 42},
		})
	})

	mux.HandleFunc("/api/workflows/wf-1/signal", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	mux.HandleFunc("/api/workflows/wf-1/query", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{"result": "query-result"})
	})

	mux.HandleFunc("/api/workflows/wf-1/terminate", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]string{"status": "terminated"})
	})

	mux.HandleFunc("/api/workflows/wf-1/cancel", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]string{"status": "cancelled"})
	})

	mux.HandleFunc("/api/workflows/wf-1/update", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{"result": "updated"})
	})

	mux.HandleFunc("/api/workflows/wf-1/reset", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"runId": "run-reset-1"})
	})

	return httptest.NewServer(mux)
}

func TestClientStartWorkflow(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	client, err := NewClient(ClientOptions{HostPort: server.URL})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	exec, err := client.Start(context.Background(), WorkflowOptions{
		WorkflowID:   "wf-1",
		WorkflowType: "TestWorkflow",
		TaskQueue:    "test-queue",
	})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if exec.WorkflowID != "wf-1" {
		t.Errorf("Expected workflowId 'wf-1', got '%s'", exec.WorkflowID)
	}
}

func TestClientDescribeWorkflow(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	client, err := NewClient(ClientOptions{HostPort: server.URL})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	desc, err := client.Describe(context.Background(), "wf-1")
	if err != nil {
		t.Fatalf("Describe failed: %v", err)
	}
	if desc.Status != WorkflowStatusCompleted {
		t.Errorf("Expected COMPLETED, got '%s'", desc.Status)
	}
}

func TestClientSignalWorkflow(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	client, err := NewClient(ClientOptions{HostPort: server.URL})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	err = client.Signal(context.Background(), "wf-1", SignalOptions{
		SignalName: "test-signal",
		Args:       []interface{}{"data"},
	})
	if err != nil {
		t.Fatalf("Signal failed: %v", err)
	}
}

func TestClientQueryWorkflow(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	client, err := NewClient(ClientOptions{HostPort: server.URL})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	result, err := client.Query(context.Background(), "wf-1", QueryOptions{
		QueryType: "getStatus",
	})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result == nil {
		t.Error("Expected non-nil query result")
	}
}

func TestClientUpdateWorkflow(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	client, err := NewClient(ClientOptions{HostPort: server.URL})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	result, err := client.Update(context.Background(), "wf-1", UpdateOptions{
		UpdateName: "update-status",
		Args:       map[string]string{"status": "active"},
		WaitPolicy: "COMPLETED",
	})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if result.UpdateID == "" {
		t.Error("Expected non-empty update ID")
	}
}

func TestClientResetWorkflow(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	client, err := NewClient(ClientOptions{HostPort: server.URL})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	newRunID, err := client.Reset(context.Background(), "wf-1", ResetOptions{
		ResetEventID: 5,
		Reason:       "testing",
	})
	if err != nil {
		t.Fatalf("Reset failed: %v", err)
	}
	if newRunID != "run-reset-1" {
		t.Errorf("Expected 'run-reset-1', got '%s'", newRunID)
	}
}

func TestClientTerminateWorkflow(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	client, err := NewClient(ClientOptions{HostPort: server.URL})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	err = client.Terminate(context.Background(), "wf-1", "test termination")
	if err != nil {
		t.Fatalf("Terminate failed: %v", err)
	}
}

func TestClientCancelWorkflow(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	client, err := NewClient(ClientOptions{HostPort: server.URL})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	err = client.Cancel(context.Background(), "wf-1")
	if err != nil {
		t.Fatalf("Cancel failed: %v", err)
	}
}

func TestClientGetWorkflowHandle(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	client, err := NewClient(ClientOptions{HostPort: server.URL})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	handle := client.GetWorkflow("wf-1")
	if handle.GetWorkflowID() != "wf-1" {
		t.Errorf("Expected workflowId 'wf-1', got '%s'", handle.GetWorkflowID())
	}

	desc, err := handle.Describe(context.Background())
	if err != nil {
		t.Fatalf("Handle Describe failed: %v", err)
	}
	if desc.Status != WorkflowStatusCompleted {
		t.Errorf("Expected COMPLETED, got '%s'", desc.Status)
	}
}

// ─── Saga Tests ───────────────────────────────────────────────────────────────

func TestSaga(t *testing.T) {
	saga := NewSaga()
	stepOrder := []string{}

	saga.AddStep("step1",
		func(ctx context.Context) (interface{}, error) {
			stepOrder = append(stepOrder, "execute-1")
			return "result-1", nil
		},
		func(ctx context.Context) error {
			stepOrder = append(stepOrder, "compensate-1")
			return nil
		},
	)

	saga.AddStep("step2",
		func(ctx context.Context) (interface{}, error) {
			stepOrder = append(stepOrder, "execute-2")
			return "result-2", nil
		},
		func(ctx context.Context) error {
			stepOrder = append(stepOrder, "compensate-2")
			return nil
		},
	)

	results, err := saga.Execute(context.Background())
	if err != nil {
		t.Fatalf("Saga execution failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
	if len(stepOrder) != 2 || stepOrder[0] != "execute-1" || stepOrder[1] != "execute-2" {
		t.Errorf("Expected [execute-1, execute-2], got %v", stepOrder)
	}
}

func TestSagaCompensation(t *testing.T) {
	saga := NewSaga()
	compensated := []string{}

	saga.AddStep("step1",
		func(ctx context.Context) (interface{}, error) {
			return "ok", nil
		},
		func(ctx context.Context) error {
			compensated = append(compensated, "step1")
			return nil
		},
	)

	saga.AddStep("step2-fails",
		func(ctx context.Context) (interface{}, error) {
			return nil, fmt.Errorf("step2 failed")
		},
		func(ctx context.Context) error {
			compensated = append(compensated, "step2")
			return nil
		},
	)

	_, err := saga.Execute(context.Background())
	if err == nil {
		t.Fatal("Expected saga to fail")
	}

	if len(compensated) != 1 || compensated[0] != "step1" {
		t.Errorf("Expected [step1] compensated, got %v", compensated)
	}
}

// ─── Continue-as-New Tests ────────────────────────────────────────────────────

func TestContinueAsNew(t *testing.T) {
	err := NewContinueAsNewError(ContinueAsNewOptions{
		WorkflowType: "LongRunningWorkflow",
		TaskQueue:    "main",
		Input:        map[string]int{"iteration": 42},
	})
	if err == nil {
		t.Fatal("Expected non-nil error")
	}
	if err.Options.WorkflowType != "LongRunningWorkflow" {
		t.Errorf("Expected WorkflowType 'LongRunningWorkflow', got '%s'", err.Options.WorkflowType)
	}
	if err.Options.Input.(map[string]int)["iteration"] != 42 {
		t.Error("Expected iteration 42 in input")
	}
}

// ─── Worker Lifecycle Tests ───────────────────────────────────────────────────

func TestWorkerCreateAndStop(t *testing.T) {
	server := newMockServer()
	defer server.Close()

	ClearWorkflows()
	ClearActivities()
	defer ClearWorkflows()
	defer ClearActivities()

	worker, err := NewWorker(WorkerOptions{
		HostPort:  server.URL,
		TaskQueue: "test-queue",
	})
	if err != nil {
		t.Fatalf("NewWorker failed: %v", err)
	}

	if worker.GetTaskQueue() != "test-queue" {
		t.Errorf("Expected task queue 'test-queue', got '%s'", worker.GetTaskQueue())
	}
	if worker.IsRunning() {
		t.Error("Worker should not be running before Run()")
	}

	// Start worker in background
	done := make(chan error, 1)
	go func() {
		done <- worker.Run()
	}()

	time.Sleep(50 * time.Millisecond)
	if !worker.IsRunning() {
		t.Error("Worker should be running after Run()")
	}

	worker.Stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Worker.Run() returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Worker did not stop in time")
	}
}

func TestWorkerNoTaskQueue(t *testing.T) {
	_, err := NewWorker(WorkerOptions{
		HostPort: "localhost:5000",
	})
	if err == nil {
		t.Fatal("Expected error when task queue is empty")
	}
}

// ─── Multi-Step Workflow Test ─────────────────────────────────────────────────

func TestMultiStepWorkflowWithActivities(t *testing.T) {
	ClearWorkflows()
	ClearActivities()
	defer ClearWorkflows()
	defer ClearActivities()

	RegisterActivity("multiply", func(ctx *ActivityContext, input interface{}) (interface{}, error) {
		args := input.([]interface{})
		a := args[0].(float64)
		b := args[1].(float64)
		return a * b, nil
	})

	RegisterActivity("add", func(ctx *ActivityContext, input interface{}) (interface{}, error) {
		args := input.([]interface{})
		a := args[0].(float64)
		b := args[1].(float64)
		return a + b, nil
	})

	RegisterWorkflow("calculator-wf", func(ctx WorkflowContext, input interface{}) (interface{}, error) {
		// Step 1: multiply 6 * 7 = 42
		prod, err := ExecuteActivity(ctx, "multiply", []interface{}{6.0, 7.0})
		if err != nil {
			return nil, err
		}

		// Step 2: add 42 + 8 = 50
		sum, err := ExecuteActivity(ctx, "add", []interface{}{prod.(float64), 8.0})
		if err != nil {
			return nil, err
		}

		return sum, nil
	})

	worker := &Worker{
		taskQueue:       "test-queue",
		executions:      make(map[string]*workflowExecution),
		activityResults: make(map[string]chan activityResult),
		stopCh:          make(chan struct{}),
	}

	result, err := worker.ExecuteWorkflow("calc-1", "calculator-wf", nil)
	if err != nil {
		t.Fatalf("ExecuteWorkflow failed: %v", err)
	}
	if result != 50.0 {
		t.Errorf("Expected 50.0, got '%v'", result)
	}
}
