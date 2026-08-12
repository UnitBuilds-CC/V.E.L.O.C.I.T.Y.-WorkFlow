// Example: Simple task worker using the VELOCITY-WorkFlow Go SDK.
//
// Demonstrates:
//   - Worker registration with a task queue
//   - Polling for tasks in a loop
//   - Executing task logic via registered handlers
//   - Error handling with typed errors
//   - Signal handling for graceful shutdown (SIGINT / SIGTERM)
//
// Prerequisites:
//   1. Start the VELOCITY-WorkFlow server:
//      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
//
//   2. Build the SDK:
//      cd VELOCITY-WorkFlow/sdk/go && go build ./...
//
//   3. Run this worker:
//      go run examples/simple_worker.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	velocity_sdk "github.com/velocity-workflow/sdk/go/velocity_sdk"
)

// ── Configuration ────────────────────────────────────────────────────────

const (
	serverAddr   = "localhost:50051"
	taskQueue    = "orders"
	pollInterval = 1 * time.Second
)

// ── Task handler ─────────────────────────────────────────────────────────

type taskHandler func(ctx context.Context, input json.RawMessage) (interface{}, error)

func processOrder(ctx context.Context, input json.RawMessage) (interface{}, error) {
	var payload struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(input, &payload); err != nil {
		return nil, fmt.Errorf("unmarshal input: %w", err)
	}
	log.Printf("[worker] Processing order %s", payload.OrderID)

	// Simulate work
	select {
	case <-time.After(50 * time.Millisecond):
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return map[string]interface{}{
		"status":   "shipped",
		"order_id": payload.OrderID,
	}, nil
}

var handlers = map[string]taskHandler{
	"order-processing": processOrder,
}

// ── Worker loop ──────────────────────────────────────────────────────────

func main() {
	log.Println("[worker] Starting VELOCITY-WorkFlow Go worker")
	log.Printf("[worker] Server: %s | Queue: %s", serverAddr, taskQueue)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("[worker] Received signal %s — shutting down...", sig)
		cancel()
	}()

	// Connect to the server
	client, err := velocity_sdk.NewClient(serverAddr, "")
	if err != nil {
		log.Fatalf("[worker] Failed to connect: %v", err)
	}
	defer client.Close()

	log.Printf("[worker] Registered on task queue '%s'", taskQueue)

	// Poll loop
	for {
		select {
		case <-ctx.Done():
			log.Println("[worker] Shut down cleanly")
			return
		default:
		}

		task, err := client.PollTask(ctx, taskQueue, 2*time.Second)
		if err != nil {
			log.Printf("[worker] Poll error: %v", err)
			select {
			case <-time.After(pollInterval):
			case <-ctx.Done():
				return
			}
			continue
		}

		if task == nil {
			select {
			case <-time.After(pollInterval):
			case <-ctx.Done():
				return
			}
			continue
		}

		handler, ok := handlers[task.WorkflowType]
		if !ok {
			log.Printf("[worker] No handler for task type '%s' — failing task", task.WorkflowType)
			_ = client.FailTask(ctx, task.WorkflowKey, fmt.Sprintf("no handler for %s", task.WorkflowType))
			continue
		}

		result, err := handler(ctx, task.Input)
		if err != nil {
			log.Printf("[worker] Task execution error: %v", err)
			_ = client.FailTask(ctx, task.WorkflowKey, err.Error())
			continue
		}

		resultBytes, _ := json.Marshal(result)
		if err := client.CompleteWorkflow(ctx, task.WorkflowKey, resultBytes); err != nil {
			log.Printf("[worker] Failed to complete workflow: %v", err)
			continue
		}
		log.Printf("[worker] Task '%s' completed successfully", task.WorkflowType)
	}
}
