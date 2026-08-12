// Example: Simple workflow worker using the VELOCITY-WorkFlow Go SDK.
//
// This demonstrates that the VELOCITY-WorkFlow gRPC API is language-agnostic.
// The same workflow engine serves Go, Python, C#, Java, TypeScript, or any gRPC client.
//
// Prerequisites:
//   1. Start the VELOCITY-WorkFlow server:
//      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server
//      dotnet run
//
//   2. Generate gRPC stubs:
//      cd VELOCITY-WorkFlow/sdk/go
//      protoc -I../../src/Velocity.Workflow.Server/Protos \
//          --go_out=velocity_sdk --go-grpc_out=velocity_sdk \
//          ../../src/Velocity.Workflow.Server/Protos/workflow_service.proto
//
//   3. Run this example:
//      go run examples/simple_worker.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	velocity_sdk "github.com/velocity-workflow/sdk/go/velocity_sdk"
)

func main() {
	fmt.Println("=== VELOCITY-WorkFlow Go SDK Example ===")
	fmt.Println()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Connect to the server (no JWT = anonymous access)
	client, err := velocity_sdk.NewClient("localhost:50051", "")
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	fmt.Printf("Connected to: %s\n", client.Target())

	// Verify connectivity
	if err := client.Ping(ctx); err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Println("Server ping: OK")

	// In a full implementation, you would:
	// 1. Start a workflow
	// 2. Describe the workflow
	// 3. Send signals
	// 4. Complete/fail/cancel the workflow
	// 5. Query the final state

	fmt.Println()
	fmt.Println("=== Go SDK connected successfully! ===")
	fmt.Println("The Go SDK can communicate with the Rust/C# workflow engine via gRPC.")
}
