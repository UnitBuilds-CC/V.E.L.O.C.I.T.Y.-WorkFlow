package velocity_sdk

import (
	"testing"
)

func TestNewClient(t *testing.T) {
	client, err := NewClient("localhost:50051", "")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	if client.Target() != "localhost:50051" {
		t.Errorf("expected target localhost:50051, got %s", client.Target())
	}
}

func TestWorkflowStatusString(t *testing.T) {
	tests := []struct {
		status WorkflowStatus
		want   string
	}{
		{StatusRunning, "Running"},
		{StatusCompleted, "Completed"},
		{StatusFailed, "Failed"},
		{StatusCanceled, "Canceled"},
		{StatusTerminated, "Terminated"},
		{StatusUnknown, "Unknown"},
	}
	for _, tt := range tests {
		if got := tt.status.String(); got != tt.want {
			t.Errorf("WorkflowStatus(%d).String() = %q, want %q", tt.status, got, tt.want)
		}
	}
}

func TestClientWithJWT(t *testing.T) {
	client, err := NewClient("localhost:50051", "test-jwt-token")
	if err != nil {
		t.Fatalf("NewClient with JWT failed: %v", err)
	}
	defer client.Close()

	if client.jwt != "test-jwt-token" {
		t.Errorf("expected jwt 'test-jwt-token', got %q", client.jwt)
	}
}
