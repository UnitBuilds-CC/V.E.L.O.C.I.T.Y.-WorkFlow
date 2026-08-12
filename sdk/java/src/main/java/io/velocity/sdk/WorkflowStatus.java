package io.velocity.sdk;

/**
 * Workflow execution status values matching the server-side WorkflowStatus enum.
 */
public enum WorkflowStatus {
    RUNNING(0),
    COMPLETED(1),
    FAILED(2),
    CANCELED(3),
    TERMINATED(4),
    CONTINUED_AS_NEW(5);

    private final int value;

    WorkflowStatus(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static WorkflowStatus fromValue(int value) {
        for (WorkflowStatus s : values()) {
            if (s.value == value) return s;
        }
        return RUNNING;
    }
}
