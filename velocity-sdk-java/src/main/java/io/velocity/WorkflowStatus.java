package io.velocity;

/**
 * Workflow execution status.
 */
public enum WorkflowStatus {
    RUNNING(0),
    COMPLETED(1),
    FAILED(2),
    CANCELED(3),
    TERMINATED(4),
    CONTINUED_AS_NEW(5),
    TIMED_OUT(6);

    private final int value;

    WorkflowStatus(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static WorkflowStatus fromValue(int value) {
        for (WorkflowStatus status : values()) {
            if (status.value == value) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown workflow status: " + value);
    }
}
