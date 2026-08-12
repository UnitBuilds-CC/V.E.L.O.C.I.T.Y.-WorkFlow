package io.velocity;

/**
 * Options for resetting a workflow.
 */
public class ResetOptions {
    private final long resetEventId;
    private String reason = "";

    public ResetOptions(long resetEventId) {
        this.resetEventId = resetEventId;
    }

    public ResetOptions setReason(String reason) { this.reason = reason; return this; }
    public long getResetEventId() { return resetEventId; }
    public String getReason() { return reason; }
}
