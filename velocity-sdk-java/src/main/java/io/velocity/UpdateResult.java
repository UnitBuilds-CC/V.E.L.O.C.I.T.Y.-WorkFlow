package io.velocity;

/**
 * Result of a workflow update.
 */
public class UpdateResult {
    private final String updateId;
    private final String status;
    private final Object result;

    public UpdateResult(String updateId, String status, Object result) {
        this.updateId = updateId;
        this.status = status;
        this.result = result;
    }

    public String getUpdateId() { return updateId; }
    public String getStatus() { return status; }
    public Object getResult() { return result; }
}
