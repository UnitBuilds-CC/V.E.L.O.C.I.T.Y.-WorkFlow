package io.velocity;

/**
 * Options for starting a batch operation.
 */
public class BatchOperationOptions {
    private String operation;
    private String query;
    private String signalName = "";
    private Object signalInput;
    private String reason = "";

    public BatchOperationOptions(String operation, String query) {
        this.operation = operation;
        this.query = query;
    }

    public BatchOperationOptions setSignalName(String signalName) { this.signalName = signalName; return this; }
    public BatchOperationOptions setSignalInput(Object signalInput) { this.signalInput = signalInput; return this; }
    public BatchOperationOptions setReason(String reason) { this.reason = reason; return this; }

    public String getOperation() { return operation; }
    public String getQuery() { return query; }
    public String getSignalName() { return signalName; }
    public Object getSignalInput() { return signalInput; }
    public String getReason() { return reason; }
}
