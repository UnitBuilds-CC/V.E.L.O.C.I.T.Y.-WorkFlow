package io.velocity;

/**
 * Options for updating a workflow.
 */
public class UpdateOptions {
    private String updateName;
    private Object args;
    private String waitPolicy = "COMPLETED";

    public UpdateOptions(String updateName) {
        this.updateName = updateName;
    }

    public UpdateOptions setArgs(Object args) { this.args = args; return this; }
    public UpdateOptions setWaitPolicy(String waitPolicy) { this.waitPolicy = waitPolicy; return this; }

    public String getUpdateName() { return updateName; }
    public Object getArgs() { return args; }
    public String getWaitPolicy() { return waitPolicy; }
}
