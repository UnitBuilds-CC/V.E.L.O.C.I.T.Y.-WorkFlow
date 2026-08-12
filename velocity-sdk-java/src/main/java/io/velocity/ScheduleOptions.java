package io.velocity;

/**
 * Options for creating a schedule.
 */
public class ScheduleOptions {
    private String scheduleId;
    private String workflowType;
    private String taskQueue;
    private String cronSchedule;
    private Object input;
    private boolean enabled = true;

    public ScheduleOptions(String scheduleId, String workflowType, String taskQueue, String cronSchedule) {
        this.scheduleId = scheduleId;
        this.workflowType = workflowType;
        this.taskQueue = taskQueue;
        this.cronSchedule = cronSchedule;
    }

    public ScheduleOptions setInput(Object input) { this.input = input; return this; }
    public ScheduleOptions setEnabled(boolean enabled) { this.enabled = enabled; return this; }

    public String getScheduleId() { return scheduleId; }
    public String getWorkflowType() { return workflowType; }
    public String getTaskQueue() { return taskQueue; }
    public String getCronSchedule() { return cronSchedule; }
    public Object getInput() { return input; }
    public boolean isEnabled() { return enabled; }
}
