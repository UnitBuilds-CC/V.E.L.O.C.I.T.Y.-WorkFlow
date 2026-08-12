package io.velocity;

import java.util.Map;
import java.util.HashMap;

/**
 * Represents a history event.
 */
public class HistoryEvent {
    private final int eventId;
    private final String eventType;
    private final long eventTime;
    private int taskId;
    private Map<String, Object> attributes;

    public HistoryEvent(int eventId, String eventType, long eventTime) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.eventTime = eventTime;
        this.taskId = 0;
        this.attributes = new HashMap<>();
    }

    // Getters
    public int getEventId() { return eventId; }
    public String getEventType() { return eventType; }
    public long getEventTime() { return eventTime; }
    public int getTaskId() { return taskId; }
    public Map<String, Object> getAttributes() { return attributes; }

    // Setters
    public void setTaskId(int taskId) { this.taskId = taskId; }
    public void setAttributes(Map<String, Object> attributes) { this.attributes = attributes; }
}
