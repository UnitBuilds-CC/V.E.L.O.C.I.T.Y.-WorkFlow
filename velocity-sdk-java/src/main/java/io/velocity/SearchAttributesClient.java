package io.velocity;

import java.util.*;

/**
 * Client for search attribute operations.
 */
public class SearchAttributesClient {
    private final String namespace;

    public SearchAttributesClient(String namespace) {
        this.namespace = namespace;
    }

    public void upsert(String workflowId, Map<String, Object> attributes) {}

    public List<Map<String, Object>> listWorkflows(String query) {
        return List.of();
    }

    public long countWorkflows(String query) {
        return 0;
    }
}
