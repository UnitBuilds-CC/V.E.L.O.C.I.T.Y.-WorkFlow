package io.velocity;

/**
 * Options for creating a Client.
 */
public class ClientOptions {
    private String hostPort;
    private String namespace;
    private boolean useTls;

    public ClientOptions() {
        this.hostPort = "localhost:7233";
        this.namespace = "default";
        this.useTls = false;
    }

    // Builder pattern
    public ClientOptions setHostPort(String hostPort) {
        this.hostPort = hostPort;
        return this;
    }

    public ClientOptions setNamespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    public ClientOptions setUseTls(boolean useTls) {
        this.useTls = useTls;
        return this;
    }

    // Getters
    public String getHostPort() { return hostPort; }
    public String getNamespace() { return namespace; }
    public boolean isUseTls() { return useTls; }
}
