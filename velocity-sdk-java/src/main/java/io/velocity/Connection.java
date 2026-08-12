package io.velocity;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.TimeUnit;

/**
 * Manages gRPC connection to the V.E.L.O.C.I.T.Y.-WorkFlow server.
 */
public class Connection {
    private final String hostPort;
    private final boolean useTls;
    private ManagedChannel channel;
    private boolean connected;

    public Connection(String hostPort, boolean useTls) {
        this.hostPort = hostPort;
        this.useTls = useTls;
        this.connected = false;
    }

    /**
     * Establish connection to the server.
     */
    public void connect() {
        if (connected) {
            return;
        }

        if (useTls) {
            channel = ManagedChannelBuilder.forTarget(hostPort).build();
        } else {
            channel = ManagedChannelBuilder.forTarget(hostPort).usePlaintext().build();
        }

        connected = true;
    }

    /**
     * Close the connection.
     */
    public void close() {
        if (channel != null) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
            connected = false;
        }
    }

    /**
     * Check if connected.
     */
    public boolean isConnected() {
        return connected && channel != null && !channel.isShutdown();
    }

    /**
     * Get the underlying gRPC channel.
     */
    public ManagedChannel getChannel() {
        return channel;
    }
}
