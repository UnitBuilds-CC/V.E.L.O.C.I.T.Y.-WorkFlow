/**
 * VELOCITY WorkFlow — WebSocket Real-Time Client
 * Provides channel-based subscriptions with auto-reconnect and message queuing
 */

class VelocityWebSocket {
    constructor() {
        this.ws = null;
        this.url = null;
        this.channels = new Map();          // channel -> Set<handler>
        this.messageQueue = [];             // offline message buffer
        this.reconnectAttempts = 0;
        this.maxReconnectDelay = 30000;
        this.baseReconnectDelay = 1000;
        this.reconnectTimer = null;
        this.connected = false;
        this.intentionalClose = false;
        this.onOpen = null;
        this.onClose = null;
        this.onError = null;
        this.onMessage = null;
        this.heartbeatInterval = null;
        this.heartbeatTimeout = 30000;
    }

    /**
     * Establish WebSocket connection with auto-reconnect
     */
    connect(url) {
        if (this.ws) this.disconnect();

        this.url = url || this._resolveUrl();
        this.intentionalClose = false;

        try {
            this.ws = new WebSocket(this.url);
        } catch (err) {
            console.error('[WS] Failed to create WebSocket:', err);
            this._scheduleReconnect();
            return;
        }

        this.ws.onopen = () => {
            this.connected = true;
            this.reconnectAttempts = 0;
            console.log('[WS] Connected to', this.url);
            this._startHeartbeat();
            this._flushQueue();
            this._sendSubscriptions();
            if (this.onOpen) this.onOpen();
        };

        this.ws.onmessage = (event) => {
            let msg;
            try {
                msg = JSON.parse(event.data);
            } catch {
                console.warn('[WS] Non-JSON message:', event.data);
                return;
            }

            // Global handler
            if (this.onMessage) this.onMessage(msg);

            // Dispatch to channel handlers
            const channel = msg.channel || msg.type || 'default';
            const handlers = this.channels.get(channel);
            if (handlers) {
                handlers.forEach(fn => {
                    try { fn(msg); } catch (e) { console.error('[WS] Handler error:', e); }
                });
            }

            // Also dispatch to wildcard listeners
            const wildcardHandlers = this.channels.get('*');
            if (wildcardHandlers) {
                wildcardHandlers.forEach(fn => {
                    try { fn(msg); } catch (e) { /* ignore */ }
                });
            }
        };

        this.ws.onclose = (event) => {
            this.connected = false;
            this._stopHeartbeat();
            console.log('[WS] Closed (code:', event.code, ')');
            if (this.onClose) this.onClose(event);
            if (!this.intentionalClose) this._scheduleReconnect();
        };

        this.ws.onerror = (err) => {
            console.error('[WS] Error:', err);
            if (this.onError) this.onError(err);
        };
    }

    /**
     * Graceful close
     */
    disconnect() {
        this.intentionalClose = true;
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
        this._stopHeartbeat();
        if (this.ws) {
            this.ws.close(1000, 'Client disconnect');
            this.ws = null;
        }
        this.connected = false;
        console.log('[WS] Disconnected');
    }

    /**
     * Subscribe to a channel
     */
    subscribe(channel, handler) {
        if (!this.channels.has(channel)) {
            this.channels.set(channel, new Set());
        }
        this.channels.get(channel).add(handler);

        // Notify server if already connected
        if (this.connected) {
            this._sendRaw({ action: 'subscribe', channel });
        }
        return handler;
    }

    /**
     * Unsubscribe from a channel
     */
    unsubscribe(channel) {
        this.channels.delete(channel);
        if (this.connected) {
            this._sendRaw({ action: 'unsubscribe', channel });
        }
    }

    /**
     * Send a message to the server
     */
    send(message) {
        if (this.connected) {
            this._sendRaw(message);
        } else {
            this.messageQueue.push(message);
            if (this.messageQueue.length > 500) {
                this.messageQueue.shift(); // drop oldest
            }
        }
    }

    /**
     * Check connection state
     */
    isConnected() {
        return this.connected;
    }

    /**
     * Get queued message count
     */
    getQueueLength() {
        return this.messageQueue.length;
    }

    // ─── Internal Methods ──────────────────────────────────────────────────────

    _sendRaw(message) {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(typeof message === 'string' ? message : JSON.stringify(message));
        }
    }

    _flushQueue() {
        while (this.messageQueue.length > 0) {
            const msg = this.messageQueue.shift();
            this._sendRaw(msg);
        }
    }

    _sendSubscriptions() {
        for (const [channel] of this.channels) {
            if (channel !== '*') {
                this._sendRaw({ action: 'subscribe', channel });
            }
        }
    }

    _scheduleReconnect() {
        if (this.intentionalClose) return;
        const delay = Math.min(
            this.baseReconnectDelay * Math.pow(2, this.reconnectAttempts),
            this.maxReconnectDelay
        );
        console.log(`[WS] Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts + 1})`);
        this.reconnectTimer = setTimeout(() => {
            this.reconnectAttempts++;
            this.connect(this.url);
        }, delay);
    }

    _startHeartbeat() {
        this._stopHeartbeat();
        this.heartbeatInterval = setInterval(() => {
            this._sendRaw({ action: 'ping' });
        }, this.heartbeatTimeout);
    }

    _stopHeartbeat() {
        if (this.heartbeatInterval) {
            clearInterval(this.heartbeatInterval);
            this.heartbeatInterval = null;
        }
    }

    _resolveUrl() {
        const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
        return `${proto}//${location.host}/ws`;
    }
}

// ─── Channel Constants ────────────────────────────────────────────────────────
const WS_CHANNELS = {
    WORKFLOW_EVENTS: 'workflow-events',
    METRICS: 'metrics',
    LOGS: 'logs',
    NOTIFICATIONS: 'notifications'
};

// ─── Global Instance ──────────────────────────────────────────────────────────
const velocityWS = new VelocityWebSocket();

if (typeof window !== 'undefined') {
    window.VelocityWebSocket = VelocityWebSocket;
    window.WS_CHANNELS = WS_CHANNELS;
    window.velocityWS = velocityWS;
}
