/**
 * VELOCITY WorkFlow - Shared JavaScript Module
 * Provides API client, SSE, theme management, and utility functions
 */

// ─── VelocityAPI: REST Client ───────────────────────────────────────────────────
class VelocityAPI {
    constructor(baseUrl = '') {
        this.baseUrl = baseUrl;
    }

    async request(method, path, data = null) {
        const opts = {
            method,
            headers: { 'Content-Type': 'application/json' }
        };
        if (data) opts.body = JSON.stringify(data);
        
        const res = await fetch(`${this.baseUrl}${path}`, opts);
        if (!res.ok) {
            const err = await res.json().catch(() => ({ error: res.statusText }));
            throw new Error(err.error || `HTTP ${res.status}`);
        }
        return res.json();
    }

    // Dashboard & Health
    async getStats() {
        return this.request('GET', '/api/stats');
    }

    async getHealth() {
        return this.request('GET', '/api/health');
    }

    // Workflows
    async getWorkflows(filter = {}) {
        const params = new URLSearchParams();
        if (filter.status) params.set('status', filter.status);
        if (filter.namespace) params.set('namespace', filter.namespace);
        if (filter.type) params.set('type', filter.type);
        if (filter.limit) params.set('limit', filter.limit);
        const qs = params.toString() ? `?${params.toString()}` : '';
        return this.request('GET', `/api/workflows${qs}`);
    }

    async getWorkflow(id) {
        return this.request('GET', `/api/workflows/${id}`);
    }

    async getEvents(id, startEventId = null, limit = null) {
        const params = new URLSearchParams();
        if (startEventId) params.set('startEventId', startEventId);
        if (limit) params.set('limit', limit);
        const qs = params.toString() ? `?${params.toString()}` : '';
        return this.request('GET', `/api/workflows/${id}/events${qs}`);
    }

    async getSignals(id) {
        return this.request('GET', `/api/workflows/${id}/signals`);
    }

    async getQueries(id) {
        return this.request('GET', `/api/workflows/${id}/queries`);
    }

    // Workflow Actions
    async signalWorkflow(id, signalName, payload = null) {
        return this.request('POST', `/api/workflows/${id}/signal`, {
            signal_name: signalName,
            payload: payload
        });
    }

    async queryWorkflow(id, queryName) {
        return this.request('POST', `/api/workflows/${id}/query`, {
            query_name: queryName
        });
    }

    async cancelWorkflow(id) {
        return this.request('POST', `/api/workflows/${id}/cancel`);
    }

    async terminateWorkflow(id) {
        return this.request('POST', `/api/workflows/${id}/terminate`);
    }

    // Namespaces
    async getNamespaces() {
        return this.request('GET', '/api/namespaces');
    }

    async createNamespace(name, description = '', retentionDays = 7) {
        return this.request('POST', '/api/namespaces', {
            name,
            description,
            retention_days: retentionDays
        });
    }

    // Task Queues
    async getTaskQueues() {
        return this.request('GET', '/api/taskqueues');
    }

    // Schedules
    async getSchedules() {
        return this.request('GET', '/api/schedules');
    }

    async createSchedule(scheduleData) {
        return this.request('POST', '/api/schedules', scheduleData);
    }

    async pauseSchedule(id) {
        return this.request('POST', `/api/schedules/${id}/pause`);
    }

    async resumeSchedule(id) {
        return this.request('POST', `/api/schedules/${id}/resume`);
    }

    async deleteSchedule(id) {
        return this.request('DELETE', `/api/schedules/${id}`);
    }
}

// ─── VelocitySSE: Server-Sent Events Client ─────────────────────────────────────
class VelocitySSE {
    constructor() {
        this.eventSource = null;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 10;
        this.reconnectDelay = 1000;
        this.handlers = new Map();
        this.connected = false;
    }

    connect(url = '/api/events', handler = null) {
        if (this.eventSource) {
            this.disconnect();
        }

        if (handler) {
            this.handlers.set('message', handler);
        }

        this.eventSource = new EventSource(url);

        this.eventSource.onopen = () => {
            this.connected = true;
            this.reconnectAttempts = 0;
            console.log('[SSE] Connected');
            const connectHandler = this.handlers.get('connect');
            if (connectHandler) connectHandler();
        };

        this.eventSource.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                const messageHandler = this.handlers.get('message');
                if (messageHandler) messageHandler(data);
            } catch (err) {
                console.error('[SSE] Parse error:', err);
            }
        };

        this.eventSource.onerror = (err) => {
            console.error('[SSE] Error:', err);
            this.connected = false;
            const errorHandler = this.handlers.get('error');
            if (errorHandler) errorHandler(err);

            // Auto-reconnect with exponential backoff
            if (this.reconnectAttempts < this.maxReconnectAttempts) {
                const delay = this.reconnectDelay * Math.pow(2, this.reconnectAttempts);
                console.log(`[SSE] Reconnecting in ${delay}ms...`);
                setTimeout(() => {
                    this.reconnectAttempts++;
                    this.connect(url);
                }, delay);
            }
        };
    }

    disconnect() {
        if (this.eventSource) {
            this.eventSource.close();
            this.eventSource = null;
            this.connected = false;
            console.log('[SSE] Disconnected');
        }
    }

    on(event, handler) {
        this.handlers.set(event, handler);
    }

    isConnected() {
        return this.connected;
    }
}

// ─── VelocityTheme: Theme Management ────────────────────────────────────────────
class VelocityTheme {
    constructor() {
        this.storageKey = 'velocity-theme';
        this.defaultTheme = 'dark';
    }

    get() {
        return localStorage.getItem(this.storageKey) || this.defaultTheme;
    }

    set(theme) {
        localStorage.setItem(this.storageKey, theme);
        document.documentElement.setAttribute('data-theme', theme);
    }

    toggle() {
        const current = this.get();
        const next = current === 'dark' ? 'light' : 'dark';
        this.set(next);
        return next;
    }

    init() {
        const theme = this.get();
        document.documentElement.setAttribute('data-theme', theme);
        return theme;
    }
}

// ─── VelocityUtils: Utility Functions ───────────────────────────────────────────
const VelocityUtils = {
    /**
     * Format timestamp (milliseconds) to ISO string
     */
    formatTimestamp(ms) {
        if (!ms) return '—';
        return new Date(ms).toISOString();
    },

    /**
     * Format duration in milliseconds to human-readable string
     */
    formatDuration(ms) {
        if (!ms || ms < 0) return '—';
        
        const seconds = Math.floor(ms / 1000);
        const minutes = Math.floor(seconds / 60);
        const hours = Math.floor(minutes / 60);
        const days = Math.floor(hours / 24);

        if (days > 0) return `${days}d ${hours % 24}h`;
        if (hours > 0) return `${hours}h ${minutes % 60}m`;
        if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
        if (seconds > 0) return `${seconds}s`;
        return `${ms}ms`;
    },

    /**
     * Format bytes to human-readable string
     */
    formatBytes(bytes) {
        if (!bytes || bytes < 0) return '—';
        
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        let unitIndex = 0;
        let value = bytes;

        while (value >= 1024 && unitIndex < units.length - 1) {
            value /= 1024;
            unitIndex++;
        }

        return `${value.toFixed(2)} ${units[unitIndex]}`;
    },

    /**
     * Create status badge HTML
     */
    statusBadge(status) {
        const statusLower = status.toLowerCase();
        return `<span class="badge badge-${statusLower}">${status}</span>`;
    },

    /**
     * Copy text to clipboard
     */
    async copyToClipboard(text) {
        try {
            await navigator.clipboard.writeText(text);
            return true;
        } catch (err) {
            console.error('Copy failed:', err);
            return false;
        }
    },

    /**
     * Debounce function
     */
    debounce(fn, delay = 300) {
        let timeoutId;
        return function(...args) {
            clearTimeout(timeoutId);
            timeoutId = setTimeout(() => fn.apply(this, args), delay);
        };
    },

    /**
     * Throttle function
     */
    throttle(fn, limit = 300) {
        let inThrottle;
        return function(...args) {
            if (!inThrottle) {
                fn.apply(this, args);
                inThrottle = true;
                setTimeout(() => inThrottle = false, limit);
            }
        };
    },

    /**
     * Parse cron expression to human-readable string
     */
    parseCron(expression) {
        const parts = expression.trim().split(/\s+/);
        if (parts.length !== 5) return 'Invalid cron expression';

        const [minute, hour, dayOfMonth, month, dayOfWeek] = parts;
        const descriptions = [];

        // Minute
        if (minute === '*') descriptions.push('every minute');
        else if (minute.startsWith('*/')) descriptions.push(`every ${minute.slice(2)} minutes`);
        else descriptions.push(`at minute ${minute}`);

        // Hour
        if (hour === '*') descriptions.push('of every hour');
        else if (hour.startsWith('*/')) descriptions.push(`every ${hour.slice(2)} hours`);
        else descriptions.push(`at ${hour}:00`);

        // Day of month
        if (dayOfMonth !== '*') descriptions.push(`on day ${dayOfMonth}`);

        // Month
        if (month !== '*') descriptions.push(`in month ${month}`);

        // Day of week
        const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
        if (dayOfWeek !== '*') {
            const dayName = dayNames[parseInt(dayOfWeek)] || dayOfWeek;
            descriptions.push(`on ${dayName}`);
        }

        return descriptions.join(', ');
    },

    /**
     * Export data as JSON file
     */
    exportJSON(data, filename = 'export.json') {
        const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        a.click();
        URL.revokeObjectURL(url);
    },

    /**
     * Export data as CSV file
     */
    exportCSV(data, filename = 'export.csv') {
        if (!data || data.length === 0) return;

        const headers = Object.keys(data[0]);
        const csv = [
            headers.join(','),
            ...data.map(row => 
                headers.map(header => {
                    const value = row[header];
                    const escaped = String(value || '').replace(/"/g, '""');
                    return `"${escaped}"`;
                }).join(',')
            )
        ].join('\n');

        const blob = new Blob([csv], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        a.click();
        URL.revokeObjectURL(url);
    }
};

// ─── Initialize Global Instances ────────────────────────────────────────────────
const velocityAPI = new VelocityAPI();
const velocitySSE = new VelocitySSE();
const velocityTheme = new VelocityTheme();

// Make available globally
if (typeof window !== 'undefined') {
    window.VelocityAPI = VelocityAPI;
    window.VelocitySSE = VelocitySSE;
    window.VelocityTheme = VelocityTheme;
    window.VelocityUtils = VelocityUtils;
    window.velocityAPI = velocityAPI;
    window.velocitySSE = velocitySSE;
    window.velocityTheme = velocityTheme;
}
