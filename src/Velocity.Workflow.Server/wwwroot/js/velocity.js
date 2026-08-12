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

// ─── VelocityWSHelper: WebSocket Integration Utilities ────────────────────────
const VelocityWSHelper = {
    /**
     * Connect to WebSocket and auto-subscribe to standard channels
     */
    connectRealtime(callbacks = {}) {
        if (typeof velocityWS === 'undefined') {
            console.warn('[WSHelper] velocityWS not loaded');
            return;
        }
        if (callbacks.onWorkflowEvent) {
            velocityWS.subscribe('workflow-events', callbacks.onWorkflowEvent);
        }
        if (callbacks.onMetrics) {
            velocityWS.subscribe('metrics', callbacks.onMetrics);
        }
        if (callbacks.onLogs) {
            velocityWS.subscribe('logs', callbacks.onLogs);
        }
        if (callbacks.onNotification) {
            velocityWS.subscribe('notifications', callbacks.onNotification);
        }
        velocityWS.connect();
        return velocityWS;
    },

    /**
     * Format a WebSocket message for display
     */
    formatMessage(msg) {
        const time = new Date(msg.timestamp || Date.now()).toLocaleTimeString();
        const channel = msg.channel || msg.type || 'unknown';
        const detail = msg.message || msg.event || JSON.stringify(msg.data || msg.payload || {});
        return { time, channel, detail: String(detail).slice(0, 200) };
    },

    /**
     * Create a connection status updater
     */
    bindConnectionStatus(dotElement, textElement) {
        const update = () => {
            if (!velocityWS) return;
            const connected = velocityWS.isConnected();
            if (dotElement) {
                dotElement.className = connected ? 'conn-dot connected' : 'conn-dot';
            }
            if (textElement) {
                textElement.textContent = connected ? 'Connected' : 'Disconnected';
            }
        };
        setInterval(update, 2000);
        update();
    }
};

// ─── VelocityDesigner: Workflow Designer Helpers ──────────────────────────────
const VelocityDesigner = {
    STEP_TYPES: ['start', 'task', 'decision', 'parallel', 'wait', 'end'],

    STEP_COLORS: {
        start: '#3fb950', task: '#58a6ff', decision: '#d29922',
        parallel: '#bc8cff', wait: '#e3872d', end: '#f85149'
    },

    /**
     * Create a blank workflow definition
     */
    createBlank(name = 'Untitled Workflow') {
        return {
            name,
            version: 1,
            steps: [],
            connections: [],
            metadata: { created: new Date().toISOString(), modified: new Date().toISOString() }
        };
    },

    /**
     * Validate a workflow definition
     */
    validate(definition) {
        const errors = [];
        if (!definition.steps || definition.steps.length === 0) {
            errors.push('Workflow must have at least one step');
        }
        const hasStart = definition.steps.some(s => s.type === 'start');
        const hasEnd = definition.steps.some(s => s.type === 'end');
        if (!hasStart) errors.push('Workflow must have a Start step');
        if (!hasEnd) errors.push('Workflow must have an End step');

        // Check for orphan steps (no connections)
        const connected = new Set();
        (definition.connections || []).forEach(c => { connected.add(c.from); connected.add(c.to); });
        definition.steps.forEach(s => {
            if (definition.steps.length > 1 && !connected.has(s.id)) {
                errors.push(`Step "${s.name || s.id}" is not connected`);
            }
        });
        return errors;
    },

    /**
     * Generate a step ID
     */
    generateStepId() {
        return 'step_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 6);
    },

    /**
     * Convert designer definition to execution format
     */
    toExecutionFormat(definition) {
        return {
            name: definition.name,
            tasks: definition.steps
                .filter(s => s.type !== 'start' && s.type !== 'end')
                .map(s => ({
                    name: s.name,
                    type: s.type === 'decision' ? 'decision' : s.type === 'parallel' ? 'parallel' : 'activity',
                    timeout: s.timeout || 30,
                    retry_policy: { type: s.retry || 'none', max_attempts: s.maxRetries || 3 },
                    input_mapping: s.input || {}
                })),
            flow: (definition.connections || []).map(c => ({
                from: (definition.steps.find(s => s.id === c.from) || {}).name || c.from,
                to: (definition.steps.find(s => s.id === c.to) || {}).name || c.to
            }))
        };
    }
};

// ─── VelocityTimeline: Event Timeline Rendering Utilities ─────────────────────
const VelocityTimeline = {
    EVENT_COLORS: {
        started: 'var(--accent-green)', completed: 'var(--accent-blue)',
        failed: 'var(--accent-red)', signaled: 'var(--accent-purple)',
        queried: 'var(--accent-cyan)', cancelled: 'var(--accent-yellow)',
        timerfired: 'var(--accent-orange)', childworkflowstarted: 'var(--accent-purple)'
    },

    /**
     * Classify an event type string
     */
    classify(eventType) {
        const t = (eventType || '').toLowerCase();
        if (t.includes('start')) return 'started';
        if (t.includes('complete')) return 'completed';
        if (t.includes('fail') || t.includes('error')) return 'failed';
        if (t.includes('signal')) return 'signaled';
        if (t.includes('quer')) return 'queried';
        if (t.includes('cancel')) return 'cancelled';
        if (t.includes('timer')) return 'timerfired';
        if (t.includes('child')) return 'childworkflowstarted';
        return 'started';
    },

    /**
     * Format event type for display
     */
    formatType(eventType) {
        return (eventType || 'Unknown')
            .replace(/([A-Z])/g, ' $1')
            .replace(/^./, s => s.toUpperCase())
            .trim();
    },

    /**
     * Create timeline HTML for a set of events
     */
    renderEvents(events) {
        if (!events || events.length === 0) {
            return '<div style="text-align:center;padding:2rem;color:var(--text-muted)">No events to display</div>';
        }
        return events.map(evt => {
            const cls = this.classify(evt.event_type || evt.type);
            const color = this.EVENT_COLORS[cls] || 'var(--accent-blue)';
            const time = new Date(evt.timestamp || Date.now()).toLocaleTimeString();
            const summary = evt.summary || evt.event_type || evt.type || '';
            return `<div class="timeline-item"><div class="timeline-dot ${cls}"></div><div class="timeline-card"><div class="timeline-card-header"><span class="timeline-event-type" style="color:${color}">${this.formatType(evt.event_type || evt.type)}</span><span class="timeline-time">${time}</span></div><div class="timeline-summary">${summary}</div></div></div>`;
        }).join('');
    },

    /**
     * Group events by type and return counts
     */
    groupByType(events) {
        const groups = {};
        (events || []).forEach(evt => {
            const cls = this.classify(evt.event_type || evt.type);
            groups[cls] = (groups[cls] || 0) + 1;
        });
        return groups;
    },

    /**
     * Calculate total duration from first to last event
     */
    totalDuration(events) {
        if (!events || events.length < 2) return 0;
        const times = events.map(e => e.timestamp || 0).filter(t => t > 0);
        if (times.length < 2) return 0;
        return Math.max(...times) - Math.min(...times);
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
    window.VelocityWSHelper = VelocityWSHelper;
    window.VelocityDesigner = VelocityDesigner;
    window.VelocityTimeline = VelocityTimeline;
    window.velocityAPI = velocityAPI;
    window.velocitySSE = velocitySSE;
    window.velocityTheme = velocityTheme;
}
