/**
 * HTTP Transport for Velocity Embedded SDK
 * Connects to the Velocity Engine REST API using DBOS-compatible patterns
 */

export interface EmbeddedRemoteConfig {
  baseUrl: string;
  timeoutMs?: number;
  headers?: Record<string, string>;
}

export interface RemoteWorkflowHandle<T = any> {
  workflowId: string;
  getStatus(): Promise<WorkflowStatusResult>;
  getResult(): Promise<T>;
}

export interface WorkflowStatusResult {
  status: 'PENDING' | 'RUNNING' | 'COMPLETED' | 'FAILED' | 'CANCELLED' | 'TERMINATED';
  workflowId: string;
  workflowName: string;
}

export class EmbeddedRemoteClient {
  private _baseUrl: string;
  private _timeoutMs: number;
  private _headers: Record<string, string>;

  constructor(config: EmbeddedRemoteConfig) {
    this._baseUrl = config.baseUrl.replace(/\/$/, '');
    this._timeoutMs = config.timeoutMs ?? 30000;
    this._headers = {
      'Content-Type': 'application/json',
      ...config.headers,
    };
  }

  private async _request<T>(method: string, path: string, body?: any): Promise<T> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this._timeoutMs);
    try {
      const res = await fetch(`${this._baseUrl}${path}`, {
        method,
        headers: this._headers,
        body: body ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(`HTTP ${res.status}: ${text}`);
      }
      const text = await res.text();
      if (!text) return {} as T;
      return JSON.parse(text) as T;
    } finally {
      clearTimeout(timer);
    }
  }

  /**
   * Start a workflow and return a handle to poll for results.
   * Maps to the engine's POST /api/workflows endpoint.
   */
  async startWorkflow<T = any>(
    workflowName: string,
    workflowId: string,
    ...args: any[]
  ): Promise<RemoteWorkflowHandle<T>> {
    const result = await this._request<{ workflow_id: string; run_id: string }>('POST', '/api/workflows', {
      workflow_type: workflowName,
      workflow_id: workflowId,
      task_queue: 'default',
      input: args,
    });

    const wfId = result.workflow_id || workflowId;
    return this._createHandle<T>(wfId, workflowName);
  }

  /**
   * Resume an existing workflow handle by ID.
   */
  retrieveWorkflow<T = any>(workflowId: string, workflowName = 'unknown'): RemoteWorkflowHandle<T> {
    return this._createHandle<T>(workflowId, workflowName);
  }

  /**
   * List workflows with optional status filter.
   */
  async listWorkflows(filter?: { status?: string; limit?: number }): Promise<any[]> {
    const params = new URLSearchParams();
    if (filter?.status) params.append('status', filter.status);
    if (filter?.limit) params.append('limit', filter.limit.toString());
    const qs = params.toString() ? `?${params.toString()}` : '';
    return this._request<any[]>('GET', `/api/workflows${qs}`);
  }

  /**
   * Send a message to a running workflow (signal).
   */
  async sendMessage(workflowId: string, topic: string, value: any): Promise<void> {
    await this._request('POST', `/api/workflows/${workflowId}/signal`, {
      signal_name: topic,
      input: value,
    });
  }

  /**
   * Health check.
   */
  async healthCheck(): Promise<boolean> {
    try {
      await this._request('GET', '/api/health');
      return true;
    } catch {
      return false;
    }
  }

  private _createHandle<T>(workflowId: string, workflowName: string): RemoteWorkflowHandle<T> {
    const self = this;
    return {
      workflowId,
      async getStatus(): Promise<WorkflowStatusResult> {
        const desc = await self._request<any>('GET', `/api/workflows/${workflowId}`);
        return {
          status: (desc.status || 'RUNNING').toUpperCase(),
          workflowId,
          workflowName,
        };
      },
      async getResult(): Promise<T> {
        const desc = await self._request<any>('GET', `/api/workflows/${workflowId}`);
        if (desc.status === 'FAILED') {
          throw new Error(`Workflow failed: ${desc.error || 'unknown error'}`);
        }
        return desc.result as T;
      },
    };
  }
}
