/**
 * HTTP Transport Layer for Velocity Classic SDK
 * Connects to the Velocity Engine REST API
 */

import { WorkflowExecution, WorkflowStatus } from './index';

export interface HttpClientConfig {
  baseUrl: string;
  timeoutMs?: number;
  headers?: Record<string, string>;
}

export class VelocityHttpClient {
  private _baseUrl: string;
  private _timeoutMs: number;
  private _headers: Record<string, string>;

  constructor(config: HttpClientConfig) {
    this._baseUrl = config.baseUrl.replace(/\/$/, ''); // Remove trailing slash
    this._timeoutMs = config.timeoutMs ?? 30000;
    this._headers = {
      'Content-Type': 'application/json',
      ...config.headers,
    };
  }

  private async _request<T>(method: string, path: string, body?: any): Promise<T> {
    const url = `${this._baseUrl}${path}`;
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this._timeoutMs);

    try {
      const response = await fetch(url, {
        method,
        headers: this._headers,
        body: body ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`HTTP ${response.status}: ${errorText}`);
      }

      const text = await response.text();
      if (!text) return {} as T;
      return JSON.parse(text) as T;
    } finally {
      clearTimeout(timeoutId);
    }
  }

  async startWorkflow(workflowType: string, taskQueue: string, input?: any): Promise<{ workflowId: string; runId: string }> {
    const result = await this._request<{ workflow_id: string; run_id: string }>('POST', '/api/workflows', {
      workflow_type: workflowType,
      task_queue: taskQueue,
      input,
    });
    return {
      workflowId: result.workflow_id,
      runId: result.run_id,
    };
  }

  async signalWorkflow(workflowId: string, signalName: string, input?: any): Promise<void> {
    await this._request('POST', `/api/workflows/${workflowId}/signal`, {
      signal_name: signalName,
      input,
    });
  }

  async queryWorkflow(workflowId: string, queryType: string, input?: any): Promise<any> {
    const result = await this._request<{ result: any }>('POST', `/api/workflows/${workflowId}/query`, {
      query_type: queryType,
      input,
    });
    return result.result;
  }

  async terminateWorkflow(workflowId: string, reason?: string): Promise<void> {
    await this._request('POST', `/api/workflows/${workflowId}/terminate`, {
      reason,
    });
  }

  async describeWorkflow(workflowId: string): Promise<any> {
    return await this._request('GET', `/api/workflows/${workflowId}`);
  }

  async listWorkflows(filter?: { status?: string; limit?: number }): Promise<any[]> {
    const params = new URLSearchParams();
    if (filter?.status) params.append('status', filter.status);
    if (filter?.limit) params.append('limit', filter.limit.toString());
    const queryString = params.toString() ? `?${params.toString()}` : '';
    return await this._request('GET', `/api/workflows${queryString}`);
  }

  async healthCheck(): Promise<boolean> {
    try {
      await this._request('GET', '/api/health');
      return true;
    } catch {
      return false;
    }
  }
}

/**
 * Remote Client that connects to Velocity Engine via HTTP
 */
export class RemoteClient {
  private _http: VelocityHttpClient;
  private _executions = new Map<string, WorkflowExecution>();

  constructor(baseUrl: string, headers?: Record<string, string>) {
    this._http = new VelocityHttpClient({ baseUrl, headers });
  }

  async startWorkflow(
    workflowId: string,
    workflowType: string,
    args: any[],
    options?: { taskQueue?: string }
  ): Promise<WorkflowExecution> {
    const result = await this._http.startWorkflow(workflowType, options?.taskQueue ?? 'default', args);
    
    const execution: WorkflowExecution = {
      workflowId,
      runId: result.runId,
      workflowType,
      status: WorkflowStatus.RUNNING,
      startTime: Date.now(),
    };
    this._executions.set(workflowId, execution);
    
    return execution;
  }

  async signal(workflowId: string, signalName: string, input: any): Promise<void> {
    await this._http.signalWorkflow(workflowId, signalName, input);
  }

  async query(workflowId: string, queryType: string, input?: any): Promise<any> {
    return await this._http.queryWorkflow(workflowId, queryType, input);
  }

  async cancel(workflowId: string): Promise<void> {
    const exec = this._executions.get(workflowId);
    if (exec) {
      exec.status = WorkflowStatus.CANCELLED;
      exec.closeTime = Date.now();
    }
    // Note: Engine doesn't have a cancel endpoint, so we just update local state
  }

  async terminate(workflowId: string, reason?: string): Promise<void> {
    await this._http.terminateWorkflow(workflowId, reason);
    const exec = this._executions.get(workflowId);
    if (exec) {
      exec.status = WorkflowStatus.TERMINATED;
      exec.closeTime = Date.now();
      exec.error = reason;
    }
  }

  async describe(workflowId: string): Promise<WorkflowExecution | undefined> {
    const local = this._executions.get(workflowId);
    if (local) return local;
    
    // Fetch from engine
    const remote = await this._http.describeWorkflow(workflowId);
    if (remote) {
      const execution: WorkflowExecution = {
        workflowId,
        runId: remote.run_id || remote.runId,
        workflowType: remote.workflow_type || remote.workflowType,
        status: this._mapStatus(remote.status),
        startTime: remote.start_time || remote.startTime,
        closeTime: remote.close_time || remote.closeTime,
        result: remote.result,
        error: remote.error,
      };
      this._executions.set(workflowId, execution);
      return execution;
    }
    return undefined;
  }

  async list(filter?: { status?: WorkflowStatus; workflowType?: string; limit?: number }): Promise<WorkflowExecution[]> {
    const remoteWorkflows = await this._http.listWorkflows({
      status: filter?.status,
      limit: filter?.limit,
    });
    
    return remoteWorkflows.map((wf: any) => {
      const execution: WorkflowExecution = {
        workflowId: wf.workflow_id || wf.workflowId,
        runId: wf.run_id || wf.runId,
        workflowType: wf.workflow_type || wf.workflowType,
        status: this._mapStatus(wf.status),
        startTime: wf.start_time || wf.startTime,
        closeTime: wf.close_time || wf.closeTime,
        result: wf.result,
        error: wf.error,
      };
      this._executions.set(execution.workflowId, execution);
      return execution;
    });
  }

  async healthCheck(): Promise<{ status: string; checks: any[]; timestamp: number }> {
    const healthy = await this._http.healthCheck();
    return {
      status: healthy ? 'healthy' : 'unhealthy',
      checks: [
        { name: 'connectivity', status: healthy ? 'healthy' : 'unhealthy', latencyMs: 0 },
      ],
      timestamp: Date.now(),
    };
  }

  private _mapStatus(status: string): WorkflowStatus {
    const statusMap: Record<string, WorkflowStatus> = {
      'running': WorkflowStatus.RUNNING,
      'completed': WorkflowStatus.COMPLETED,
      'failed': WorkflowStatus.FAILED,
      'cancelled': WorkflowStatus.CANCELLED,
      'terminated': WorkflowStatus.TERMINATED,
    };
    return statusMap[status.toLowerCase()] || WorkflowStatus.RUNNING;
  }
}
