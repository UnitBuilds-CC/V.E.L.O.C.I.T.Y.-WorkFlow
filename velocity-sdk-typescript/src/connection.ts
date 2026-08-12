/**
 * HTTP Connection to Velocity server
 */

export interface ConnectionOptions {
  address: string;
  tls?: boolean;
  headers?: Record<string, string>;
}

export class Connection {
  private baseUrl: string;
  private headers: Record<string, string>;

  constructor(options: ConnectionOptions) {
    let address = options.address;
    if (!address.startsWith('http://') && !address.startsWith('https://')) {
      const scheme = options.tls ? 'https' : 'http';
      address = `${scheme}://${address}`;
    }
    this.baseUrl = address.replace(/\/+$/, '');
    this.headers = {
      'Content-Type': 'application/json',
      ...options.headers,
    };
  }

  private async request<T = any>(method: string, path: string, body?: any): Promise<T> {
    const url = `${this.baseUrl}${path}`;
    const resp = await fetch(url, {
      method,
      headers: this.headers,
      body: body ? JSON.stringify(body) : undefined,
    });

    const text = await resp.text();
    if (!resp.ok) {
      throw new Error(`HTTP ${resp.status}: ${text}`);
    }

    if (text.length === 0) return undefined as any;
    try {
      return JSON.parse(text) as T;
    } catch {
      return text as any;
    }
  }

  async startWorkflow(params: {
    namespace: string;
    workflowId: string;
    workflowType: string;
    taskQueue: string;
    input?: any;
  }): Promise<{ workflowId: string; runId: string }> {
    return this.request('POST', '/api/workflows', params);
  }

  async signalWorkflow(params: {
    namespace: string;
    workflowId: string;
    signalName: string;
    input?: any;
  }): Promise<void> {
    await this.request('POST', `/api/workflows/${params.workflowId}/signal`, params);
  }

  async queryWorkflow(params: {
    namespace: string;
    workflowId: string;
    queryType: string;
    input?: any;
  }): Promise<any> {
    return this.request('POST', `/api/workflows/${params.workflowId}/query`, params);
  }

  async terminateWorkflow(params: {
    namespace: string;
    workflowId: string;
    reason?: string;
  }): Promise<void> {
    await this.request('POST', `/api/workflows/${params.workflowId}/terminate`, params);
  }

  async cancelWorkflow(params: {
    namespace: string;
    workflowId: string;
  }): Promise<void> {
    await this.request('POST', `/api/workflows/${params.workflowId}/cancel`, params);
  }

  async describeWorkflow(params: {
    namespace: string;
    workflowId: string;
  }): Promise<any> {
    return this.request('GET', `/api/workflows/${params.workflowId}`);
  }

  async getWorkflowHistory(params: {
    namespace: string;
    workflowId: string;
  }): Promise<any[]> {
    try {
      return await this.request('GET', `/api/workflows/${params.workflowId}/history`);
    } catch {
      return [];
    }
  }

  async listWorkflows(namespace: string): Promise<any[]> {
    try {
      const resp = await this.request<{ workflows: any[] }>('GET', '/api/workflows');
      return resp?.workflows || [];
    } catch {
      return [];
    }
  }

  async healthCheck(): Promise<boolean> {
    try {
      const resp = await this.request<{ status: string }>('GET', '/api/health');
      return resp?.status === 'healthy' || resp?.status === 'ok';
    } catch {
      return false;
    }
  }

  // Polling methods (for worker task processing)
  async pollWorkflowTaskQueue(_params: {
    namespace: string;
    taskQueue: string;
  }): Promise<any> {
    // In HTTP mode, workflow tasks come from the REST API polling
    return null;
  }

  async pollActivityTaskQueue(_params: {
    namespace: string;
    taskQueue: string;
  }): Promise<any> {
    // In HTTP mode, activity tasks come from the REST API polling
    return null;
  }

  async respondWorkflowTaskCompleted(_params: {
    taskToken: string;
    commands: any[];
  }): Promise<void> {
    // In HTTP mode, workflow task responses are sent via the REST API
  }

  async respondActivityTaskCompleted(_params: {
    taskToken: string;
    result?: any;
  }): Promise<void> {
    // In HTTP mode, activity task responses are sent via the REST API
  }

  async respondActivityTaskFailed(_params: {
    taskToken: string;
    failure: string;
  }): Promise<void> {
    // In HTTP mode, activity task failures are reported via the REST API
  }

  close(): void {
    // No-op for HTTP connections
  }
}
