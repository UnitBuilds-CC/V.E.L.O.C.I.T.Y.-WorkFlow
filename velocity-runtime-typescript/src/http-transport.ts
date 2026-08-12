/**
 * HTTP Transport for Velocity Runtime SDK
 * Connects to the Velocity Engine REST API
 */

export interface RuntimeRemoteConfig {
  baseUrl: string;
  timeoutMs?: number;
}

export class RuntimeRemoteClient {
  private _baseUrl: string;
  private _timeoutMs: number;

  constructor(config: RuntimeRemoteConfig) {
    this._baseUrl = config.baseUrl.replace(/\/$/, '');
    this._timeoutMs = config.timeoutMs ?? 30000;
  }

  private async _request<T>(method: string, path: string, body?: any): Promise<T> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this._timeoutMs);
    try {
      const res = await fetch(`${this._baseUrl}${path}`, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: body ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const text = await res.text();
      if (!text) return {} as T;
      return JSON.parse(text) as T;
    } finally {
      clearTimeout(timer);
    }
  }

  async invoke(serviceName: string, handlerName: string, key: string, args: any[] = []): Promise<any> {
    const result = await this._request<any>('POST', '/api/invoke', {
      service_name: serviceName,
      handler_name: handlerName,
      key,
      args,
    });
    return result.data;
  }

  async send(serviceName: string, handlerName: string, key: string, args: any[] = []): Promise<string> {
    const result = await this._request<{ invocation_id: string }>('POST', '/api/send', {
      service_name: serviceName,
      handler_name: handlerName,
      key,
      args,
    });
    return result.invocation_id;
  }

  async getInvocation(invocationId: string): Promise<any> {
    return this._request('GET', `/api/invocations/${invocationId}`);
  }

  async resolveAwakeable(id: string, value: any): Promise<void> {
    await this._request('POST', `/api/awakeables/${id}/resolve`, { value });
  }

  async rejectAwakeable(id: string, reason: string): Promise<void> {
    await this._request('POST', `/api/awakeables/${id}/reject`, { reason });
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
