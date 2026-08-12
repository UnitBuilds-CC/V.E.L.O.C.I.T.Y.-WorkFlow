/**
 * Velocity Engine HTTP Server
 * 
 * REST API server that exposes workflow operations over HTTP.
 * Connects to a Worker for actual execution.
 */

import * as http from 'http';
import { Worker, Client, WorkflowStatus } from './index';

// ─── Types ───────────────────────────────────────────────────────────────────

export interface ServerConfig {
  port: number;
  host?: string;
  worker: Worker;
}

export interface ApiResponse<T = any> {
  success: boolean;
  data?: T;
  error?: string;
}

// ─── HTTP Server ─────────────────────────────────────────────────────────────

export class VelocityServer {
  private _server: http.Server | null = null;
  private _worker: Worker;
  private _client: Client;
  private _port: number;
  private _host: string;

  constructor(config: ServerConfig) {
    this._worker = config.worker;
    this._client = new Client({}, config.worker);
    this._port = config.port;
    this._host = config.host ?? '0.0.0.0';
  }

  async start(): Promise<void> {
    return new Promise((resolve) => {
      this._server = http.createServer(async (req, res) => {
        // CORS headers
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
        res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
        
        if (req.method === 'OPTIONS') {
          res.writeHead(204);
          res.end();
          return;
        }

        try {
          const url = new URL(req.url || '/', `http://${req.headers.host}`);
          const path = url.pathname;
          const method = req.method || 'GET';
          
          // Parse body
          let body: any = {};
          if (method === 'POST' || method === 'PUT') {
            body = await this._readBody(req);
          }

          // Route
          const response = await this._route(method, path, body);
          
          res.writeHead(response.success ? 200 : 400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(response));
        } catch (err: any) {
          res.writeHead(500, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: false, error: err.message || 'Internal error' }));
        }
      });

      this._server.listen(this._port, this._host, () => {
        resolve();
      });
    });
  }

  async stop(): Promise<void> {
    return new Promise((resolve) => {
      if (this._server) {
        this._server.close(() => resolve());
      } else {
        resolve();
      }
    });
  }

  get port(): number { return this._port; }
  get host(): string { return this._host; }

  // ─── Routing ─────────────────────────────────────────────────────────────

  private async _route(method: string, path: string, body: any): Promise<ApiResponse> {
    // Health check
    if (path === '/api/health' && method === 'GET') {
      const health = await this._worker.healthCheck();
      return { success: true, data: health };
    }

    // List workflows
    if (path === '/api/workflows' && method === 'GET') {
      const executions = await this._client.list();
      return { success: true, data: executions };
    }

    // Start workflow
    if (path === '/api/workflows' && method === 'POST') {
      const { workflow_id, workflow_type, task_queue, input } = body;
      const execution = await this._client.startWorkflow(
        workflow_id || `wf-${Date.now()}`,
        workflow_type,
        input || [],
        { taskQueue: task_queue }
      );
      return { success: true, data: execution };
    }

    // Workflow-specific routes
    const workflowMatch = path.match(/^\/api\/workflows\/([^/]+)(\/(\w+))?$/);
    if (workflowMatch) {
      const workflowId = workflowMatch[1];
      const action = workflowMatch[3];

      if (!action && method === 'GET') {
        // Describe workflow
        const execution = await this._client.describe(workflowId);
        if (!execution) return { success: false, error: 'Workflow not found' };
        return { success: true, data: execution };
      }

      if (action === 'signal' && method === 'POST') {
        // Signal workflow
        const { signal_name, input } = body;
        await this._client.signal(workflowId, signal_name, input);
        return { success: true };
      }

      if (action === 'query' && method === 'POST') {
        // Query workflow
        const { query_type, input } = body;
        const result = await this._client.query(workflowId, query_type, input);
        return { success: true, data: { result } };
      }

      if (action === 'terminate' && method === 'POST') {
        // Terminate workflow
        const { reason } = body;
        await this._client.terminate(workflowId, reason);
        return { success: true };
      }

      if (action === 'cancel' && method === 'POST') {
        // Cancel workflow
        await this._client.cancel(workflowId);
        return { success: true };
      }

      if (action === 'update' && method === 'POST') {
        // Update workflow
        const { update_type, input } = body;
        const result = await this._client.update(workflowId, update_type, input);
        return { success: true, data: result };
      }

      if (action === 'reset' && method === 'POST') {
        // Reset workflow
        const { event_id } = body;
        const execution = await this._client.reset(workflowId, event_id);
        return { success: true, data: execution };
      }
    }

    // Nexus operations
    if (path.startsWith('/api/nexus/') && method === 'POST') {
      const operation = path.replace('/api/nexus/', '');
      // Nexus operations require endpoint registration
      return { success: false, error: 'Nexus operation not configured' };
    }

    return { success: false, error: `Not found: ${method} ${path}` };
  }

  private _readBody(req: http.IncomingMessage): Promise<any> {
    return new Promise((resolve, reject) => {
      let data = '';
      req.on('data', chunk => data += chunk);
      req.on('end', () => {
        try {
          resolve(data ? JSON.parse(data) : {});
        } catch {
          resolve({});
        }
      });
      req.on('error', reject);
    });
  }
}
