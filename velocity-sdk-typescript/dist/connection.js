"use strict";
/**
 * gRPC Connection to Velocity server
 */
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.Connection = void 0;
const grpc = __importStar(require("@grpc/grpc-js"));
const protoLoader = __importStar(require("@grpc/proto-loader"));
const path = __importStar(require("path"));
class Connection {
    constructor(options) {
        this.options = options;
        this.initializeClient();
    }
    initializeClient() {
        // Load proto files
        const protoPath = path.join(__dirname, '..', 'proto', 'velocity', 'v1', 'workflow_service.proto');
        const packageDefinition = protoLoader.loadSync(protoPath, {
            keepCase: false,
            longs: String,
            enums: String,
            defaults: true,
            oneofs: true,
        });
        const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
        const velocityService = protoDescriptor.velocity.v1.WorkflowService;
        // Create gRPC client
        const credentials = this.options.tls
            ? grpc.credentials.createSsl()
            : grpc.credentials.createInsecure();
        this.client = new velocityService(this.options.address, credentials);
    }
    async startWorkflow(params) {
        return new Promise((resolve, reject) => {
            this.client.StartWorkflowExecution({
                namespace: params.namespace,
                workflow_execution: {
                    workflow_id: params.workflowId,
                    run_id: '',
                },
                workflow_type: {
                    name: params.workflowType,
                    type_id: 0,
                },
                task_queue: {
                    name: params.taskQueue,
                    hash: 0,
                    kind: 0,
                },
                input: params.input ? { data: Buffer.from(JSON.stringify(params.input)), encoding: 0, metadata: {} } : undefined,
            }, (err, response) => {
                if (err)
                    reject(err);
                else
                    resolve({ workflowId: params.workflowId, runId: response.run_id || '' });
            });
        });
    }
    async signalWorkflow(params) {
        return new Promise((resolve, reject) => {
            this.client.SignalWorkflowExecution({
                namespace: params.namespace,
                workflow_execution: {
                    workflow_id: params.workflowId,
                    run_id: '',
                },
                signal_name: params.signalName,
                input: params.input ? { data: Buffer.from(JSON.stringify(params.input)), encoding: 0, metadata: {} } : undefined,
            }, (err, _response) => {
                if (err)
                    reject(err);
                else
                    resolve();
            });
        });
    }
    async queryWorkflow(params) {
        return new Promise((resolve, reject) => {
            this.client.QueryWorkflow({
                namespace: params.namespace,
                workflow_execution: {
                    workflow_id: params.workflowId,
                    run_id: '',
                },
                query_type: params.queryType,
                input: params.input ? { data: Buffer.from(JSON.stringify(params.input)), encoding: 0, metadata: {} } : undefined,
            }, (err, response) => {
                if (err)
                    reject(err);
                else
                    resolve(response.result ? JSON.parse(response.result.data.toString()) : undefined);
            });
        });
    }
    async terminateWorkflow(params) {
        return new Promise((resolve, reject) => {
            this.client.TerminateWorkflowExecution({
                namespace: params.namespace,
                workflow_execution: {
                    workflow_id: params.workflowId,
                    run_id: '',
                },
                reason: params.reason || '',
            }, (err, _response) => {
                if (err)
                    reject(err);
                else
                    resolve();
            });
        });
    }
    async cancelWorkflow(params) {
        return new Promise((resolve, reject) => {
            this.client.RequestCancelWorkflowExecution({
                namespace: params.namespace,
                workflow_execution: {
                    workflow_id: params.workflowId,
                    run_id: '',
                },
            }, (err, _response) => {
                if (err)
                    reject(err);
                else
                    resolve();
            });
        });
    }
    async describeWorkflow(params) {
        return new Promise((resolve, reject) => {
            this.client.DescribeWorkflowExecution({
                namespace: params.namespace,
                workflow_execution: {
                    workflow_id: params.workflowId,
                    run_id: '',
                },
            }, (err, response) => {
                if (err)
                    reject(err);
                else
                    resolve(response);
            });
        });
    }
    async getWorkflowHistory(params) {
        return new Promise((resolve, reject) => {
            this.client.GetWorkflowExecutionHistory({
                namespace: params.namespace,
                workflow_execution: {
                    workflow_id: params.workflowId,
                    run_id: '',
                },
            }, (err, response) => {
                if (err)
                    reject(err);
                else
                    resolve(response.history?.events || []);
            });
        });
    }
    async pollWorkflowTaskQueue(params) {
        return new Promise((resolve, reject) => {
            this.client.PollWorkflowTaskQueue({
                namespace: params.namespace,
                task_queue: {
                    name: params.taskQueue,
                    hash: 0,
                    kind: 0,
                },
                identity: 'typescript-worker',
            }, (err, response) => {
                if (err)
                    reject(err);
                else
                    resolve(response);
            });
        });
    }
    async pollActivityTaskQueue(params) {
        return new Promise((resolve, reject) => {
            this.client.PollActivityTaskQueue({
                namespace: params.namespace,
                task_queue: {
                    name: params.taskQueue,
                    hash: 0,
                    kind: 0,
                },
                identity: 'typescript-worker',
            }, (err, response) => {
                if (err)
                    reject(err);
                else
                    resolve(response);
            });
        });
    }
    async respondWorkflowTaskCompleted(params) {
        return new Promise((resolve, reject) => {
            this.client.RespondWorkflowTaskCompleted({
                task_token: params.taskToken,
                commands: params.commands,
            }, (err, _response) => {
                if (err)
                    reject(err);
                else
                    resolve();
            });
        });
    }
    async respondActivityTaskCompleted(params) {
        return new Promise((resolve, reject) => {
            this.client.RespondActivityTaskCompleted({
                task_token: params.taskToken,
                result: params.result ? { data: Buffer.from(JSON.stringify(params.result)), encoding: 0, metadata: {} } : undefined,
            }, (err, _response) => {
                if (err)
                    reject(err);
                else
                    resolve();
            });
        });
    }
    async respondActivityTaskFailed(params) {
        return new Promise((resolve, reject) => {
            this.client.RespondActivityTaskFailed({
                task_token: params.taskToken,
                failure: { data: Buffer.from(params.failure), encoding: 0, metadata: {} },
            }, (err, _response) => {
                if (err)
                    reject(err);
                else
                    resolve();
            });
        });
    }
    close() {
        if (this.client) {
            this.client.close();
        }
    }
}
exports.Connection = Connection;
//# sourceMappingURL=connection.js.map