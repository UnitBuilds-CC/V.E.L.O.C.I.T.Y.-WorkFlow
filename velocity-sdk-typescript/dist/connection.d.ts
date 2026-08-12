/**
 * gRPC Connection to Velocity server
 */
import * as grpc from '@grpc/grpc-js';
export interface ConnectionOptions {
    address: string;
    tls?: boolean;
    metadata?: grpc.Metadata;
}
export declare class Connection {
    private client;
    private options;
    constructor(options: ConnectionOptions);
    private initializeClient;
    startWorkflow(params: {
        namespace: string;
        workflowId: string;
        workflowType: string;
        taskQueue: string;
        input?: any;
    }): Promise<{
        workflowId: string;
        runId: string;
    }>;
    signalWorkflow(params: {
        namespace: string;
        workflowId: string;
        signalName: string;
        input?: any;
    }): Promise<void>;
    queryWorkflow(params: {
        namespace: string;
        workflowId: string;
        queryType: string;
        input?: any;
    }): Promise<any>;
    terminateWorkflow(params: {
        namespace: string;
        workflowId: string;
        reason?: string;
    }): Promise<void>;
    cancelWorkflow(params: {
        namespace: string;
        workflowId: string;
    }): Promise<void>;
    describeWorkflow(params: {
        namespace: string;
        workflowId: string;
    }): Promise<any>;
    getWorkflowHistory(params: {
        namespace: string;
        workflowId: string;
    }): Promise<any[]>;
    pollWorkflowTaskQueue(params: {
        namespace: string;
        taskQueue: string;
    }): Promise<any>;
    pollActivityTaskQueue(params: {
        namespace: string;
        taskQueue: string;
    }): Promise<any>;
    respondWorkflowTaskCompleted(params: {
        taskToken: string;
        commands: any[];
    }): Promise<void>;
    respondActivityTaskCompleted(params: {
        taskToken: string;
        result?: any;
    }): Promise<void>;
    respondActivityTaskFailed(params: {
        taskToken: string;
        failure: string;
    }): Promise<void>;
    close(): void;
}
