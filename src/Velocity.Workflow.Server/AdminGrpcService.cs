using Grpc.Core;
using Velocity.Workflow.Core;
using Velocity.Workflow.Server.Protos.Admin;

namespace Velocity.Workflow.Server;

/// <summary>
/// Admin service for cluster management and operational diagnostics.
/// Mirrors Temporal's AdminService for operational tasks.
/// </summary>
public class AdminGrpcService : AdminService.AdminServiceBase
{
    private readonly WorkflowRuntime _runtime;
    private readonly ILogger<AdminGrpcService> _logger;

    public AdminGrpcService(WorkflowRuntime runtime, ILogger<AdminGrpcService> logger)
    {
        _runtime = runtime;
        _logger = logger;
    }

    public override Task<DescribeClusterResponse> DescribeCluster(DescribeClusterRequest request, ServerCallContext context)
    {
        var response = new DescribeClusterResponse
        {
            ClusterName = "velocity-cluster-0",
            ClusterId = "velocity-single-node",
            HistoryShardCount = 1, // Single-node
            WorkflowCount = (long)_runtime.VisibilityCount,
            NamespaceCount = (long)_runtime.NamespaceCount,
        };
        response.ClusterMetadata["version"] = "1.0.0";
        response.ClusterMetadata["engine"] = "rust";
        response.ClusterMetadata["ffi_exports"] = "120+";
        response.ClusterMetadata["grpc_rpcs"] = "34";

        return Task.FromResult(response);
    }

    public override Task<ListClustersResponse> ListClusters(ListClustersRequest request, ServerCallContext context)
    {
        var response = new ListClustersResponse();
        response.Clusters.Add(new Protos.Admin.ClusterInfo
        {
            ClusterName = "velocity-cluster-0",
            ClusterId = "velocity-single-node",
            IsActive = true,
            FailoverVersion = 0,
        });
        return Task.FromResult(response);
    }

    public override Task<GetWorkflowExecutionRawResponse> GetWorkflowExecutionRaw(
        GetWorkflowExecutionRawRequest request, ServerCallContext context)
    {
        ulong workflowKey = ulong.Parse(request.RunId);
        var status = _runtime.GetStatus(workflowKey);
        var events = _runtime.GetEventHistory(workflowKey);

        return Task.FromResult(new GetWorkflowExecutionRawResponse
        {
            Status = (int)status,
            EventCount = events.Count,
            StepCount = events.Count,
        });
    }

    public override Task<UpdateNamespaceResponse> UpdateNamespace(UpdateNamespaceRequest request, ServerCallContext context)
    {
        // Namespace updates are limited for now — just log the request
        _logger.LogInformation("UpdateNamespace: {Name} isActive={IsActive}",
            request.NamespaceName, request.IsActive);
        return Task.FromResult(new UpdateNamespaceResponse { Success = true });
    }

    public override Task<ForceUnloadTaskQueueResponse> ForceUnloadTaskQueue(
        ForceUnloadTaskQueueRequest request, ServerCallContext context)
    {
        _logger.LogInformation("ForceUnloadTaskQueue: {TaskQueue}", request.TaskQueue);
        return Task.FromResult(new ForceUnloadTaskQueueResponse { Success = true });
    }
}
