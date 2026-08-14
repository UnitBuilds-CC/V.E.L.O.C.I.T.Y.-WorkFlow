---
kind: api_pattern
name: gRPC Benchmark Service API
category: api
scope:
    - 'proto/**'
    - 'velocity-workflow-server/**'
    - 'bench-suite/prod-bench/**'
source_files:
    - proto/bench/v1/bench.proto
    - velocity-workflow-server/src/main.rs
    - bench-suite/prod-bench/src/velocity_client.rs
---

The Velocity Server exposes a gRPC API defined in Protocol Buffers, enabling high-performance workflow operations across multiple languages.

**Service Definition:**
```protobuf
syntax = "proto3";

package velocity.bench.v1;

service BenchmarkService {
  // Workflow lifecycle
  rpc StartWorkflow(StartWorkflowRequest) returns (StartWorkflowResponse);
  rpc SignalWorkflow(SignalWorkflowRequest) returns (SignalWorkflowResponse);
  rpc QueryWorkflow(QueryWorkflowRequest) returns (QueryWorkflowResponse);
  rpc CompleteStep(CompleteStepRequest) returns (CompleteStepResponse);
  rpc WaitForCompletion(WaitForCompletionRequest) returns (WaitForCompletionResponse);
  
  // Administrative
  rpc RegisterNamespace(RegisterNamespaceRequest) returns (RegisterNamespaceResponse);
  rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
}
```

**Message Types:**
```protobuf
message StartWorkflowRequest {
  string namespace = 1;
  string workflow_id = 2;
  string workflow_type = 3;
  bytes input = 4;
  map<string, string> search_attributes = 5;
}

message StartWorkflowResponse {
  string workflow_id = 1;
  string run_id = 2;
}

message SignalWorkflowRequest {
  string workflow_id = 1;
  string run_id = 2;
  string signal_name = 3;
  bytes payload = 4;
}

message QueryWorkflowRequest {
  string workflow_id = 1;
  string run_id = 2;
  string query_type = 3;
}

message QueryWorkflowResponse {
  bytes result = 1;
}
```

**Server Implementation (Rust):**
```rust
use tonic::{Request, Response, Status};

pub struct BenchmarkServiceImpl {
    engine: Arc<WorkflowEngine>,
}

#[tonic::async_trait]
impl BenchmarkService for BenchmarkServiceImpl {
    async fn start_workflow(
        &self,
        request: Request<StartWorkflowRequest>,
    ) -> Result<Response<StartWorkflowResponse>, Status> {
        let req = request.into_inner();
        
        let (workflow_id, run_id) = self.engine
            .start_workflow(
                &req.namespace,
                &req.workflow_id,
                &req.workflow_type,
                req.input,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        Ok(Response::new(StartWorkflowResponse {
            workflow_id,
            run_id,
        }))
    }
    
    async fn signal_workflow(
        &self,
        request: Request<SignalWorkflowRequest>,
    ) -> Result<Response<SignalWorkflowResponse>, Status> {
        let req = request.into_inner();
        
        self.engine
            .signal_workflow(
                &req.workflow_id,
                &req.run_id,
                &req.signal_name,
                req.payload,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        Ok(Response::new(SignalWorkflowResponse {}))
    }
    
    // ... other methods
}
```

**Client Implementation (Rust):**
```rust
use tonic::transport::Channel;

pub struct VelocityClient {
    client: BenchmarkServiceClient<Channel>,
    namespace: String,
}

impl VelocityClient {
    pub async fn new(grpc_url: &str) -> Result<Self, String> {
        let channel = Channel::from_shared(grpc_url.to_string())
            .connect()
            .await
            .map_err(|e| format!("Connect failed: {}", e))?;
        
        let client = BenchmarkServiceClient::new(channel);
        
        Ok(Self {
            client,
            namespace: "benchmark".to_string(),
        })
    }
    
    pub async fn start_workflow(
        &mut self,
        workflow_id: &str,
        workflow_type: &str,
        input: Vec<u8>,
    ) -> Result<(String, String), String> {
        let request = StartWorkflowRequest {
            namespace: self.namespace.clone(),
            workflow_id: workflow_id.to_string(),
            workflow_type: workflow_type.to_string(),
            input,
            search_attributes: HashMap::new(),
        };
        
        let response = self.client
            .start_workflow(request)
            .await
            .map_err(|e| format!("StartWorkflow failed: {}", e))?;
        
        let resp = response.into_inner();
        Ok((resp.workflow_id, resp.run_id))
    }
}
```

**Code Generation:**

**Rust (via build.rs):**
```rust
// velocity-workflow-server/build.rs
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&["proto/bench/v1/bench.proto"], &["proto"])?;
    Ok(())
}
```

**TypeScript:**
```bash
cd proto
npx protoc \
  --ts_out . \
  --ts_opt optimize_code_size \
  --proto_path . \
  bench/v1/bench.proto
```

**Python:**
```bash
python -m grpc_tools.protoc \
  --proto_path=proto \
  --python_out=. \
  --grpc_python_out=. \
  proto/bench/v1/bench.proto
```

**Performance Characteristics:**
- Request latency: ~1-5ms (network + serialization)
- Throughput: ~10k req/s (single connection)
- Connection overhead: ~10ms (TCP + TLS handshake)
- Message size: ~100 bytes (typical request)

**Error Handling:**
```rust
// Server-side error mapping
impl From<WorkflowError> for Status {
    fn from(err: WorkflowError) -> Self {
        match err {
            WorkflowError::NotFound => Status::not_found(err.to_string()),
            WorkflowError::AlreadyExists => Status::already_exists(err.to_string()),
            WorkflowError::InvalidArgument => Status::invalid_argument(err.to_string()),
            WorkflowError::Internal(e) => Status::internal(e.to_string()),
        }
    }
}
```

**Key files:**
- `proto/bench/v1/bench.proto` — Service definition
- `velocity-workflow-server/src/main.rs` — Server implementation
- `bench-suite/prod-bench/src/velocity_client.rs` — Client implementation
- `velocity-workflow-server/build.rs` — Code generation

**Rules for developers:**
1. Always use Protocol Buffers for API definitions
2. Generate code for all supported languages
3. Map domain errors to appropriate gRPC status codes
4. Use streaming RPCs for large payloads or long-running operations
5. Implement health checks for service discovery
6. Document all RPCs with comments in proto files
7. Test with realistic network conditions (latency, packet loss)
