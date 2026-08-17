# VCTP SDKs and Developer Tools

## Overview

A complete developer ecosystem for VCTP: native SDKs in TypeScript, Python, and Go; a CLI tool for operations; a Wireshark dissector for packet inspection; and an OpenAPI spec generator for REST documentation.

## VCTP SDKs

### TypeScript SDK

**Path:** `velocity-sdk-typescript/src/vctp-transport.ts` (358 lines)

```typescript
class VctpTransport {
  async connect(): Promise<void>;
  async disconnect(): Promise<void>;
  async startWorkflow(opts: StartWorkflowOptions): Promise<WorkflowHandle>;
  async signalWorkflow(workflowId: string, signalName: string, payload?: any): Promise<void>;
  async queryWorkflow(workflowId: string, queryType: string): Promise<any>;
  async cancelWorkflow(workflowId: string): Promise<void>;
  async terminateWorkflow(workflowId: string): Promise<void>;
  async describeWorkflow(workflowId: string): Promise<WorkflowDescription>;
}
```

- Async/await API with full TypeScript types
- Auto-reconnect with configurable retry
- Heartbeat monitoring

### Python SDK

**Path:** `velocity-sdk-python/velocity_workflow/vctp.py` (316 lines)

```python
class VctpTransport:
    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
    async def start_workflow(self, *, workflow_type: str, workflow_id: str, ...) -> WorkflowHandle: ...
    async def signal_workflow(self, workflow_id: str, signal_name: str, ...) -> None: ...
    async def query_workflow(self, workflow_id: str, query_type: str) -> Any: ...
    async def cancel_workflow(self, workflow_id: str) -> None: ...
    async def terminate_workflow(self, workflow_id: str) -> None: ...
    async def describe_workflow(self, workflow_id: str) -> WorkflowDescription: ...
```

- Python asyncio-based with type hints
- Context manager support (`async with`)
- Same API surface as TypeScript SDK

### Go SDK

**Path:** `velocity-sdk-go/vctp/client.go` (451 lines)

```go
type Client struct { ... }

func (c *Client) StartWorkflow(ctx context.Context, opts StartWorkflowOptions) (*StartWorkflowResult, error)
func (c *Client) DescribeWorkflow(ctx context.Context, workflowID string) (*StartWorkflowResult, error)
func (c *Client) SignalWorkflow(ctx context.Context, workflowID, signalName string, payload []byte) error
func (c *Client) QueryWorkflow(ctx context.Context, workflowID, queryType string) ([]byte, error)
func (c *Client) CancelWorkflow(ctx context.Context, workflowID string) error
func (c *Client) TerminateWorkflow(ctx context.Context, workflowID string) error
```

- Idiomatic Go with context support
- Error types for VCTP status codes
- Connection pooling

## Developer Tools

### vctp-cli

**Path:** `tools/vctp-cli/vctp_cli.py` (267 lines)

Python CLI for VCTP server operations:

```bash
# Health check
vctp-cli health --server 127.0.0.1:9090

# Start a workflow
vctp-cli start-workflow --server 127.0.0.1:9090 --type my-workflow --steps 5

# Signal a workflow
vctp-cli signal --server 127.0.0.1:9090 --workflow-id wf-123 --name my-signal

# Query workflow state
vctp-cli query --server 127.0.0.1:9090 --workflow-id wf-123

# Cancel/terminate
vctp-cli cancel --server 127.0.0.1:9090 --workflow-id wf-123
vctp-cli terminate --server 127.0.0.1:9090 --workflow-id wf-123
```

- Subcommands for all VCTP methods
- JSON output for scripting
- Used by K8s health probes

### Wireshark Dissector

**Path:** `tools/vctp-wireshark/vctp.lua` (221 lines)

Lua-based Wireshark protocol dissector for VCTP packet inspection:

**Installation:**
- Windows: `%APPDATA%\Wireshark\plugins\vctp.lua`
- macOS: `~/.local/lib/wireshark/plugins/vctp.lua`
- Linux: `~/.local/lib/wireshark/plugins/vctp.lua`

**Features:**
- Decodes VCTP header fields (magic, sequence, workflow_id, slab_offset, payload_length)
- Resolves method names from method IDs
- Fragment reassembly display (index/total from slab_offset)
- JSON payload pretty-print
- Filter support: `vctp.magic`, `vctp.sequence`, `vctp.method`, etc.

### OpenAPI Spec Generator

**Path:** `tools/vctp-openapi/gen_openapi.py` (407 lines)

Generates OpenAPI 3.0.3 specification from VCTP protocol definitions:

```bash
# Generate to stdout
python gen_openapi.py

# Generate to file
python gen_openapi.py --output openapi.yaml
```

**Output includes:**
- All VCTP methods as REST endpoints
- Request/response schemas
- Auth security schemes (JWT bearer, API key)
- Error response definitions

### Protocol Definition Schema

**Path:** `proto/vctp_service.json` (167 lines)

Machine-readable JSON Schema defining the complete VCTP protocol:

```json
{
  "transport": {
    "protocol": "UDP",
    "header_size_bytes": 28,
    "magic": "0x50544356",
    "max_payload_bytes": 65479,
    "checksum": "CRC32",
    "default_port": 9090
  },
  "auth": {
    "methods": ["jwt_bearer", "api_key"],
    "jwt_algorithm": "HS256"
  },
  "security": {
    "rate_limiting": { "type": "token_bucket" },
    "circuit_breaker": { "states": ["Closed", "Open", "HalfOpen"] }
  },
  "methods": { ... }
}
```

## Source Files

| File | Lines | Role |
|------|-------|------|
| `velocity-sdk-typescript/src/vctp-transport.ts` | 358 | TypeScript VCTP client SDK |
| `velocity-sdk-python/velocity_workflow/vctp.py` | 316 | Python VCTP client SDK |
| `velocity-sdk-go/vctp/client.go` | 451 | Go VCTP client SDK |
| `tools/vctp-cli/vctp_cli.py` | 267 | Python CLI tool |
| `tools/vctp-wireshark/vctp.lua` | 221 | Wireshark protocol dissector |
| `tools/vctp-openapi/gen_openapi.py` | 407 | OpenAPI 3.0 spec generator |
| `proto/vctp_service.json` | 167 | Protocol definition schema |
