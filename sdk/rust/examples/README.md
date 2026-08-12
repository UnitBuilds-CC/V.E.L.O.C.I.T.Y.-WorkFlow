# VELOCITY-WorkFlow Rust SDK Examples

## Prerequisites

1. **Build the workflow engine:**
   ```bash
   cd VELOCITY-WorkFlow/velocity-workflow-engine
   cargo build --release
   ```

2. **Build the SDK:**
   ```bash
   cd VELOCITY-WorkFlow/sdk/rust
   cargo build
   ```

## Examples

| File | Description |
|------|-------------|
| `basic_workflow.rs` | Simple workflow with signal and query |
| `saga_pattern.rs` | Multi-step saga with compensation on failure |

## Running an Example

```bash
cargo run --example basic_workflow
cargo run --example saga_pattern
```

The Rust SDK links directly against the native `velocity-workflow-engine`
library — no network or gRPC server required.
