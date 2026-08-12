# VELOCITY-WorkFlow PHP SDK Examples

## Prerequisites

1. **Start the VELOCITY-WorkFlow server:**
   ```bash
   cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server
   dotnet run
   ```

2. **Install dependencies:**
   ```bash
   cd VELOCITY-WorkFlow/sdk/php
   composer install
   ```

3. **Build the native engine** (for FFI mode):
   ```bash
   cd VELOCITY-WorkFlow/velocity-workflow-engine
   cargo build --release
   ```

## Examples

| File | Description |
|------|-------------|
| `basic_workflow.php` | Simple workflow with signal and query |
| `saga_pattern.php` | Multi-step saga with compensation on failure |

## Running an Example

```bash
php examples/basic_workflow.php
```

Each example connects to `localhost:50051` by default. Update the address in the
script if your server runs elsewhere.
