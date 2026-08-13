# VELOCITY-WorkFlow Ruby SDK Examples

## Prerequisites

1. **Start the VELOCITY-WorkFlow server:**
   ```bash
   cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server
   dotnet run
   ```

2. **Install dependencies:**
   ```bash
   cd VELOCITY-WorkFlow/sdk/ruby
   bundle install
   ```

3. **Build the native engine** (for FFI mode):
   ```bash
   cd VELOCITY-WorkFlow/velocity-workflow-engine
   cargo build --release
   ```

## Examples

| File | Description |
|------|-------------|
| `basic_workflow.rb` | Simple workflow with signal and query |
| `saga_pattern.rb` | Multi-step saga with compensation on failure |

## Running an Example

```bash
ruby examples/basic_workflow.rb
```

Each example connects to `localhost:7234` by default. Update the address in the
script if your server runs elsewhere.
