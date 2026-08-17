# Slab Engine — Merkle Root State Proof

## Overview

The slab engine provides cryptographic state verification for workflow execution. Each workflow has a `SlabHeader` with a SHA-256 Merkle root that proves the integrity of the workflow's step completion state. Any tampering with step completions is detectable by recomputing and comparing the Merkle root.

## SlabHeader Layout

`#[repr(C)]` binary struct for FFI compatibility and memory-mapped persistence:

```rust
// velocity-workflow-core/src/slab.rs
#[repr(C)]
pub struct SlabHeader {
    pub magic: u32,                 // 4 bytes: "VLCT" (0x564C4354)
    pub schema_version: u32,        // 4 bytes: Schema version ID
    pub workflow_id: u64,           // 8 bytes: Unique workflow instance ID
    pub run_id: u64,                // 8 bytes: Unique run ID
    pub current_step: u32,          // 4 bytes: Current step index
    pub total_steps: u32,           // 4 bytes: Total planned steps
    pub merkle_root: [u8; 32],      // 32 bytes: SHA-256 state proof
    pub step_bitmask: Bitmask256,   // 32 bytes: O(1) step completion flags
    pub reserved_padding: [u8; 32], // 32 bytes: Reserved for migrations
}
// Total: 128 bytes (SLAB_HEADER_SIZE)
```

## Bitmask256 — O(1) Step Tracking

256-bit bitmask for tracking up to 256 step completions in O(1):

```rust
// velocity-workflow-core/src/bitmask.rs
pub struct Bitmask256 {
    pub bits: [u64; 4],  // 4 × 64 = 256 bits
}

impl Bitmask256 {
    pub fn set_step(&mut self, step_index: usize) -> bool {
        if step_index >= 256 { return false; }
        let word = step_index / 64;
        let bit = step_index % 64;
        self.bits[word] |= 1u64 << bit;
        true
    }

    pub fn is_step_set(&self, step_index: usize) -> bool {
        if step_index >= 256 { return false; }
        let word = step_index / 64;
        let bit = step_index % 64;
        (self.bits[word] & (1u64 << bit)) != 0
    }
}
```

- **set_step():** Single bitwise OR — O(1)
- **is_step_set():** Single bitwise AND — O(1)
- **No iteration needed** to check if a step is complete

## Merkle Root Computation

SHA-256 hash of the complete slab state:

```rust
pub fn recalculate_merkle_root(&mut self) {
    let mut hasher = Sha256::new();
    hasher.update(self.magic.to_le_bytes());
    hasher.update(self.schema_version.to_le_bytes());
    hasher.update(self.workflow_id.to_le_bytes());
    hasher.update(self.run_id.to_le_bytes());
    hasher.update(self.current_step.to_le_bytes());
    hasher.update(self.total_steps.to_le_bytes());
    for word in &self.step_bitmask.bits {
        hasher.update(word.to_le_bytes());
    }
    let result = hasher.finalize();
    self.merkle_root.copy_from_slice(&result);
}
```

**Inputs to hash:**
1. Magic bytes (schema identifier)
2. Schema version
3. Workflow ID
4. Run ID
5. Current step index
6. Total steps
7. All 4 words of the step bitmask (32 bytes)

**Output:** 32-byte SHA-256 digest stored in `merkle_root`

## Verification

```rust
pub fn verify_merkle_root(&self) -> bool {
    // Recompute SHA-256 from current state
    let mut hasher = Sha256::new();
    // ... same inputs as recalculate ...
    let result = hasher.finalize();
    self.merkle_root == result.as_slice()
}
```

Returns `true` if the stored Merkle root matches the recomputed value. Any tampering with the bitmask, workflow_id, run_id, or step counters will cause verification to fail.

## Step Completion Flow

When a step completes:

```
1. ctx.complete_step(step, result)
   → slab.mark_step_completed(step)
     → bitmask.set_step(step)         // O(1) bitwise OR
     → slab.recalculate_merkle_root()  // SHA-256 recompute
   → step_results.insert(step, result) // Store result data
```

The Merkle root changes on every step completion because the bitmask changes. This provides a cryptographic chain of custody for the entire workflow execution.

## Database Persistence

The slab state is persisted to the database adapter via `WorkflowRecord::from_context()`:

```rust
pub fn from_context(ctx: &WorkflowContext, namespace_name: &str) -> Self {
    Self {
        merkle_root: ctx.slab.merkle_root.to_vec(),
        step_bitmask: ctx.slab.step_bitmask.bits
            .iter().flat_map(|w| w.to_le_bytes()).collect(),
        current_step: ctx.slab.current_step,
        total_steps: ctx.slab.total_steps,
        // ... other fields
    }
}
```

## Source Files

| File | Role |
|------|------|
| `velocity-workflow-core/src/slab.rs` | SlabHeader struct, Merkle root computation/verification |
| `velocity-workflow-core/src/bitmask.rs` | Bitmask256 with O(1) step tracking |
| `velocity-workflow-core/src/ffi.rs` | FFI bindings for slab verification |
| `velocity-workflow-engine/src/engine.rs` | WorkflowContext with slab integration |
| `velocity-workflow-engine/src/db_adapter.rs` | WorkflowRecord::from_context() persistence |
