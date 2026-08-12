//! Example: Multi-step saga with compensation using the VELOCITY-WorkFlow Rust SDK.
//!
//! Demonstrates:
//!   - Defining a saga with compensable steps
//!   - Executing steps in order
//!   - Triggering compensation on failure
//!   - Rolling back completed steps in reverse order
//!
//! Run:
//!   cd VELOCITY-WorkFlow/sdk/rust
//!   cargo run --example saga_pattern

use velocity_sdk::{VelocityClient, WorkflowStatus};

/// A saga step with a forward action and a compensation action.
struct SagaStep {
    name: &'static str,
    compensate: &'static str,
    signal_id: u64,
    compensate_id: u64,
}

const STEPS: &[SagaStep] = &[
    SagaStep { name: "reserve_inventory", compensate: "release_inventory", signal_id: 100, compensate_id: 200 },
    SagaStep { name: "charge_payment",    compensate: "refund_payment",    signal_id: 101, compensate_id: 201 },
    SagaStep { name: "book_shipping",     compensate: "cancel_shipping",   signal_id: 102, compensate_id: 202 },
    SagaStep { name: "send_confirmation", compensate: "send_cancellation", signal_id: 103, compensate_id: 203 },
];

/// Run the saga. If `simulate_failure_at` is Some(i), step i will fail.
fn run_saga(client: &VelocityClient, simulate_failure_at: Option<usize>) -> bool {
    let key = client.start_workflow(1, 1, 42, STEPS.len() as u32);
    assert!(key > 0);
    println!("  Saga started: key={key}");

    let mut completed: Vec<usize> = Vec::new();

    for (i, step) in STEPS.iter().enumerate() {
        // Simulate failure
        if simulate_failure_at == Some(i) {
            println!("\n   ✗ Step '{}' FAILED — triggering compensation", step.name);
            // Compensate in reverse order
            for &prev_idx in completed.iter().rev() {
                let prev = &STEPS[prev_idx];
                println!("   Compensating: {}", prev.compensate);
                client.signal_workflow(key, prev.compensate_id, prev.compensate.as_bytes().to_vec());
            }
            return false;
        }

        println!("   Executing: {}", step.name);
        client.signal_workflow(key, step.signal_id, step.name.as_bytes().to_vec());
        completed.push(i);
    }

    println!("   ✓ All saga steps completed successfully");
    true
}

fn main() {
    println!("=== VELOCITY-WorkFlow Rust SDK — Saga Pattern ===\n");

    let client = VelocityClient::new();

    // Scenario 1: Happy path
    println!("Scenario 1: Happy path");
    run_saga(&client, None);

    // Scenario 2: Payment step fails (index=1)
    println!("\nScenario 2: Payment step fails (index=1)");
    run_saga(&client, Some(1));

    client.destroy();
    println!("\n=== Saga examples finished! ===");
}
