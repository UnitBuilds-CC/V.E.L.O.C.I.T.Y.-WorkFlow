//! Example: Basic workflow with signal and query using the VELOCITY-WorkFlow Rust SDK.
//!
//! Demonstrates:
//!   - Starting a workflow
//!   - Completing steps
//!   - Sending signals
//!   - Querying workflow state
//!   - Completing the workflow
//!
//! Run:
//!   cd VELOCITY-WorkFlow/sdk/rust
//!   cargo run --example basic_workflow

use velocity_sdk::{VelocityClient, WorkflowStatus};

fn main() {
    println!("=== VELOCITY-WorkFlow Rust SDK — Basic Workflow ===\n");

    let client = VelocityClient::new();

    // 1. Start a workflow (type_id=1, ns_id=1, tq_hash=42, steps=3)
    let key = client.start_workflow(1, 1, 42, 3);
    assert!(key > 0, "Workflow should start");
    println!("1. Workflow started: key={key}");

    // 2. Check initial status
    let status = client.get_status(key);
    assert_eq!(status, WorkflowStatus::Running);
    println!("2. Status: {status:?}");

    // 3. Complete steps
    client.complete_step(key, 0, b"step0_done".to_vec()).unwrap();
    client.complete_step(key, 1, b"step1_done".to_vec()).unwrap();
    println!("3. Steps 0 and 1 completed");

    // 4. Send a signal
    client.signal_workflow(key, 99, b"payment-confirmed".to_vec());
    println!("4. Signal sent: payment-confirmed (id=99)");

    // 5. Describe the workflow
    let desc = client.describe_workflow(key).unwrap();
    println!("5. Describe: step={}/{}, status={:?}", desc.current_step, desc.total_steps, desc.status);

    // 6. Complete the final step and workflow
    client.complete_step(key, 2, b"step2_done".to_vec()).unwrap();
    println!("6. All steps completed");

    // 7. Verify final status
    let final_status = client.get_status(key);
    println!("7. Final status: {final_status:?}");

    client.destroy();
    println!("\n=== Basic workflow example finished! ===");
}
