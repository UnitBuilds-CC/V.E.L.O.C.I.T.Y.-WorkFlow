//! Example: Simple task worker using the VELOCITY-WorkFlow Rust SDK.
//!
//! Demonstrates:
//!   - Worker registration with a task queue
//!   - Polling for tasks in a loop
//!   - Executing task logic via registered handlers
//!   - Error handling with typed errors
//!
//! Run:
//!   cd VELOCITY-WorkFlow/sdk/rust
//!   cargo run --example simple_worker

use velocity_sdk::{VelocityClient, WorkflowStatus};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// ── Configuration ────────────────────────────────────────────────────────

const SERVER_ADDR: &str = "localhost:7234";
const TASK_QUEUE: &str = "orders";

// ── Task handler type ────────────────────────────────────────────────────

type TaskHandler = Box<dyn Fn(&Task) -> Result<String, String>>;

struct Task {
    workflow_key: u64,
    workflow_type: String,
    input: Vec<u8>,
}

// ── Task handlers ────────────────────────────────────────────────────────

fn process_order(task: &Task) -> Result<String, String> {
    let input_str = String::from_utf8_lossy(&task.input);
    println!("[worker] Processing order: {}", input_str);
    // Simulate work
    std::thread::sleep(std::time::Duration::from_millis(50));
    Ok(format!(r#"{{"status":"shipped","input":{}}}"#, input_str))
}

fn build_handlers() -> HashMap<&'static str, TaskHandler> {
    let mut handlers: HashMap<&'static str, TaskHandler> = HashMap::new();
    handlers.insert("order-processing", Box::new(process_order));
    handlers
}

// ── Worker loop ──────────────────────────────────────────────────────────

fn main() {
    println!("[worker] Starting VELOCITY-WorkFlow Rust worker");
    println!("[worker] Server: {} | Queue: {}", SERVER_ADDR, TASK_QUEUE);

    let running = Arc::new(AtomicBool::new(true));

    // Set up Ctrl+C handler
    let r = running.clone();
    ctrlc_handler(r);

    let client = VelocityClient::new();
    let handlers = build_handlers();

    println!("[worker] Registered on task queue '{}'", TASK_QUEUE);
    println!("[worker] Polling for tasks... (Ctrl+C to stop)");

    while running.load(Ordering::Relaxed) {
        // Poll for a task from the server
        match poll_task(&client) {
            Ok(Some(task)) => {
                let handler = handlers.get(task.workflow_type.as_str());
                match handler {
                    Some(h) => match h(&task) {
                        Ok(result) => {
                            println!(
                                "[worker] Task '{}' completed successfully",
                                task.workflow_type
                            );
                            let _ = client.complete_workflow(task.workflow_key, result.into_bytes());
                        }
                        Err(e) => {
                            eprintln!("[worker] Task execution error: {}", e);
                        }
                    },
                    None => {
                        eprintln!(
                            "[worker] No handler for task type '{}' — skipping",
                            task.workflow_type
                        );
                    }
                }
            }
            Ok(None) => {
                // No task available — sleep briefly
                std::thread::sleep(std::time::Duration::from_millis(500));
            }
            Err(e) => {
                eprintln!("[worker] Poll error: {}", e);
                std::thread::sleep(std::time::Duration::from_millis(1000));
            }
        }
    }

    client.destroy();
    println!("[worker] Shut down cleanly");
}

fn poll_task(client: &VelocityClient) -> Result<Option<Task>, String> {
    // In a full implementation, this calls client.poll_task(TASK_QUEUE, timeout)
    // For this example, we simulate the poll returning None (no task).
    Ok(None)
}

fn ctrlc_handler(running: Arc<AtomicBool>) {
    // Simplified — in production, use the `ctrlc` crate.
    // This is a placeholder that demonstrates the pattern.
    let _ = running; // Used in real implementation
}
