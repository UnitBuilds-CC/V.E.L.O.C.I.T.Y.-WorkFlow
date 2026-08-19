//! Auto-apply workflow and activity registration for the VELOCITY Rust SDK.
//!
//! Provides a trait-based registration system where workflow and activity
//! implementations register themselves into a global registry. The Worker
//! discovers all registered handlers at startup — no manual wiring needed.
//!
//! # Example
//!
//! ```rust,no_run
//! use velocity_sdk::auto_apply::{register_workflow, register_activity, WorkflowHandler, ActivityHandler};
//!
//! struct OrderWorkflow;
//! impl WorkflowHandler for OrderWorkflow {
//!     fn workflow_type() -> &'static str { "OrderWorkflow" }
//!     // ... execute implementation
//! }
//!
//! fn process_payment(activity_id: &str, input: &[u8]) -> Vec<u8> {
//!     b"charged".to_vec()
//! }
//!
//! // Register at startup (or use the inventory crate for link-time registration)
//! register_workflow::<OrderWorkflow>();
//! register_activity("process_payment", process_payment);
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

// ─── Handler Traits ──────────────────────────────────────────────────────────

/// Trait for workflow implementations.
///
/// Implement this trait on a struct to make it discoverable by the Worker.
/// The `workflow_type()` method returns the type name used for dispatch.
pub trait WorkflowHandler: Send + Sync + 'static {
    /// The workflow type name used for dispatch (e.g., "OrderWorkflow").
    fn workflow_type() -> &'static str;

    /// Execute the workflow with the given context and input.
    fn execute(
        &self,
        ctx: &WorkflowHandlerContext,
        input: &[u8],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Trait for activity implementations.
///
/// Activities are plain functions that perform non-deterministic work.
pub type ActivityHandler = Arc<dyn Fn(&ActivityHandlerContext, &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> + Send + Sync>;

// ─── Context Types ───────────────────────────────────────────────────────────

/// Context available inside workflow execution.
#[derive(Debug, Clone)]
pub struct WorkflowHandlerContext {
    pub workflow_key: u64,
    pub workflow_id: String,
    pub run_id: String,
    pub workflow_type: String,
    pub task_queue: String,
    pub current_step: u32,
}

/// Context available inside activity execution.
#[derive(Debug, Clone)]
pub struct ActivityHandlerContext {
    pub activity_type: String,
    pub activity_id: String,
    pub attempt: u32,
}

// ─── Global Registry ─────────────────────────────────────────────────────────

/// Factory function that creates a workflow handler instance.
type WorkflowFactory = Arc<dyn Fn() -> Box<dyn WorkflowHandlerInstance> + Send + Sync>;

/// Object-safe version of WorkflowHandler for dynamic dispatch.
pub trait WorkflowHandlerInstance: Send + Sync {
    fn execute(
        &self,
        ctx: &WorkflowHandlerContext,
        input: &[u8],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Blanket implementation: any WorkflowHandler is also a WorkflowHandlerInstance.
impl<T: WorkflowHandler> WorkflowHandlerInstance for T {
    fn execute(
        &self,
        ctx: &WorkflowHandlerContext,
        input: &[u8],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        WorkflowHandler::execute(self, ctx, input)
    }
}

struct Registry {
    workflows: HashMap<String, WorkflowFactory>,
    activities: HashMap<String, ActivityHandler>,
}

static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();

fn registry() -> &'static Mutex<Registry> {
    REGISTRY.get_or_init(|| {
        Mutex::new(Registry {
            workflows: HashMap::new(),
            activities: HashMap::new(),
        })
    })
}

// ─── Registration Functions ──────────────────────────────────────────────────

/// Register a workflow type in the global registry.
///
/// Call this at startup (e.g., in `main()`) or use the `inventory` crate
/// for link-time auto-registration.
pub fn register_workflow<T: WorkflowHandler>() {
    let factory: WorkflowFactory = Arc::new(|| Box::new(T) as Box<dyn WorkflowHandlerInstance>);
    let mut reg = registry().lock().unwrap();
    reg.workflows.insert(T::workflow_type().to_string(), factory);
}

/// Register an activity function in the global registry.
pub fn register_activity(name: &str, handler: impl Fn(&ActivityHandlerContext, &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static) {
    let mut reg = registry().lock().unwrap();
    reg.activities.insert(name.to_string(), Arc::new(handler));
}

/// Get all registered workflow type names.
pub fn registered_workflow_types() -> Vec<String> {
    let reg = registry().lock().unwrap();
    reg.workflows.keys().cloned().collect()
}

/// Get all registered activity names.
pub fn registered_activity_names() -> Vec<String> {
    let reg = registry().lock().unwrap();
    reg.activities.keys().cloned().collect()
}

/// Create a workflow handler instance by type name.
pub fn create_workflow(workflow_type: &str) -> Option<Box<dyn WorkflowHandlerInstance>> {
    let reg = registry().lock().unwrap();
    reg.workflows.get(workflow_type).map(|factory| factory())
}

/// Get an activity handler by name.
pub fn get_activity(name: &str) -> Option<ActivityHandler> {
    let reg = registry().lock().unwrap();
    reg.activities.get(name).cloned()
}

/// Clear both registries (useful for testing).
pub fn clear_registries() {
    let mut reg = registry().lock().unwrap();
    reg.workflows.clear();
    reg.activities.clear();
}

/// Count of registered workflows.
pub fn workflow_count() -> usize {
    let reg = registry().lock().unwrap();
    reg.workflows.len()
}

/// Count of registered activities.
pub fn activity_count() -> usize {
    let reg = registry().lock().unwrap();
    reg.activities.len()
}
