//! # VELOCITY-WorkFlow Rust SDK
//!
//! Native Rust client for the VELOCITY-WorkFlow engine.
//! Unlike the gRPC-based SDKs (Python, Go, TypeScript, Java), this SDK links
//! directly against `velocity-workflow-engine` as a library — zero network overhead.
//!
//! ## Quick start
//!
//! ```rust,no_run
//! use velocity_sdk::{VelocityClient, WorkflowStatus};
//!
//! let mut client = VelocityClient::new();
//! let key = client.start_workflow(42, 1, 99, 5);
//! client.complete_step(key, 0, b"ok".to_vec());
//! assert_eq!(client.get_status(key), WorkflowStatus::Running);
//! client.destroy();
//! ```

pub mod client;
pub mod errors;
pub mod interceptors;
pub mod testing;
pub mod retry;
pub mod codec;
pub mod update;

#[cfg(test)]
mod tests;

// ─── Re-exports ──────────────────────────────────────────────────────────────

pub use client::{VelocityClient, WorkflowHandle, WorkflowDescription};
pub use errors::{VelocityError, ErrorKind};
pub use interceptors::{
    WorkflowInterceptor, ActivityInterceptor,
    LoggingInterceptor, MetricsInterceptor, InterceptorChain,
};
pub use testing::{TestWorkflowEnvironment, MockClient};
pub use retry::{RetryPolicy, RetryPolicyBuilder, execute_with_retry, execute_with_retry_if};
pub use codec::{PayloadCodec, JsonCodec, BinaryCodec, NullCodec, CodecChain, CodecError};
pub use update::{UpdateClient, UpdateRequest, UpdateResult, UpdateStatus, UpdateWaitPolicy};

/// Re-export the engine's WorkflowStatus so consumers don't need a direct dep.
pub use velocity_workflow_engine::WorkflowStatus;

/// Crate version, kept in sync with Cargo.toml.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
