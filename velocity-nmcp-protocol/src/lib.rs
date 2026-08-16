//! Shared NMCP protocol types, frame parsing, shmem IPC, and WebSocket transport.
//!
//! This crate extracts the common NMCP protocol code shared between
//! `velocity-classic` and `velocity-embedded` servers:
//!
//! - `NmcpFrame` — binary frame format (16-byte header + JSON payload)
//! - `NmcpRequestBody` — parsed JSON request body
//! - `NmcpRouterStats` — per-router dispatch statistics
//! - `NmcpDispatch` — trait for frame dispatch (implemented by each flavor's router)
//! - `ShmemBuffer` — file-backed shared memory IPC buffer
//! - `NmcpShmemServer` / `NmcpShmemClient` — shmem IPC server and client
//! - `NmcpWebSocketServer` / `NmcpWebSocketClient` — WebSocket server and client

pub mod frame;
pub mod shmem;
pub mod ws;

// Re-export all public types at the crate root for convenience.
pub use frame::*;
pub use shmem::{ShmemBuffer, NmcpShmemServer, NmcpShmemClient, SHMEM_BUFFER_SIZE, MAX_REQ_PAYLOAD, MAX_RES_PAYLOAD};
pub use ws::{NmcpWebSocketServer, NmcpWebSocketClient};
