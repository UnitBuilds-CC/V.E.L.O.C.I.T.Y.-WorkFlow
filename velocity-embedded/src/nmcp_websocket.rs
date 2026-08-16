//! NMCP WebSocket Server for Embedded Server — remote access via WebSocket.
//!
//! Shared implementation from `velocity-nmcp-protocol`. This module re-exports
//! the shared WebSocket types for use by Embedded server and clients.

// Re-export shared WebSocket types from the protocol crate.
pub use velocity_nmcp_protocol::ws::{NmcpWebSocketServer, NmcpWebSocketClient};
