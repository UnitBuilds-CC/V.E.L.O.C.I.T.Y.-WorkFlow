//! NMCP Shared Memory IPC for Embedded Server — file-backed atomic IPC.
//!
//! Shared implementation from `velocity-nmcp-protocol`. This module re-exports
//! the shared types and keeps Embedded-specific test code.
//!
//! Same 5-state protocol as classic:
//!   IDLE → REQ_READY → PROCESSING → RES_READY → IDLE

// Re-export shared shmem types from the protocol crate.
pub use velocity_nmcp_protocol::shmem::{
    ShmemBuffer, NmcpShmemServer, NmcpShmemClient,
    SHMEM_BUFFER_SIZE, MAX_REQ_PAYLOAD, MAX_RES_PAYLOAD,
};

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nmcp_router::NmcpFrameRouter;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;
    use dashmap::DashMap;
    use velocity_workflow_engine::engine::WorkflowEngine;

    #[test]
    fn test_shmem_buffer_create() {
        let path = format!("/tmp/velocity-emb-test-{}.nmcp", std::process::id());
        let buffer = ShmemBuffer::open(&path, SHMEM_BUFFER_SIZE);
        assert!(buffer.is_ok());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_shmem_read_write() {
        let path = format!("/tmp/velocity-emb-rw-{}.nmcp", std::process::id());
        let mut buffer = ShmemBuffer::open(&path, SHMEM_BUFFER_SIZE).unwrap();

        buffer.write_byte(0, 42).unwrap();
        assert_eq!(buffer.read_byte(0).unwrap(), 42);

        buffer.write_u32(1, 12345).unwrap();
        assert_eq!(buffer.read_u32(1).unwrap(), 12345);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_shmem_server_creation() {
        let path = format!("/tmp/velocity-emb-sc-{}.nmcp", std::process::id());

        let engine = Arc::new(WorkflowEngine::new());
        let workflow_map = Arc::new(DashMap::new());
        let workflow_counter = Arc::new(AtomicU64::new(1));
        let router = Arc::new(NmcpFrameRouter::new(engine, workflow_map, workflow_counter));

        let server = NmcpShmemServer::new(router, path.clone());
        // Verify the server was created successfully
        server.shutdown();
        let _ = std::fs::remove_file(&path);
    }
}
