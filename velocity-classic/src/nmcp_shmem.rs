//! NMCP Shared Memory IPC — file-backed atomic IPC for co-located workers.
//!
//! Shared implementation from `velocity-nmcp-protocol`. This module re-exports
//! the shared types and keeps Classic-specific test code.
//!
//! Implements the 5-state atomic protocol:
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
    use crate::nmcp_router::{ClassicFrameTypes, NmcpFrame, NmcpFrameRouter};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use dashmap::DashMap;
    use velocity_workflow_engine::engine::WorkflowEngine;

    #[test]
    fn test_shmem_buffer_create() {
        let path = "/tmp/velocity_test_shmem_create.buf";
        let buffer = ShmemBuffer::open(path, SHMEM_BUFFER_SIZE).unwrap();
        drop(buffer);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_shmem_buffer_read_write() {
        let path = "/tmp/velocity_test_shmem_rw.buf";
        let mut buffer = ShmemBuffer::open(path, SHMEM_BUFFER_SIZE).unwrap();

        buffer.write_byte(0, 42).unwrap();
        assert_eq!(buffer.read_byte(0).unwrap(), 42);

        buffer.write_u32(10, 12345).unwrap();
        assert_eq!(buffer.read_u32(10).unwrap(), 12345);

        let data = b"hello world";
        buffer.write_bytes(100, data).unwrap();
        let read_back = buffer.read_bytes(100, data.len()).unwrap();
        assert_eq!(read_back, data);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_shmem_server_client_roundtrip() {
        let path = "/tmp/velocity_test_shmem_roundtrip.buf";

        // Clean up any existing file
        let _ = std::fs::remove_file(path);

        let engine = Arc::new(WorkflowEngine::new());
        let map = Arc::new(DashMap::new());
        let counter = Arc::new(AtomicU64::new(1));
        let router = Arc::new(NmcpFrameRouter::new(engine, map, counter));

        let server = NmcpShmemServer::new(router, path.to_string());

        // Run server in background thread
        let server_running = server.is_running();
        let server_handle = {
            let path = path.to_string();
            std::thread::spawn(move || {
                server.run();
            })
        };

        // Give server time to initialize
        std::thread::sleep(Duration::from_millis(50));

        // Client: send health check
        let client = NmcpShmemClient::new(path.to_string());
        let resp = client.call(ClassicFrameTypes::HEALTH_CHECK, b"{}".to_vec()).unwrap();
        let body: serde_json::Value = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(body["success"], true);
        assert_eq!(body["data"]["transport"], "nmcp");

        // Client: start a workflow
        let start_body = serde_json::json!({"workflow_id": "shmem-wf-1", "workflow_type": "bench"});
        let start_frame = NmcpFrame::new(
            ClassicFrameTypes::START_WORKFLOW,
            2,
            serde_json::to_vec(&start_body).unwrap(),
        );
        let resp = client.call(ClassicFrameTypes::START_WORKFLOW, start_frame.payload).unwrap();
        let body: serde_json::Value = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(body["success"], true);
        assert_eq!(body["data"]["status"], "COMPLETED");

        // Shut down
        let _ = std::fs::remove_file(path);
    }
}
