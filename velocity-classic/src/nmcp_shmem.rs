//! NMCP Shared Memory IPC — file-backed atomic IPC for co-located workers.
//!
//! Implements the 5-state atomic protocol from Velocity-Drone:
//!   IDLE → REQ_READY → PROCESSING → RES_READY → IDLE
//!
//! Layout (64KB total):
//!   Offset 0:       request state byte
//!   Offset 1-4:     request payload length (i32 LE)
//!   Offset 5-4096:  request payload (4092 bytes max)
//!   Offset 4100:    response state byte
//!   Offset 4101-4:  response payload length (i32 LE)
//!   Offset 4105+:   response payload (61431 bytes max)

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::nmcp_router::{NmcpFrame, NmcpFrameRouter};

// ─── Constants ───────────────────────────────────────────────────────────────

/// Total shared memory buffer size (64KB).
pub const SHMEM_BUFFER_SIZE: usize = 65536;

/// Request state offset.
const REQ_STATE_OFFSET: usize = 0;
/// Request length offset.
const REQ_LEN_OFFSET: usize = 1;
/// Request payload offset.
const REQ_PAYLOAD_OFFSET: usize = 5;
/// Max request payload size.
pub const MAX_REQ_PAYLOAD: usize = 4092;

/// Response state offset.
const RES_STATE_OFFSET: usize = 4100;
/// Response length offset.
const RES_LEN_OFFSET: usize = 4101;
/// Response payload offset.
const RES_PAYLOAD_OFFSET: usize = 4105;
/// Max response payload size.
pub const MAX_RES_PAYLOAD: usize = 61431;

// State machine constants
const STATE_IDLE: u8 = 0;
const STATE_REQ_READY: u8 = 1;
const STATE_PROCESSING: u8 = 2;
const STATE_RES_READY: u8 = 3;
const STATE_ERROR: u8 = 4;

/// Polling interval when waiting for state transitions.
const POLL_INTERVAL: Duration = Duration::from_micros(100);

// ─── Shared Memory Buffer ────────────────────────────────────────────────────

/// A file-backed shared memory buffer for IPC.
///
/// Uses a regular file with read/write/seek for cross-process communication.
/// On Linux, place this on a tmpfs mount for true shared memory semantics.
pub struct ShmemBuffer {
    file: File,
    buf: Vec<u8>,
}

impl ShmemBuffer {
    /// Create or open a shared memory buffer at the given path.
    pub fn open(path: &str, size: usize) -> Result<Self, String> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|e| format!("Failed to open shmem file {}: {}", path, e))?;

        // Ensure file is the right size
        let meta = file.metadata().map_err(|e| format!("metadata: {}", e))?;
        if meta.len() < size as u64 {
            let mut f = OpenOptions::new()
                .write(true)
                .open(path)
                .map_err(|e| format!("open for resize: {}", e))?;
            f.seek(std::io::SeekFrom::End(0)).map_err(|e| format!("seek: {}", e))?;
            let needed = size as u64 - meta.len();
            let zeros = vec![0u8; needed as usize];
            f.write_all(&zeros).map_err(|e| format!("write zeros: {}", e))?;
            f.flush().map_err(|e| format!("flush: {}", e))?;
        }

        Ok(Self {
            file,
            buf: vec![0u8; size],
        })
    }

    /// Read a single byte at the given offset.
    pub fn read_byte(&mut self, offset: usize) -> Result<u8, String> {
        use std::io::Seek;
        self.file
            .seek(std::io::SeekFrom::Start(offset as u64))
            .map_err(|e| format!("seek: {}", e))?;
        let mut byte = [0u8; 1];
        self.file
            .read_exact(&mut byte)
            .map_err(|e| format!("read: {}", e))?;
        Ok(byte[0])
    }

    /// Write a single byte at the given offset.
    pub fn write_byte(&mut self, offset: usize, value: u8) -> Result<(), String> {
        use std::io::Seek;
        self.file
            .seek(std::io::SeekFrom::Start(offset as u64))
            .map_err(|e| format!("seek: {}", e))?;
        self.file
            .write_all(&[value])
            .map_err(|e| format!("write: {}", e))?;
        self.file.flush().map_err(|e| format!("flush: {}", e))?;
        Ok(())
    }

    /// Read bytes at the given offset.
    pub fn read_bytes(&mut self, offset: usize, len: usize) -> Result<Vec<u8>, String> {
        use std::io::Seek;
        self.file
            .seek(std::io::SeekFrom::Start(offset as u64))
            .map_err(|e| format!("seek: {}", e))?;
        let mut buf = vec![0u8; len];
        self.file
            .read_exact(&mut buf)
            .map_err(|e| format!("read: {}", e))?;
        Ok(buf)
    }

    /// Write bytes at the given offset.
    pub fn write_bytes(&mut self, offset: usize, data: &[u8]) -> Result<(), String> {
        use std::io::Seek;
        self.file
            .seek(std::io::SeekFrom::Start(offset as u64))
            .map_err(|e| format!("seek: {}", e))?;
        self.file
            .write_all(data)
            .map_err(|e| format!("write: {}", e))?;
        self.file.flush().map_err(|e| format!("flush: {}", e))?;
        Ok(())
    }

    /// Read a u32 LE at the given offset.
    pub fn read_u32(&mut self, offset: usize) -> Result<u32, String> {
        let bytes = self.read_bytes(offset, 4)?;
        Ok(u32::from_le_bytes(bytes.try_into().map_err(|_| "bad u32 read")?))
    }

    /// Write a u32 LE at the given offset.
    pub fn write_u32(&mut self, offset: usize, value: u32) -> Result<(), String> {
        self.write_bytes(offset, &value.to_le_bytes())
    }
}

// ─── Shmem Server ────────────────────────────────────────────────────────────

/// NMCP Shared Memory Server — handles IPC from co-located workers.
///
/// Polls the shared memory buffer for incoming requests, dispatches them
/// through the frame router, and writes responses back.
pub struct NmcpShmemServer {
    router: Arc<NmcpFrameRouter>,
    buffer_path: String,
    running: AtomicBool,
}

impl NmcpShmemServer {
    /// Create a new shmem server.
    pub fn new(router: Arc<NmcpFrameRouter>, buffer_path: String) -> Self {
        Self {
            router,
            buffer_path,
            running: AtomicBool::new(true),
        }
    }

    /// Shut down the server.
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Run the shmem IPC server loop.
    ///
    /// Polls for REQ_READY state, reads request, dispatches, writes response.
    pub fn run(&self) {
        let mut buffer = match ShmemBuffer::open(&self.buffer_path, SHMEM_BUFFER_SIZE) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("Failed to open shmem buffer: {}", e);
                return;
            }
        };

        // Initialize to IDLE state
        let _ = buffer.write_byte(REQ_STATE_OFFSET, STATE_IDLE);
        let _ = buffer.write_byte(RES_STATE_OFFSET, STATE_IDLE);

        while self.running.load(Ordering::Relaxed) {
            // Poll for REQ_READY
            let req_state = match buffer.read_byte(REQ_STATE_OFFSET) {
                Ok(s) => s,
                Err(_) => {
                    std::thread::sleep(POLL_INTERVAL);
                    continue;
                }
            };

            if req_state != STATE_REQ_READY {
                std::thread::sleep(POLL_INTERVAL);
                continue;
            }

            // Transition to PROCESSING
            let _ = buffer.write_byte(REQ_STATE_OFFSET, STATE_PROCESSING);

            // Read request length and payload
            let req_len = match buffer.read_u32(REQ_LEN_OFFSET) {
                Ok(l) => l as usize,
                Err(_) => {
                    let _ = buffer.write_byte(REQ_STATE_OFFSET, STATE_ERROR);
                    continue;
                }
            };

            if req_len > MAX_REQ_PAYLOAD {
                let _ = buffer.write_byte(REQ_STATE_OFFSET, STATE_ERROR);
                continue;
            }

            let req_data = match buffer.read_bytes(REQ_PAYLOAD_OFFSET, req_len) {
                Ok(d) => d,
                Err(_) => {
                    let _ = buffer.write_byte(REQ_STATE_OFFSET, STATE_ERROR);
                    continue;
                }
            };

            // Parse the NMCP frame from the request payload
            let request_frame = match NmcpFrame::from_bytes(&req_data) {
                Some(f) => f,
                None => {
                    // Write error response
                    let err = NmcpFrame::error_response(0, 400, "invalid NMCP frame");
                    let err_bytes = err.to_bytes();
                    let _ = buffer.write_u32(RES_LEN_OFFSET, err_bytes.len() as u32);
                    let _ = buffer.write_bytes(RES_PAYLOAD_OFFSET, &err_bytes);
                    let _ = buffer.write_byte(RES_STATE_OFFSET, STATE_RES_READY);
                    let _ = buffer.write_byte(REQ_STATE_OFFSET, STATE_IDLE);
                    continue;
                }
            };

            // Dispatch through the router
            let response_frame = self.router.dispatch(&request_frame);
            let resp_bytes = response_frame.to_bytes();

            // Write response
            if resp_bytes.len() <= MAX_RES_PAYLOAD {
                let _ = buffer.write_u32(RES_LEN_OFFSET, resp_bytes.len() as u32);
                let _ = buffer.write_bytes(RES_PAYLOAD_OFFSET, &resp_bytes);
            } else {
                // Truncate oversized responses
                let truncated = &resp_bytes[..MAX_RES_PAYLOAD];
                let _ = buffer.write_u32(RES_LEN_OFFSET, MAX_RES_PAYLOAD as u32);
                let _ = buffer.write_bytes(RES_PAYLOAD_OFFSET, truncated);
            }

            // Transition to RES_READY
            let _ = buffer.write_byte(RES_STATE_OFFSET, STATE_RES_READY);

            // Wait for client to consume the response (return to IDLE)
            loop {
                if !self.running.load(Ordering::Relaxed) {
                    break;
                }
                let rs = buffer.read_byte(RES_STATE_OFFSET).unwrap_or(STATE_ERROR);
                if rs == STATE_IDLE {
                    break;
                }
                std::thread::sleep(POLL_INTERVAL);
            }

            // Reset request state to IDLE
            let _ = buffer.write_byte(REQ_STATE_OFFSET, STATE_IDLE);
        }
    }
}

// ─── Shmem Client ────────────────────────────────────────────────────────────

/// NMCP Shared Memory Client — sends requests to the shmem server.
///
/// Used by co-located workers for zero-network-overhead IPC.
pub struct NmcpShmemClient {
    buffer_path: String,
    next_seq: std::sync::atomic::AtomicU32,
}

impl NmcpShmemClient {
    /// Create a new shmem client.
    pub fn new(buffer_path: String) -> Self {
        Self {
            buffer_path,
            next_seq: std::sync::atomic::AtomicU32::new(1),
        }
    }

    /// Send a request and wait for response.
    pub fn call(&self, frame_type: u32, payload: Vec<u8>) -> Result<NmcpFrame, String> {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let request = NmcpFrame::new(frame_type, seq, payload);
        let req_bytes = request.to_bytes();

        if req_bytes.len() > MAX_REQ_PAYLOAD {
            return Err(format!(
                "request too large: {} > {}",
                req_bytes.len(),
                MAX_REQ_PAYLOAD
            ));
        }

        let mut buffer = ShmemBuffer::open(&self.buffer_path, SHMEM_BUFFER_SIZE)?;

        // Wait for IDLE state
        for _ in 0..10000 {
            let state = buffer.read_byte(REQ_STATE_OFFSET)?;
            if state == STATE_IDLE {
                break;
            }
            std::thread::sleep(POLL_INTERVAL);
        }

        // Write request
        buffer.write_bytes(REQ_PAYLOAD_OFFSET, &req_bytes)?;
        buffer.write_u32(REQ_LEN_OFFSET, req_bytes.len() as u32)?;
        buffer.write_byte(REQ_STATE_OFFSET, STATE_REQ_READY)?;

        // Wait for RES_READY
        for _ in 0..100000 {
            let state = buffer.read_byte(RES_STATE_OFFSET)?;
            if state == STATE_RES_READY {
                break;
            }
            if state == STATE_ERROR {
                return Err("server error".to_string());
            }
            std::thread::sleep(POLL_INTERVAL);
        }

        // Read response
        let resp_len = buffer.read_u32(RES_LEN_OFFSET)? as usize;
        let resp_data = buffer.read_bytes(RES_PAYLOAD_OFFSET, resp_len)?;

        // Reset to IDLE (signal server we consumed the response)
        buffer.write_byte(RES_STATE_OFFSET, STATE_IDLE)?;

        NmcpFrame::from_bytes(&resp_data).ok_or_else(|| "invalid response frame".to_string())
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nmcp_router::ClassicFrameTypes;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU64;
    use std::sync::{Arc, Mutex};
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
        let map = Arc::new(Mutex::new(HashMap::new()));
        let counter = Arc::new(AtomicU64::new(1));
        let router = Arc::new(NmcpFrameRouter::new(engine, map, counter));

        let server = NmcpShmemServer::new(router, path.to_string());

        // Run server in background thread
        let server_running = server.running.load(Ordering::Relaxed);
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
        // We can't directly access server.running from here, so just let it timeout
        let _ = std::fs::remove_file(path);
    }
}
