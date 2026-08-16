//! NMCP Shared Memory IPC — file-backed ring-buffer IPC for co-located workers.
//!
//! Implements a 64-slot × 4KB SPSC ring buffer:
//!   - Client writes requests to slots[write_idx], advances write_idx
//!   - Server reads from slots[read_idx], dispatches, writes response, advances read_idx
//!   - Client reads response from its slot, marks EMPTY
//!
//! Layout (256KB + 64-byte header):
//!   Header (64 bytes):
//!     Offset 0-3:  write_idx (u32 LE) — next slot for client to write
//!     Offset 4-7:  read_idx  (u32 LE) — next slot for server to read
//!     Offset 8-11: capacity  (u32 LE) — always 64
//!     Offset 12-15: flags    (u32 LE) — reserved (0)
//!   Slots (64 × 4096 bytes):
//!     Offset 0:     state byte (0=EMPTY, 1=REQ_READY, 2=PROCESSING, 3=RES_READY, 4=ERROR)
//!     Offset 1-4:   payload length (u32 LE)
//!     Offset 5-12:  sequence number (u64 LE) — for response matching
//!     Offset 13-4095: payload (4083 bytes max per slot)

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::frame::{NmcpDispatch, NmcpFrame};

// ─── Constants ───────────────────────────────────────────────────────────────

/// Number of slots in the ring buffer.
pub const RING_SLOTS: usize = 64;

/// Size of each slot (4KB).
pub const SLOT_SIZE: usize = 4096;

/// Header size (64 bytes).
const HEADER_SIZE: usize = 64;

/// Total shared memory buffer size.
pub const SHMEM_BUFFER_SIZE: usize = HEADER_SIZE + RING_SLOTS * SLOT_SIZE;

/// Max payload per slot (slot - 1 state - 4 length - 8 seq = 4083).
pub const MAX_REQ_PAYLOAD: usize = SLOT_SIZE - 13;

/// Max response payload (same as request).
pub const MAX_RES_PAYLOAD: usize = SLOT_SIZE - 13;

// Header field offsets
const WRITE_IDX_OFFSET: usize = 0;
const READ_IDX_OFFSET: usize = 4;
const CAPACITY_OFFSET: usize = 8;

// Slot field offsets (relative to slot start)
const SLOT_STATE_OFFSET: usize = 0;
const SLOT_LEN_OFFSET: usize = 1;
const SLOT_SEQ_OFFSET: usize = 5;
const SLOT_PAYLOAD_OFFSET: usize = 13;

// Slot states
const SLOT_EMPTY: u8 = 0;
const SLOT_REQ_READY: u8 = 1;
const SLOT_PROCESSING: u8 = 2;
const SLOT_RES_READY: u8 = 3;
const SLOT_ERROR: u8 = 4;

/// Maximum iterations to spin-wait (no sleep) before backing off.
const MAX_SPIN_ITERATIONS: u32 = 10;

/// Maximum backoff duration for idle polling.
const MAX_BACKOFF: Duration = Duration::from_millis(1);

/// Initial backoff duration for exponential backoff.
const INITIAL_BACKOFF: Duration = Duration::from_nanos(100);

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

        let meta = file.metadata().map_err(|e| format!("metadata: {}", e))?;
        if meta.len() < size as u64 {
            let mut f = OpenOptions::new()
                .write(true)
                .open(path)
                .map_err(|e| format!("open for resize: {}", e))?;
            f.seek(std::io::SeekFrom::End(0))
                .map_err(|e| format!("seek: {}", e))?;
            let needed = size as u64 - meta.len();
            let zeros = vec![0u8; needed as usize];
            f.write_all(&zeros)
                .map_err(|e| format!("write zeros: {}", e))?;
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

    /// Read a u64 LE at the given offset.
    pub fn read_u64(&mut self, offset: usize) -> Result<u64, String> {
        let bytes = self.read_bytes(offset, 8)?;
        Ok(u64::from_le_bytes(bytes.try_into().map_err(|_| "bad u64 read")?))
    }

    /// Write a u64 LE at the given offset.
    pub fn write_u64(&mut self, offset: usize, value: u64) -> Result<(), String> {
        self.write_bytes(offset, &value.to_le_bytes())
    }
}

// ─── Ring Buffer Helpers ─────────────────────────────────────────────────────

/// Get the file offset for a slot's state byte.
#[inline]
fn slot_offset(slot_idx: usize) -> usize {
    HEADER_SIZE + slot_idx * SLOT_SIZE
}

/// Read the write index from the header.
fn read_write_idx(buf: &mut ShmemBuffer) -> Result<u32, String> {
    buf.read_u32(WRITE_IDX_OFFSET)
}

/// Read the read index from the header.
fn read_read_idx(buf: &mut ShmemBuffer) -> Result<u32, String> {
    buf.read_u32(READ_IDX_OFFSET)
}

/// Write the write index to the header.
fn write_write_idx(buf: &mut ShmemBuffer, idx: u32) -> Result<(), String> {
    buf.write_u32(WRITE_IDX_OFFSET, idx)
}

/// Write the read index to the header.
fn write_read_idx(buf: &mut ShmemBuffer, idx: u32) -> Result<(), String> {
    buf.write_u32(READ_IDX_OFFSET, idx)
}

// ─── Adaptive Wait Helper ────────────────────────────────────────────────────

/// Wait for a slot's state byte to match a predicate using adaptive backoff.
/// Returns `(state_byte, was_contended)`.
fn wait_for_slot_state<F>(
    buffer: &mut ShmemBuffer,
    slot_idx: usize,
    predicate: F,
    running: &AtomicBool,
) -> (u8, bool)
where
    F: Fn(u8) -> bool,
{
    let offset = slot_offset(slot_idx) + SLOT_STATE_OFFSET;
    let mut backoff = INITIAL_BACKOFF;

    // Phase 1: Spin-wait for hot path (no sleep)
    for _ in 0..MAX_SPIN_ITERATIONS {
        if !running.load(Ordering::Relaxed) {
            return (0xFF, false);
        }
        match buffer.read_byte(offset) {
            Ok(s) if predicate(s) => return (s, false),
            _ => std::hint::spin_loop(),
        }
    }

    // Phase 2: Exponential backoff (contention detected)
    while backoff < MAX_BACKOFF {
        if !running.load(Ordering::Relaxed) {
            return (0xFF, true);
        }
        std::thread::sleep(backoff);
        match buffer.read_byte(offset) {
            Ok(s) if predicate(s) => return (s, true),
            _ => backoff = backoff.mul_f32(2.0),
        }
    }

    // Phase 3: Steady-state polling at MAX_BACKOFF
    loop {
        if !running.load(Ordering::Relaxed) {
            return (0xFF, true);
        }
        std::thread::sleep(MAX_BACKOFF);
        match buffer.read_byte(offset) {
            Ok(s) if predicate(s) => return (s, true),
            Ok(_) => continue,
            Err(_) => return (0xFF, true),
        }
    }
}

// ─── Shmem Server ────────────────────────────────────────────────────────────

/// NMCP Shared Memory Server — handles IPC from co-located workers.
///
/// Uses a 64-slot ring buffer for concurrent request processing.
/// The server reads from `read_idx`, processes, writes response, advances.
pub struct NmcpShmemServer<D: NmcpDispatch> {
    router: Arc<D>,
    buffer_path: String,
    running: AtomicBool,
    /// Number of times the shmem IPC wait went past spin-wait into backoff.
    contentions_total: AtomicU64,
}

impl<D: NmcpDispatch> NmcpShmemServer<D> {
    /// Create a new shmem server.
    pub fn new(router: Arc<D>, buffer_path: String) -> Self {
        Self {
            router,
            buffer_path,
            running: AtomicBool::new(true),
            contentions_total: AtomicU64::new(0),
        }
    }

    /// Shut down the server.
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Whether the server is still running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Total shmem IPC contention events (wait exceeded spin-wait phase).
    pub fn contentions_total(&self) -> u64 {
        self.contentions_total.load(Ordering::Relaxed)
    }

    /// Run the shmem IPC server loop (ring buffer mode).
    ///
    /// Reads requests from slots[read_idx], dispatches, writes responses,
    /// and advances read_idx. Slots wrap around at RING_SLOTS.
    pub fn run(&self) {
        let mut buffer = match ShmemBuffer::open(&self.buffer_path, SHMEM_BUFFER_SIZE) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("Failed to open shmem buffer: {}", e);
                return;
            }
        };

        // Initialize header: capacity + zero indices
        let _ = buffer.write_u32(CAPACITY_OFFSET, RING_SLOTS as u32);
        let _ = write_write_idx(&mut buffer, 0);
        // Initialize all slots to EMPTY
        for i in 0..RING_SLOTS {
            let _ = buffer.write_byte(slot_offset(i) + SLOT_STATE_OFFSET, SLOT_EMPTY);
        }
        // Re-open to get a clean handle after header init
        let mut buffer = match ShmemBuffer::open(&self.buffer_path, SHMEM_BUFFER_SIZE) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("Failed to re-open shmem buffer: {}", e);
                return;
            }
        };

        let mut read_idx: u32 = 0;

        while self.running.load(Ordering::Relaxed) {
            // Wait for the current read_idx slot to have REQ_READY
            let base = slot_offset(read_idx as usize);
            let (state, contended) = wait_for_slot_state(
                &mut buffer,
                read_idx as usize,
                |s| s == SLOT_REQ_READY,
                &self.running,
            );
            if contended {
                self.contentions_total.fetch_add(1, Ordering::Relaxed);
            }

            if !self.running.load(Ordering::Relaxed) {
                break;
            }
            if state != SLOT_REQ_READY {
                continue;
            }

            // Mark slot as PROCESSING
            let _ = buffer.write_byte(base + SLOT_STATE_OFFSET, SLOT_PROCESSING);

            // Read sequence number and payload
            let seq = buffer.read_u64(base + SLOT_SEQ_OFFSET).unwrap_or(0);
            let req_len = match buffer.read_u32(base + SLOT_LEN_OFFSET) {
                Ok(l) => l as usize,
                Err(_) => {
                    let _ = buffer.write_byte(base + SLOT_STATE_OFFSET, SLOT_ERROR);
                    read_idx = (read_idx + 1) % RING_SLOTS as u32;
                    let _ = write_read_idx(&mut buffer, read_idx);
                    continue;
                }
            };

            if req_len > MAX_REQ_PAYLOAD {
                let _ = buffer.write_byte(base + SLOT_STATE_OFFSET, SLOT_ERROR);
                read_idx = (read_idx + 1) % RING_SLOTS as u32;
                let _ = write_read_idx(&mut buffer, read_idx);
                continue;
            }

            let req_data = match buffer.read_bytes(base + SLOT_PAYLOAD_OFFSET, req_len) {
                Ok(d) => d,
                Err(_) => {
                    let _ = buffer.write_byte(base + SLOT_STATE_OFFSET, SLOT_ERROR);
                    read_idx = (read_idx + 1) % RING_SLOTS as u32;
                    let _ = write_read_idx(&mut buffer, read_idx);
                    continue;
                }
            };

            // Parse and dispatch
            let response_frame = match NmcpFrame::from_bytes(&req_data) {
                Some(frame) => self.router.dispatch(&frame),
                None => NmcpFrame::error_response(seq as u32, 400, "invalid NMCP frame"),
            };
            let resp_bytes = response_frame.to_bytes();

            // Write response to same slot
            let resp_len = resp_bytes.len().min(MAX_RES_PAYLOAD);
            let _ = buffer.write_u32(base + SLOT_LEN_OFFSET, resp_len as u32);
            let _ = buffer.write_bytes(base + SLOT_PAYLOAD_OFFSET, &resp_bytes[..resp_len]);
            let _ = buffer.write_u64(base + SLOT_SEQ_OFFSET, seq);

            // Mark slot as RES_READY (client will read and reset to EMPTY)
            let _ = buffer.write_byte(base + SLOT_STATE_OFFSET, SLOT_RES_READY);

            // Wait for client to consume (slot goes back to EMPTY)
            let mut backoff = INITIAL_BACKOFF;
            loop {
                if !self.running.load(Ordering::Relaxed) {
                    break;
                }
                let s = buffer.read_byte(base + SLOT_STATE_OFFSET).unwrap_or(SLOT_ERROR);
                if s == SLOT_EMPTY {
                    break;
                }
                // Spin phase
                let mut spun = false;
                for _ in 0..MAX_SPIN_ITERATIONS {
                    let s = buffer.read_byte(base + SLOT_STATE_OFFSET).unwrap_or(SLOT_ERROR);
                    if s == SLOT_EMPTY {
                        spun = true;
                        break;
                    }
                    std::hint::spin_loop();
                }
                if spun {
                    break;
                }
                // Backoff phase
                std::thread::sleep(backoff);
                if backoff < MAX_BACKOFF {
                    backoff = backoff.mul_f32(2.0);
                }
            }

            // Advance read index
            read_idx = (read_idx + 1) % RING_SLOTS as u32;
            let _ = write_read_idx(&mut buffer, read_idx);
        }
    }
}

// ─── Shmem Client ────────────────────────────────────────────────────────────

/// NMCP Shared Memory Client — sends requests to the shmem server via ring buffer.
///
/// Each call writes to the next slot (write_idx), waits for the response
/// in the same slot, then advances write_idx.
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

        // Read current write_idx
        let write_idx = read_write_idx(&mut buffer)?;
        let base = slot_offset(write_idx as usize);

        // Wait for slot to be EMPTY (server has consumed previous use)
        let mut backoff = INITIAL_BACKOFF;
        for i in 0..100000u32 {
            let state = buffer.read_byte(base + SLOT_STATE_OFFSET)?;
            if state == SLOT_EMPTY || state == 0 {
                break;
            }
            if i < MAX_SPIN_ITERATIONS {
                std::hint::spin_loop();
            } else {
                std::thread::sleep(backoff);
                if backoff < MAX_BACKOFF {
                    backoff = backoff.mul_f32(2.0);
                }
            }
        }

        // Write request to slot
        buffer.write_u64(base + SLOT_SEQ_OFFSET, seq as u64)?;
        buffer.write_bytes(base + SLOT_PAYLOAD_OFFSET, &req_bytes)?;
        buffer.write_u32(base + SLOT_LEN_OFFSET, req_bytes.len() as u32)?;
        // Flush payload before marking REQ_READY (ordering guarantee)
        buffer.write_byte(base + SLOT_STATE_OFFSET, SLOT_REQ_READY)?;

        // Advance write_idx
        let next_idx = (write_idx + 1) % RING_SLOTS as u32;
        write_write_idx(&mut buffer, next_idx)?;

        // Wait for RES_READY in our slot
        let mut backoff = INITIAL_BACKOFF;
        for i in 0..100000u32 {
            let state = buffer.read_byte(base + SLOT_STATE_OFFSET)?;
            if state == SLOT_RES_READY {
                break;
            }
            if state == SLOT_ERROR {
                return Err("server error".to_string());
            }
            if i < MAX_SPIN_ITERATIONS {
                std::hint::spin_loop();
            } else {
                std::thread::sleep(backoff);
                if backoff < MAX_BACKOFF {
                    backoff = backoff.mul_f32(2.0);
                }
            }
        }

        // Read response
        let resp_len = buffer.read_u32(base + SLOT_LEN_OFFSET)? as usize;
        let resp_data = buffer.read_bytes(base + SLOT_PAYLOAD_OFFSET, resp_len)?;

        // Mark slot EMPTY (signal server we consumed the response)
        buffer.write_byte(base + SLOT_STATE_OFFSET, SLOT_EMPTY)?;

        NmcpFrame::from_bytes(&resp_data).ok_or_else(|| "invalid response frame".to_string())
    }
}
