//! VCTP RPC Layer — thin request/response dispatch over VCTP UDP transport.
//!
//! Replaces gRPC (tonic/HTTP2) with a zero-copy UDP-based RPC protocol.
//! Each RPC call is a VCTP packet with a JSON envelope payload, correlated
//! by the packet's `sequence_number` field.
//!
//! Architecture:
//!   [SDK clients] ──VCTP/UDP──► [VctpRpcServer] ──► [WorkflowEngine + WAL]
//!                                  (method dispatch)
//!
//! Wire format (inside VCTP packet payload):
//!   Request:  {"method": 100, "namespace": "default", "workflow_id": "wf-1", "payload": [...]}
//!   Response: {"status": 0, "sequence": 42, "payload": [...], "error": null}

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::engine::{WorkflowEngine, WorkflowStatus};
use crate::vctp_transport::VctpTransport;
use velocity_workflow_core::vctp::VctpPacket;

// ─── Method Constants ────────────────────────────────────────────────────────

/// VCTP RPC method identifiers.
///
/// Encoded in the `workflow_id` field of the VCTP packet header for fast
/// dispatch without parsing the JSON payload.
pub struct VctpMethods;

impl VctpMethods {
    // Workflow Lifecycle (100-199)
    pub const START_WORKFLOW: u64 = 100;
    pub const SIGNAL_WORKFLOW: u64 = 101;
    pub const QUERY_WORKFLOW: u64 = 102;
    pub const CANCEL_WORKFLOW: u64 = 103;
    pub const TERMINATE_WORKFLOW: u64 = 104;
    pub const DESCRIBE_WORKFLOW: u64 = 105;
    pub const LIST_WORKFLOWS: u64 = 106;
    pub const RESET_WORKFLOW: u64 = 107;
    pub const UPDATE_WORKFLOW: u64 = 108;
    pub const COMPLETE_WORKFLOW: u64 = 109;

    // Task Dispatch (200-299)
    pub const POLL_WORKFLOW_TASK: u64 = 200;
    pub const POLL_ACTIVITY_TASK: u64 = 201;
    pub const COMPLETE_WORKFLOW_TASK: u64 = 202;
    pub const COMPLETE_ACTIVITY_TASK: u64 = 203;

    // Namespace Management (300-399)
    pub const REGISTER_NAMESPACE: u64 = 300;
    pub const DESCRIBE_NAMESPACE: u64 = 301;
    pub const UPDATE_NAMESPACE: u64 = 302;
    pub const DELETE_NAMESPACE: u64 = 303;

    // History & Visibility (400-499)
    pub const GET_HISTORY: u64 = 400;
    pub const GET_WORKFLOW_EXECUTION: u64 = 401;

    // System (500-599)
    pub const HEALTH_CHECK: u64 = 500;
    pub const RECORD_HEARTBEAT: u64 = 501;
    pub const COUNT_WORKFLOWS: u64 = 502;
    pub const BATCH_SIGNAL: u64 = 503;
    pub const BATCH_TERMINATE: u64 = 504;

    // Advanced (600-699)
    pub const START_CHILD_WORKFLOW: u64 = 600;
    pub const CONTINUE_AS_NEW: u64 = 601;
    pub const SCHEDULE_TIMER: u64 = 602;
    pub const CANCEL_TIMER: u64 = 603;
    pub const SET_MEMO: u64 = 604;
    pub const UPSERT_SEARCH_ATTRIBUTES: u64 = 605;
    pub const SIGNAL_WITH_START: u64 = 606;
}

// ─── RPC Envelope Types ──────────────────────────────────────────────────────

/// RPC request envelope (serialized as JSON in VCTP payload).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VctpRpcRequest {
    /// Method identifier (maps to VctpMethods constants).
    pub method: u64,
    /// Namespace for the operation.
    #[serde(default = "default_namespace")]
    pub namespace: String,
    /// Workflow ID target.
    #[serde(default)]
    pub workflow_id: String,
    /// Optional binary payload (workflow input, signal data, etc.).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Vec<u8>>,
    /// Optional string fields for method-specific parameters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_name: Option<String>,
    /// Numeric parameters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_steps: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_count: Option<i64>,
    /// Metadata (search attributes, memos, etc.).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
}

fn default_namespace() -> String {
    "default".to_string()
}

/// RPC response envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VctpRpcResponse {
    /// Status code: 0 = OK, non-zero = error.
    pub status: u32,
    /// Correlates to request's VCTP sequence_number.
    pub sequence: u64,
    /// Optional binary response payload.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Vec<u8>>,
    /// Human-readable error message (when status != 0).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// String result fields for method-specific responses.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_status: Option<String>,
    /// Numeric result fields.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u64>,
}

impl VctpRpcResponse {
    /// Create a success response.
    pub fn ok(sequence: u64) -> Self {
        Self {
            status: 0,
            sequence,
            payload: None,
            error: None,
            workflow_id: None,
            run_id: None,
            run_status: None,
            count: None,
        }
    }

    /// Create an error response.
    pub fn err(sequence: u64, status: u32, error: impl Into<String>) -> Self {
        Self {
            status,
            sequence,
            payload: None,
            error: Some(error.into()),
            workflow_id: None,
            run_id: None,
            run_status: None,
            count: None,
        }
    }

    /// Attach a payload to this response.
    pub fn with_payload(mut self, payload: Vec<u8>) -> Self {
        self.payload = Some(payload);
        self
    }

    /// Attach workflow identifiers.
    pub fn with_workflow(mut self, workflow_id: String, run_id: String) -> Self {
        self.workflow_id = Some(workflow_id);
        self.run_id = Some(run_id);
        self
    }

    /// Attach a status string.
    pub fn with_status(mut self, status: impl Into<String>) -> Self {
        self.run_status = Some(status.into());
        self
    }

    /// Attach a count.
    pub fn with_count(mut self, count: u64) -> Self {
        self.count = Some(count);
        self
    }
}

// ─── Fragmentation ───────────────────────────────────────────────────────────

/// Maximum payload per VCTP packet (65507 - 28 header = 65479 bytes).
pub const MAX_VCTP_PAYLOAD: usize = 65479;

/// Fragment metadata packed into the `slab_offset` field.
///
/// Layout: [fragment_index (u16) | total_fragments (u16)]
pub fn encode_fragment_meta(index: u16, total: u16) -> u32 {
    (index as u32) << 16 | total as u32
}

/// Decode fragment metadata from the `slab_offset` field.
pub fn decode_fragment_meta(slab_offset: u32) -> (u16, u16) {
    let index = (slab_offset >> 16) as u16;
    let total = (slab_offset & 0xFFFF) as u16;
    (index, total)
}

/// Fragment a large payload into VCTP-sized chunks.
pub fn fragment_payload(payload: &[u8]) -> Vec<Vec<u8>> {
    if payload.len() <= MAX_VCTP_PAYLOAD {
        return vec![payload.to_vec()];
    }
    payload.chunks(MAX_VCTP_PAYLOAD).map(|c| c.to_vec()).collect()
}

/// Reassemble fragments into a complete payload.
pub fn reassemble_fragments(fragments: &mut HashMap<u16, Vec<u8>>, total: u16) -> Option<Vec<u8>> {
    if fragments.len() != total as usize {
        return None;
    }
    let mut result = Vec::new();
    for i in 0..total {
        match fragments.remove(&i) {
            Some(chunk) => result.extend_from_slice(&chunk),
            None => return None,
        }
    }
    Some(result)
}

// ─── Status Mapping ──────────────────────────────────────────────────────────

/// Map engine WorkflowStatus to string representation.
fn status_to_str(s: WorkflowStatus) -> &'static str {
    match s {
        WorkflowStatus::Running => "RUNNING",
        WorkflowStatus::Completed => "COMPLETED",
        WorkflowStatus::Failed => "FAILED",
        WorkflowStatus::Canceled => "CANCELLED",
        WorkflowStatus::Terminated => "TERMINATED",
        WorkflowStatus::ContinuedAsNew => "CONTINUING_AS_NEW",
        WorkflowStatus::TimedOut => "TIMED_OUT",
        WorkflowStatus::Void => "UNKNOWN",
    }
}

// ─── RPC Server ──────────────────────────────────────────────────────────────

/// Statistics for the VCTP RPC server.
#[derive(Debug, Clone, Default)]
pub struct VctpRpcStats {
    pub requests_received: u64,
    pub responses_sent: u64,
    pub errors: u64,
    pub unknown_methods: u64,
    pub fragmented_requests: u64,
    pub fragmented_responses: u64,
}

/// VCTP RPC Server — dispatches incoming VCTP packets to engine operations.
///
/// Replaces the gRPC `BenchmarkServiceImpl` with a UDP-based RPC layer.
/// The server receives VCTP packets, extracts the JSON RPC envelope from the
/// payload, dispatches to the appropriate engine method, and sends back a
/// VCTP response packet correlated by sequence number.
pub struct VctpRpcServer {
    transport: Arc<VctpTransport>,
    engine: Arc<WorkflowEngine>,
    /// Maps string workflow_id → engine workflow_key (lock-free concurrent map).
    workflow_map: Arc<DashMap<String, u64>>,
    /// Counter for generating numeric workflow IDs.
    workflow_counter: AtomicU64,
    /// Counter for generating namespace IDs.
    namespace_counter: AtomicU64,
    running: AtomicBool,
    stats: std::sync::RwLock<VctpRpcStats>,
    /// Reassembly buffer for fragmented incoming payloads: src_addr → (fragments, total).
    frag_buf: std::sync::RwLock<HashMap<std::net::SocketAddr, (HashMap<u16, Vec<u8>>, u16)>>,
}

impl VctpRpcServer {
    /// Create a new VCTP RPC server.
    pub fn new(
        transport: Arc<VctpTransport>,
        engine: Arc<WorkflowEngine>,
    ) -> Self {
        Self {
            transport,
            engine,
            workflow_map: Arc::new(DashMap::new()),
            workflow_counter: AtomicU64::new(1),
            namespace_counter: AtomicU64::new(1),
            running: AtomicBool::new(true),
            stats: std::sync::RwLock::new(VctpRpcStats::default()),
            frag_buf: std::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Get current server statistics.
    pub fn stats(&self) -> VctpRpcStats {
        self.stats.read().unwrap().clone()
    }

    /// Shut down the server.
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::Relaxed);
        self.transport.shutdown();
    }

    /// Main receive-dispatch loop. Call this from an async context.
    ///
    /// Receives VCTP packets, dispatches to engine methods, sends responses.
    /// Returns when `shutdown()` is called or the transport stops.
    pub fn run(&self) {
        // VCTP RPC server started

        while self.running.load(Ordering::Relaxed) {
            let packets = self.transport.recv_packets();

            for (packet, src_addr) in packets {
                self.stats.write().unwrap().requests_received += 1;

                // Check for fragmented packet
                let (frag_index, frag_total) = decode_fragment_meta(packet.header.slab_offset);
                if frag_total > 1 {
                    self.handle_fragment(src_addr, frag_index, frag_total, &packet);
                    continue;
                }

                // Parse the JSON RPC request from the payload
                let request: VctpRpcRequest = match serde_json::from_slice(&packet.payload) {
                    Ok(r) => r,
                    Err(e) => {
                        self.stats.write().unwrap().errors += 1;
                        let resp = VctpRpcResponse::err(
                            packet.header.sequence_number,
                            400,
                            format!("invalid request: {}", e),
                        );
                        self.send_response(src_addr, &resp);
                        continue;
                    }
                };

                // Dispatch to the appropriate handler
                let response = self.dispatch(packet.header.sequence_number, request);

                // Send response (fragmenting if necessary)
                self.send_response(src_addr, &response);
            }

            // Process retransmissions
            self.transport.process_retransmissions();

            // Yield to avoid busy-spinning
            std::thread::yield_now();
        }

        // VCTP RPC server stopped
    }

    /// Handle a fragmented packet.
    fn handle_fragment(
        &self,
        src_addr: std::net::SocketAddr,
        index: u16,
        total: u16,
        packet: &VctpPacket,
    ) {
        self.stats.write().unwrap().fragmented_requests += 1;

        let mut frag_buf = self.frag_buf.write().unwrap();
        let entry = frag_buf
            .entry(src_addr)
            .or_insert_with(|| (HashMap::new(), total));

        entry.0.insert(index, packet.payload.clone());

        // Check if we have all fragments
        if let Some(complete_payload) = reassemble_fragments(&mut entry.0, entry.1) {
            let sequence = packet.header.sequence_number;
            drop(frag_buf);

            let request: VctpRpcRequest = match serde_json::from_slice(&complete_payload) {
                Ok(r) => r,
                Err(e) => {
                    self.stats.write().unwrap().errors += 1;
                    let resp = VctpRpcResponse::err(sequence, 400, format!("invalid request: {}", e));
                    self.send_response(src_addr, &resp);
                    return;
                }
            };

            let response = self.dispatch(sequence, request);
            self.send_response(src_addr, &response);
        }
    }

    /// Send a response, fragmenting if the payload exceeds VCTP max.
    fn send_response(&self, addr: std::net::SocketAddr, response: &VctpRpcResponse) {
        let response_bytes = serde_json::to_vec(response).unwrap_or_default();

        if response_bytes.len() <= MAX_VCTP_PAYLOAD {
            // Single packet response
            let _ = self.transport.send_packet(addr, 0, 0, response_bytes);
            self.stats.write().unwrap().responses_sent += 1;
        } else {
            // Fragmented response
            let fragments = fragment_payload(&response_bytes);
            let total = fragments.len() as u16;
            for (i, fragment) in fragments.iter().enumerate() {
                let slab_offset = encode_fragment_meta(i as u16, total);
                let _ = self.transport.send_packet(addr, 0, slab_offset, fragment.clone());
            }
            self.stats.write().unwrap().responses_sent += 1;
            self.stats.write().unwrap().fragmented_responses += 1;
        }
    }

    /// Dispatch an RPC request to the appropriate engine method.
    fn dispatch(&self, sequence: u64, req: VctpRpcRequest) -> VctpRpcResponse {
        match req.method {
            VctpMethods::START_WORKFLOW => self.handle_start_workflow(sequence, req),
            VctpMethods::SIGNAL_WORKFLOW => self.handle_signal_workflow(sequence, req),
            VctpMethods::QUERY_WORKFLOW => self.handle_query_workflow(sequence, req),
            VctpMethods::CANCEL_WORKFLOW => self.handle_cancel_workflow(sequence, req),
            VctpMethods::TERMINATE_WORKFLOW => self.handle_terminate_workflow(sequence, req),
            VctpMethods::DESCRIBE_WORKFLOW => self.handle_describe_workflow(sequence, req),
            VctpMethods::COMPLETE_WORKFLOW => self.handle_complete_workflow(sequence, req),
            VctpMethods::UPDATE_WORKFLOW => self.handle_update_workflow(sequence, req),
            VctpMethods::RESET_WORKFLOW => self.handle_reset_workflow(sequence, req),
            VctpMethods::HEALTH_CHECK => self.handle_health_check(sequence),
            VctpMethods::COUNT_WORKFLOWS => self.handle_count_workflows(sequence, req),
            VctpMethods::BATCH_SIGNAL => self.handle_batch_signal(sequence, req),
            VctpMethods::SIGNAL_WITH_START => self.handle_signal_with_start(sequence, req),
            VctpMethods::REGISTER_NAMESPACE => self.handle_register_namespace(sequence, req),
            VctpMethods::DESCRIBE_NAMESPACE => self.handle_describe_namespace(sequence, req),
            _ => {
                self.stats.write().unwrap().unknown_methods += 1;
                VctpRpcResponse::err(sequence, 404, format!("unknown method: {}", req.method))
            }
        }
    }

    // ─── Method Handlers ─────────────────────────────────────────────────────

    fn handle_start_workflow(&self, seq: u64, req: VctpRpcRequest) -> VctpRpcResponse {
        let wf_id = if req.workflow_id.is_empty() {
            format!("wf-{}", self.workflow_counter.fetch_add(1, Ordering::Relaxed))
        } else {
            req.workflow_id.clone()
        };
        let wf_type = req.workflow_type.as_deref().unwrap_or("Unknown");
        let total_steps = req.total_steps.unwrap_or(10);

        let wf_id_num = self.workflow_counter.fetch_add(1, Ordering::Relaxed);
        let wf_type_id = wf_type.len() as u64;
        let namespace_id = self.namespace_counter.fetch_add(1, Ordering::Relaxed);
        let task_queue_hash = req.namespace.len() as u64;

        let workflow_key = self.engine.start_workflow(
            wf_id_num,
            wf_type_id,
            namespace_id,
            task_queue_hash,
            total_steps,
            None,
        );

        // Store mapping
        self.workflow_map.insert(wf_id.clone(), workflow_key);

        // Sequential per-step durable execution: each step is WAL-fsynced + PG-persisted
        // before the next step begins.  Crash at any point → resume from last persisted step.
        let total = self.engine.get_total_steps(workflow_key);
        for step in 0..total {
            let _ = self.engine.persist_step(workflow_key, step, "default");
        }
        self.engine.complete_workflow(workflow_key, Some(vec![]));

        // Final persist with completed status.
        let _ = self.engine.persist_workflow_by_key(workflow_key, "default");

        let run_id = format!("run-{}", workflow_key);
        VctpRpcResponse::ok(seq)
            .with_workflow(wf_id, run_id)
            .with_status("COMPLETED")
    }

    fn handle_signal_workflow(&self, seq: u64, req: VctpRpcRequest) -> VctpRpcResponse {
        let key = match self.workflow_map.get(&req.workflow_id) {
            Some(k) => *k,
            None => return VctpRpcResponse::err(seq, 404, "workflow not found"),
        };
        let signal_name = req.signal_name.as_deref().unwrap_or("unknown");
        let signal_id = signal_name.len() as u64;
        let payload = req.payload.unwrap_or_default();
        self.engine.signal_workflow(key, signal_id, payload);
        VctpRpcResponse::ok(seq)
    }

    fn handle_query_workflow(&self, seq: u64, req: VctpRpcRequest) -> VctpRpcResponse {
        let key = match self.workflow_map.get(&req.workflow_id) {
            Some(k) => *k,
            None => return VctpRpcResponse::err(seq, 404, "workflow not found"),
        };
        let status = self.engine.get_status(key);
        VctpRpcResponse::ok(seq).with_status(status_to_str(status))
    }

    fn handle_cancel_workflow(&self, seq: u64, req: VctpRpcRequest) -> VctpRpcResponse {
        let key = match self.workflow_map.get(&req.workflow_id) {
            Some(k) => *k,
            None => return VctpRpcResponse::err(seq, 404, "workflow not found"),
        };
        self.engine.cancel_workflow(key);
        VctpRpcResponse::ok(seq)
    }

    fn handle_terminate_workflow(&self, seq: u64, req: VctpRpcRequest) -> VctpRpcResponse {
        let key = match self.workflow_map.get(&req.workflow_id) {
            Some(k) => *k,
            None => return VctpRpcResponse::err(seq, 404, "workflow not found"),
        };
        self.engine.terminate_workflow(key);
        VctpRpcResponse::ok(seq)
    }

    fn handle_describe_workflow(&self, seq: u64, req: VctpRpcRequest) -> VctpRpcResponse {
        let key = match self.workflow_map.get(&req.workflow_id) {
            Some(k) => *k,
            None => return VctpRpcResponse::err(seq, 404, "workflow not found"),
        };
        let status = self.engine.get_status(key);
        VctpRpcResponse::ok(seq)
            .with_workflow(req.workflow_id, format!("run-{}", key))
            .with_status(status_to_str(status))
    }

    fn handle_complete_workflow(&self, seq: u64, req: VctpRpcRequest) -> VctpRpcResponse {
        let key = match self.workflow_map.get(&req.workflow_id) {
            Some(k) => *k,
            None => return VctpRpcResponse::err(seq, 404, "workflow not found"),
        };
        self.engine.complete_workflow(key, req.payload);
        VctpRpcResponse::ok(seq)
    }

    fn handle_update_workflow(&self, seq: u64, _req: VctpRpcRequest) -> VctpRpcResponse {
        // Stub: update workflow execution
        VctpRpcResponse::ok(seq)
    }

    fn handle_reset_workflow(&self, seq: u64, req: VctpRpcRequest) -> VctpRpcResponse {
        let reset_id = self.workflow_counter.fetch_add(1, Ordering::Relaxed);
        VctpRpcResponse::ok(seq)
            .with_workflow(req.workflow_id, format!("reset-{}", reset_id))
    }

    fn handle_health_check(&self, seq: u64) -> VctpRpcResponse {
        VctpRpcResponse::ok(seq).with_status("healthy")
    }

    fn handle_count_workflows(&self, seq: u64, _req: VctpRpcRequest) -> VctpRpcResponse {
        VctpRpcResponse::ok(seq).with_count(self.workflow_map.len() as u64)
    }

    fn handle_batch_signal(&self, seq: u64, req: VctpRpcRequest) -> VctpRpcResponse {
        let key = match self.workflow_map.get(&req.workflow_id) {
            Some(k) => *k,
            None => return VctpRpcResponse::err(seq, 404, "workflow not found"),
        };
        let signal_name = req.signal_name.as_deref().unwrap_or("unknown");
        let signal_id = signal_name.len() as u64;
        let count = req.signal_count.unwrap_or(1);
        let template = req.payload.unwrap_or_default();

        let mut processed = 0u32;
        for i in 0..count {
            let mut payload = template.clone();
            payload.extend_from_slice(&i.to_le_bytes());
            self.engine.signal_workflow(key, signal_id, payload);
            processed += 1;
        }
        VctpRpcResponse::ok(seq).with_count(processed as u64)
    }

    fn handle_signal_with_start(&self, seq: u64, req: VctpRpcRequest) -> VctpRpcResponse {
        // Start the workflow first
        let start_resp = self.handle_start_workflow(seq, req.clone());
        if start_resp.status != 0 {
            return start_resp;
        }
        // Then signal it
        let signal_resp = self.handle_signal_workflow(seq, req);
        signal_resp
    }

    fn handle_register_namespace(&self, seq: u64, _req: VctpRpcRequest) -> VctpRpcResponse {
        let _ns_id = self.namespace_counter.fetch_add(1, Ordering::Relaxed);
        VctpRpcResponse::ok(seq)
            .with_status("REGISTERED")
    }

    fn handle_describe_namespace(&self, seq: u64, req: VctpRpcRequest) -> VctpRpcResponse {
        VctpRpcResponse::ok(seq)
            .with_workflow(req.namespace, String::new())
            .with_status("REGISTERED")
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_method_constants() {
        assert_eq!(VctpMethods::START_WORKFLOW, 100);
        assert_eq!(VctpMethods::HEALTH_CHECK, 500);
    }

    #[test]
    fn test_request_serialization() {
        let req = VctpRpcRequest {
            method: VctpMethods::START_WORKFLOW,
            namespace: "default".to_string(),
            workflow_id: "wf-1".to_string(),
            payload: Some(vec![1, 2, 3]),
            workflow_type: Some("test-wf".to_string()),
            signal_name: None,
            query_type: None,
            update_name: None,
            total_steps: Some(5),
            signal_count: None,
            max_count: None,
            metadata: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: VctpRpcRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.method, 100);
        assert_eq!(decoded.workflow_id, "wf-1");
        assert_eq!(decoded.total_steps, Some(5));
    }

    #[test]
    fn test_response_serialization() {
        let resp = VctpRpcResponse::ok(42)
            .with_workflow("wf-1".to_string(), "run-1".to_string())
            .with_status("COMPLETED");
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: VctpRpcResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.status, 0);
        assert_eq!(decoded.sequence, 42);
        assert_eq!(decoded.workflow_id.as_deref(), Some("wf-1"));
        assert_eq!(decoded.run_status.as_deref(), Some("COMPLETED"));
    }

    #[test]
    fn test_error_response() {
        let resp = VctpRpcResponse::err(7, 404, "not found");
        assert_eq!(resp.status, 404);
        assert_eq!(resp.error.as_deref(), Some("not found"));
    }

    #[test]
    fn test_fragment_encoding() {
        let encoded = encode_fragment_meta(3, 10);
        let (index, total) = decode_fragment_meta(encoded);
        assert_eq!(index, 3);
        assert_eq!(total, 10);
    }

    #[test]
    fn test_fragment_payload_small() {
        let payload = vec![1u8; 100];
        let fragments = fragment_payload(&payload);
        assert_eq!(fragments.len(), 1);
        assert_eq!(fragments[0], payload);
    }

    #[test]
    fn test_fragment_payload_large() {
        let payload = vec![1u8; MAX_VCTP_PAYLOAD * 3 + 100];
        let fragments = fragment_payload(&payload);
        assert_eq!(fragments.len(), 4);
        assert_eq!(fragments[0].len(), MAX_VCTP_PAYLOAD);
        assert_eq!(fragments[3].len(), 100);
    }

    #[test]
    fn test_reassemble_fragments() {
        let mut frags = HashMap::new();
        frags.insert(0, vec![1, 2, 3]);
        frags.insert(1, vec![4, 5, 6]);
        frags.insert(2, vec![7, 8, 9]);
        let result = reassemble_fragments(&mut frags, 3).unwrap();
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_reassemble_incomplete() {
        let mut frags = HashMap::new();
        frags.insert(0, vec![1, 2, 3]);
        assert!(reassemble_fragments(&mut frags, 3).is_none());
    }

    #[test]
    fn test_status_mapping() {
        assert_eq!(status_to_str(WorkflowStatus::Running), "RUNNING");
        assert_eq!(status_to_str(WorkflowStatus::Completed), "COMPLETED");
        assert_eq!(status_to_str(WorkflowStatus::Failed), "FAILED");
        assert_eq!(status_to_str(WorkflowStatus::Canceled), "CANCELLED");
        assert_eq!(status_to_str(WorkflowStatus::Terminated), "TERMINATED");
    }
}
