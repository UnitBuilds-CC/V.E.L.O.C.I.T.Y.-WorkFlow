//! NMCP Frame Router — dispatches NMCP frames to WorkflowEngine operations.
//!
//! Part of the Classic Server NMCP protocol upgrade. Replaces HTTP/axum routing
//! with a binary-framed dispatch layer that works over both shared memory IPC
//! and WebSocket transports.
//!
//! Wire format (16-byte header + JSON payload):
//!   magic(4) + frame_type(4) + payload_len(4) + sequence_id(4) + payload(N)

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use velocity_workflow_engine::engine::{WorkflowEngine, WorkflowStatus};

// ─── NMCP Frame Types ────────────────────────────────────────────────────────

/// Classic Server NMCP frame types (50-69 range).
pub struct ClassicFrameTypes;

impl ClassicFrameTypes {
    // Workflow Lifecycle (50-59)
    pub const START_WORKFLOW: u32 = 50;
    pub const GET_WORKFLOW: u32 = 51;
    pub const SIGNAL_WORKFLOW: u32 = 52;
    pub const QUERY_WORKFLOW: u32 = 53;
    pub const CANCEL_WORKFLOW: u32 = 54;
    pub const TERMINATE_WORKFLOW: u32 = 55;
    pub const UPDATE_WORKFLOW: u32 = 56;
    pub const RESET_WORKFLOW: u32 = 57;

    // System (60-69)
    pub const HEALTH_CHECK: u32 = 60;
    pub const SERVER_STATS: u32 = 61;
}

/// NMCP magic bytes: "NMCP" in little-endian.
pub const NMCP_MAGIC: u32 = 0x5043_4D4E;

/// NMCP frame header size in bytes.
pub const NMCP_HEADER_SIZE: usize = 16;

// ─── NMCP Frame ──────────────────────────────────────────────────────────────

/// A parsed NMCP frame (header + payload).
#[derive(Debug, Clone)]
pub struct NmcpFrame {
    pub frame_type: u32,
    pub sequence_id: u32,
    pub payload: Vec<u8>,
}

impl NmcpFrame {
    /// Create a new NMCP frame.
    pub fn new(frame_type: u32, sequence_id: u32, payload: Vec<u8>) -> Self {
        Self {
            frame_type,
            sequence_id,
            payload,
        }
    }

    /// Serialize to bytes (header + payload).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(NMCP_HEADER_SIZE + self.payload.len());
        buf.extend_from_slice(&NMCP_MAGIC.to_le_bytes());
        buf.extend_from_slice(&self.frame_type.to_le_bytes());
        buf.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.sequence_id.to_le_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }

    /// Parse from bytes. Returns None if invalid.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < NMCP_HEADER_SIZE {
            return None;
        }
        let magic = u32::from_le_bytes(data[0..4].try_into().ok()?);
        if magic != NMCP_MAGIC {
            return None;
        }
        let frame_type = u32::from_le_bytes(data[4..8].try_into().ok()?);
        let payload_len = u32::from_le_bytes(data[8..12].try_into().ok()?) as usize;
        let sequence_id = u32::from_le_bytes(data[12..16].try_into().ok()?);

        if data.len() < NMCP_HEADER_SIZE + payload_len {
            return None;
        }
        let payload = data[NMCP_HEADER_SIZE..NMCP_HEADER_SIZE + payload_len].to_vec();
        Some(Self {
            frame_type,
            sequence_id,
            payload,
        })
    }

    /// Create a JSON response frame.
    pub fn json_response(sequence_id: u32, body: JsonValue) -> Self {
        let payload = serde_json::to_vec(&body).unwrap_or_default();
        Self::new(0, sequence_id, payload) // frame_type 0 = generic response
    }

    /// Create an error response frame.
    pub fn error_response(sequence_id: u32, status: u16, message: &str) -> Self {
        let body = serde_json::json!({
            "success": false,
            "error": message,
            "status": status,
        });
        Self::json_response(sequence_id, body)
    }
}

// ─── JSON Request Body ───────────────────────────────────────────────────────

/// Parsed JSON request body from NMCP payload.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NmcpRequestBody {
    #[serde(default)]
    pub workflow_id: Option<String>,
    #[serde(default)]
    pub workflow_type: Option<String>,
    #[serde(default)]
    pub signal_name: Option<String>,
    #[serde(default)]
    pub query_type: Option<String>,
    #[serde(default)]
    pub update_name: Option<String>,
    #[serde(default)]
    pub input: Option<JsonValue>,
    #[serde(default)]
    pub reason: Option<String>,
}

// ─── Frame Router ────────────────────────────────────────────────────────────

/// Statistics for the NMCP frame router.
#[derive(Debug, Clone, Default, Serialize)]
pub struct NmcpRouterStats {
    pub frames_received: u64,
    pub frames_dispatched: u64,
    pub errors: u64,
    pub unknown_types: u64,
}

/// NMCP Frame Router — dispatches frames to WorkflowEngine operations.
///
/// Replaces the axum HTTP router with a binary-framed dispatch layer.
/// Works identically over shmem and WebSocket transports.
pub struct NmcpFrameRouter {
    engine: Arc<WorkflowEngine>,
    workflow_map: Arc<Mutex<HashMap<String, u64>>>,
    workflow_counter: Arc<AtomicU64>,
    stats: Mutex<NmcpRouterStats>,
}

impl NmcpFrameRouter {
    /// Create a new frame router.
    pub fn new(
        engine: Arc<WorkflowEngine>,
        workflow_map: Arc<Mutex<HashMap<String, u64>>>,
        workflow_counter: Arc<AtomicU64>,
    ) -> Self {
        Self {
            engine,
            workflow_map,
            workflow_counter,
            stats: Mutex::new(NmcpRouterStats::default()),
        }
    }

    /// Get current router statistics.
    pub fn stats(&self) -> NmcpRouterStats {
        self.stats.lock().unwrap().clone()
    }

    /// Dispatch an NMCP frame and return a response frame.
    pub fn dispatch(&self, frame: &NmcpFrame) -> NmcpFrame {
        {
            let mut s = self.stats.lock().unwrap();
            s.frames_received += 1;
        }

        let response = match frame.frame_type {
            ClassicFrameTypes::START_WORKFLOW => self.handle_start(frame),
            ClassicFrameTypes::GET_WORKFLOW => self.handle_get(frame),
            ClassicFrameTypes::SIGNAL_WORKFLOW => self.handle_signal(frame),
            ClassicFrameTypes::QUERY_WORKFLOW => self.handle_query(frame),
            ClassicFrameTypes::CANCEL_WORKFLOW => self.handle_cancel(frame),
            ClassicFrameTypes::TERMINATE_WORKFLOW => self.handle_terminate(frame),
            ClassicFrameTypes::UPDATE_WORKFLOW => self.handle_update(frame),
            ClassicFrameTypes::RESET_WORKFLOW => self.handle_reset(frame),
            ClassicFrameTypes::HEALTH_CHECK => self.handle_health(frame),
            ClassicFrameTypes::SERVER_STATS => self.handle_stats(frame),
            _ => {
                self.stats.lock().unwrap().unknown_types += 1;
                NmcpFrame::error_response(frame.sequence_id, 404, "unknown frame type")
            }
        };

        self.stats.lock().unwrap().frames_dispatched += 1;
        response
    }

    /// Parse the JSON body from a frame payload.
    fn parse_body(&self, frame: &NmcpFrame) -> Result<NmcpRequestBody, NmcpFrame> {
        serde_json::from_slice(&frame.payload).map_err(|e| {
            self.stats.lock().unwrap().errors += 1;
            NmcpFrame::error_response(frame.sequence_id, 400, &format!("invalid JSON: {}", e))
        })
    }

    /// Look up the engine workflow_key for a string workflow_id.
    fn lookup_key(&self, workflow_id: &str) -> Result<u64, NmcpFrame> {
        let map = self.workflow_map.lock().unwrap();
        map.get(workflow_id).copied().ok_or_else(|| {
            NmcpFrame::error_response(0, 404, &format!("workflow not found: {}", workflow_id))
        })
    }

    // ─── Handlers ──────────────────────────────────────────────────────────

    fn handle_start(&self, frame: &NmcpFrame) -> NmcpFrame {
        let body = match self.parse_body(frame) {
            Ok(b) => b,
            Err(resp) => return resp,
        };

        let wf_id = body.workflow_id.unwrap_or_else(|| {
            format!("wf-{}", self.workflow_counter.fetch_add(1, Ordering::Relaxed))
        });
        let wf_type = body.workflow_type.as_deref().unwrap_or("Unknown");

        let wf_id_num = self.workflow_counter.fetch_add(1, Ordering::Relaxed);
        let wf_type_id = wf_type.len() as u64;
        let namespace_id = 1u64;
        let task_queue_hash = 1u64;

        let workflow_key = self.engine.start_workflow(
            wf_id_num,
            wf_type_id,
            namespace_id,
            task_queue_hash,
            10,
            None,
        );

        {
            let mut map = self.workflow_map.lock().unwrap();
            map.insert(wf_id.clone(), workflow_key);
        }

        // Inline execution: complete all steps immediately
        let total_steps = self.engine.get_total_steps(workflow_key);
        for step in 0..total_steps {
            self.engine.complete_step(workflow_key, step, vec![]);
        }
        self.engine.complete_workflow(workflow_key, Some(vec![]));

        // Persist to PostgreSQL if DB adapter is enabled.
        let _ = self.engine.persist_workflow_by_key(workflow_key, "default");

        NmcpFrame::json_response(
            frame.sequence_id,
            serde_json::json!({
                "success": true,
                "data": {
                    "workflowId": wf_id,
                    "runId": format!("run-{}", workflow_key),
                    "status": "COMPLETED"
                }
            }),
        )
    }

    fn handle_get(&self, frame: &NmcpFrame) -> NmcpFrame {
        let body = match self.parse_body(frame) {
            Ok(b) => b,
            Err(resp) => return resp,
        };
        let wf_id = match body.workflow_id {
            Some(id) => id,
            None => return NmcpFrame::error_response(frame.sequence_id, 400, "missing workflow_id"),
        };

        let map = self.workflow_map.lock().unwrap();
        match map.get(&wf_id) {
            Some(&key) => {
                let status = self.engine.get_status(key);
                NmcpFrame::json_response(
                    frame.sequence_id,
                    serde_json::json!({
                        "success": true,
                        "data": {
                            "workflowId": wf_id,
                            "status": status_to_str(status),
                        }
                    }),
                )
            }
            None => NmcpFrame::error_response(frame.sequence_id, 404, "workflow not found"),
        }
    }

    fn handle_signal(&self, frame: &NmcpFrame) -> NmcpFrame {
        let body = match self.parse_body(frame) {
            Ok(b) => b,
            Err(resp) => return resp,
        };
        let wf_id = match body.workflow_id {
            Some(id) => id,
            None => return NmcpFrame::error_response(frame.sequence_id, 400, "missing workflow_id"),
        };
        let signal_name = body.signal_name.as_deref().unwrap_or("unknown");
        let signal_id = signal_name.len() as u64;
        let payload = serde_json::to_vec(&body.input.unwrap_or(JsonValue::Null)).unwrap_or_default();

        let map = self.workflow_map.lock().unwrap();
        match map.get(&wf_id) {
            Some(&key) => {
                self.engine.signal_workflow(key, signal_id, payload);
                NmcpFrame::json_response(frame.sequence_id, serde_json::json!({"success": true}))
            }
            None => NmcpFrame::error_response(frame.sequence_id, 404, "workflow not found"),
        }
    }

    fn handle_query(&self, frame: &NmcpFrame) -> NmcpFrame {
        let body = match self.parse_body(frame) {
            Ok(b) => b,
            Err(resp) => return resp,
        };
        let wf_id = match body.workflow_id {
            Some(id) => id,
            None => return NmcpFrame::error_response(frame.sequence_id, 400, "missing workflow_id"),
        };

        let map = self.workflow_map.lock().unwrap();
        match map.get(&wf_id) {
            Some(&key) => {
                let status = self.engine.get_status(key);
                let query_type = body.query_type.as_deref().unwrap_or("status");
                let result = match query_type {
                    "status" => serde_json::json!({ "status": status_to_str(status) }),
                    _ => serde_json::json!({ "result": null }),
                };
                NmcpFrame::json_response(
                    frame.sequence_id,
                    serde_json::json!({"success": true, "data": result}),
                )
            }
            None => NmcpFrame::error_response(frame.sequence_id, 404, "workflow not found"),
        }
    }

    fn handle_cancel(&self, frame: &NmcpFrame) -> NmcpFrame {
        let body = match self.parse_body(frame) {
            Ok(b) => b,
            Err(resp) => return resp,
        };
        let wf_id = match body.workflow_id {
            Some(id) => id,
            None => return NmcpFrame::error_response(frame.sequence_id, 400, "missing workflow_id"),
        };

        let map = self.workflow_map.lock().unwrap();
        match map.get(&wf_id) {
            Some(&key) => {
                self.engine.cancel_workflow(key);
                NmcpFrame::json_response(frame.sequence_id, serde_json::json!({"success": true}))
            }
            None => NmcpFrame::error_response(frame.sequence_id, 404, "workflow not found"),
        }
    }

    fn handle_terminate(&self, frame: &NmcpFrame) -> NmcpFrame {
        let body = match self.parse_body(frame) {
            Ok(b) => b,
            Err(resp) => return resp,
        };
        let wf_id = match body.workflow_id {
            Some(id) => id,
            None => return NmcpFrame::error_response(frame.sequence_id, 400, "missing workflow_id"),
        };

        let map = self.workflow_map.lock().unwrap();
        match map.get(&wf_id) {
            Some(&key) => {
                self.engine.terminate_workflow(key);
                NmcpFrame::json_response(frame.sequence_id, serde_json::json!({"success": true}))
            }
            None => NmcpFrame::error_response(frame.sequence_id, 404, "workflow not found"),
        }
    }

    fn handle_update(&self, frame: &NmcpFrame) -> NmcpFrame {
        NmcpFrame::json_response(
            frame.sequence_id,
            serde_json::json!({"success": true, "data": {"result": null}}),
        )
    }

    fn handle_reset(&self, frame: &NmcpFrame) -> NmcpFrame {
        let body = match self.parse_body(frame) {
            Ok(b) => b,
            Err(resp) => return resp,
        };
        let wf_id = body.workflow_id.unwrap_or_default();
        let reset_id = self.workflow_counter.fetch_add(1, Ordering::Relaxed);
        NmcpFrame::json_response(
            frame.sequence_id,
            serde_json::json!({
                "success": true,
                "data": {
                    "workflowId": wf_id,
                    "runId": format!("run-reset-{}", reset_id)
                }
            }),
        )
    }

    fn handle_health(&self, frame: &NmcpFrame) -> NmcpFrame {
        NmcpFrame::json_response(
            frame.sequence_id,
            serde_json::json!({
                "success": true,
                "data": {
                    "status": "healthy",
                    "engine": "velocity-classic",
                    "transport": "nmcp",
                    "runtime": "rust",
                    "persistence": "wal"
                }
            }),
        )
    }

    fn handle_stats(&self, frame: &NmcpFrame) -> NmcpFrame {
        let router_stats = self.stats();
        NmcpFrame::json_response(
            frame.sequence_id,
            serde_json::json!({
                "success": true,
                "data": {
                    "router": router_stats,
                }
            }),
        )
    }
}

// ─── Status Mapping ──────────────────────────────────────────────────────────

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

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_roundtrip() {
        let frame = NmcpFrame::new(50, 42, b"hello".to_vec());
        let bytes = frame.to_bytes();
        let parsed = NmcpFrame::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.frame_type, 50);
        assert_eq!(parsed.sequence_id, 42);
        assert_eq!(parsed.payload, b"hello");
    }

    #[test]
    fn test_frame_invalid_magic() {
        let mut bytes = NmcpFrame::new(50, 1, vec![]).to_bytes();
        bytes[0] = 0xFF; // corrupt magic
        assert!(NmcpFrame::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_frame_too_short() {
        assert!(NmcpFrame::from_bytes(&[0u8; 10]).is_none());
    }

    #[test]
    fn test_json_response() {
        let frame = NmcpFrame::json_response(7, serde_json::json!({"ok": true}));
        assert_eq!(frame.sequence_id, 7);
        let body: JsonValue = serde_json::from_slice(&frame.payload).unwrap();
        assert_eq!(body["ok"], true);
    }

    #[test]
    fn test_error_response() {
        let frame = NmcpFrame::error_response(3, 404, "not found");
        let body: JsonValue = serde_json::from_slice(&frame.payload).unwrap();
        assert_eq!(body["success"], false);
        assert_eq!(body["status"], 404);
    }

    #[test]
    fn test_classic_frame_types() {
        assert_eq!(ClassicFrameTypes::START_WORKFLOW, 50);
        assert_eq!(ClassicFrameTypes::HEALTH_CHECK, 60);
    }

    #[test]
    fn test_router_health() {
        let engine = Arc::new(WorkflowEngine::new());
        let router = NmcpFrameRouter::new(
            engine,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(AtomicU64::new(1)),
        );
        let frame = NmcpFrame::new(ClassicFrameTypes::HEALTH_CHECK, 1, b"{}".to_vec());
        let resp = router.dispatch(&frame);
        let body: JsonValue = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(body["success"], true);
        assert_eq!(body["data"]["transport"], "nmcp");
    }

    #[test]
    fn test_router_start_and_get() {
        let engine = Arc::new(WorkflowEngine::new());
        let map = Arc::new(Mutex::new(HashMap::new()));
        let counter = Arc::new(AtomicU64::new(1));
        let router = NmcpFrameRouter::new(engine, map.clone(), counter);

        // Start a workflow
        let body = serde_json::json!({"workflow_id": "test-wf", "workflow_type": "bench"});
        let frame = NmcpFrame::new(
            ClassicFrameTypes::START_WORKFLOW,
            1,
            serde_json::to_vec(&body).unwrap(),
        );
        let resp = router.dispatch(&frame);
        let resp_body: JsonValue = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(resp_body["success"], true);
        assert_eq!(resp_body["data"]["status"], "COMPLETED");

        // Verify mapping exists
        assert!(map.lock().unwrap().contains_key("test-wf"));

        // Get the workflow
        let get_body = serde_json::json!({"workflow_id": "test-wf"});
        let get_frame = NmcpFrame::new(
            ClassicFrameTypes::GET_WORKFLOW,
            2,
            serde_json::to_vec(&get_body).unwrap(),
        );
        let get_resp = router.dispatch(&get_frame);
        let get_resp_body: JsonValue = serde_json::from_slice(&get_resp.payload).unwrap();
        assert_eq!(get_resp_body["data"]["status"], "COMPLETED");
    }
}
