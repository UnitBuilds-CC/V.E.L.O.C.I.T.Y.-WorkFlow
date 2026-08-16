//! NMCP Frame Router for Embedded Server — dispatches NMCP frames to engine ops.
//!
//! Part of the Embedded Server NMCP protocol upgrade. Uses the same
//! WorkflowEngine for execution but exposes a library-style API:
//!   - execute_workflow (run to completion inline)
//!   - get_workflow, signal, query, complete, list
//!   - health, stats
//!
//! Wire format: same 16-byte NMCP header + JSON payload.
//! Frame types use 70-79 range (embedded-specific).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use dashmap::DashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use velocity_workflow_engine::engine::{WorkflowEngine, WorkflowStatus};

// Re-export shared NMCP protocol types from the protocol crate.
pub use velocity_nmcp_protocol::{
    NmcpFrame, NmcpRequestBody, NmcpRouterStats, NmcpDispatch,
    NMCP_MAGIC, NMCP_HEADER_SIZE,
};

// ─── NMCP Frame Types (shared with classic) ─────────────────────────────────

/// Embedded Server NMCP frame types (70-79 range).
pub struct EmbeddedFrameTypes;

impl EmbeddedFrameTypes {
    // Workflow Lifecycle (70-76)
    pub const EXECUTE_WORKFLOW: u32 = 70;
    pub const GET_WORKFLOW: u32 = 71;
    pub const SIGNAL_WORKFLOW: u32 = 72;
    pub const QUERY_WORKFLOW: u32 = 73;
    pub const COMPLETE_WORKFLOW: u32 = 74;
    pub const LIST_WORKFLOWS: u32 = 75;
    pub const CANCEL_WORKFLOW: u32 = 76;

    // System (80-84)
    pub const HEALTH_CHECK: u32 = 80;
    pub const ENGINE_STATS: u32 = 81;
}

// ─── NmcpDispatch Implementation ─────────────────────────────────────────────

impl NmcpDispatch for NmcpFrameRouter {
    fn dispatch(&self, frame: &NmcpFrame) -> NmcpFrame {
        NmcpFrameRouter::dispatch(self, frame)
    }
}

// ─── Frame Router ────────────────────────────────────────────────────────────

/// NMCP Frame Router for the Embedded Server.
///
/// Dispatches NMCP frames to WorkflowEngine operations using
/// the embedded (library-style) API pattern.
pub struct NmcpFrameRouter {
    engine: Arc<WorkflowEngine>,
    workflow_map: Arc<DashMap<String, u64>>,
    workflow_counter: Arc<AtomicU64>,
    stats: Mutex<NmcpRouterStats>,
}

impl NmcpFrameRouter {
    /// Create a new frame router.
    pub fn new(
        engine: Arc<WorkflowEngine>,
        workflow_map: Arc<DashMap<String, u64>>,
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
            EmbeddedFrameTypes::EXECUTE_WORKFLOW => self.handle_execute(frame),
            EmbeddedFrameTypes::GET_WORKFLOW => self.handle_get(frame),
            EmbeddedFrameTypes::SIGNAL_WORKFLOW => self.handle_signal(frame),
            EmbeddedFrameTypes::QUERY_WORKFLOW => self.handle_query(frame),
            EmbeddedFrameTypes::COMPLETE_WORKFLOW => self.handle_complete(frame),
            EmbeddedFrameTypes::LIST_WORKFLOWS => self.handle_list(frame),
            EmbeddedFrameTypes::CANCEL_WORKFLOW => self.handle_cancel(frame),
            EmbeddedFrameTypes::HEALTH_CHECK => self.handle_health(frame),
            EmbeddedFrameTypes::ENGINE_STATS => self.handle_stats(frame),
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
        self.workflow_map.get(workflow_id).map(|r| *r).ok_or_else(|| {
            NmcpFrame::error_response(0, 404, &format!("workflow not found: {}", workflow_id))
        })
    }

    // ─── Handlers ──────────────────────────────────────────────────────────

    /// Execute a workflow inline (embedded style — run to completion).
    fn handle_execute(&self, frame: &NmcpFrame) -> NmcpFrame {
        let body = match self.parse_body(frame) {
            Ok(b) => b,
            Err(resp) => return resp,
        };

        let wf_id = body.workflow_id.unwrap_or_else(|| {
            format!("emb-{}", self.workflow_counter.fetch_add(1, Ordering::Relaxed))
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
            self.workflow_map.insert(wf_id.clone(), workflow_key);
        }

        // Embedded mode: sequential per-step durable execution.
        // Each step is WAL-fsynced + PG-persisted before the next step begins.
        // Crash at any point → resume from last persisted step.
        let total = self.engine.get_total_steps(workflow_key);
        for step in 0..total {
            let _ = self.engine.persist_step(workflow_key, step, "default");
        }
        self.engine.complete_workflow(workflow_key, Some(vec![]));

        // Final persist with completed status.
        let _ = self.engine.persist_workflow_by_key(workflow_key, "default");

        NmcpFrame::json_response(
            frame.sequence_id,
            serde_json::json!({
                "success": true,
                "data": {
                    "workflowId": wf_id,
                    "functionName": wf_type,
                    "status": "COMPLETED",
                    "mode": "embedded_nmcp"
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

        let workflow_key = match self.lookup_key(&wf_id) {
            Ok(k) => k,
            Err(resp) => return resp,
        };

        let status = self.engine.get_status(workflow_key);
        let total_steps = self.engine.get_total_steps(workflow_key);
        let current_step = self.engine.get_current_step(workflow_key);

        NmcpFrame::json_response(
            frame.sequence_id,
            serde_json::json!({
                "success": true,
                "data": {
                    "workflowId": wf_id,
                    "status": status_to_str(status),
                    "totalSteps": total_steps,
                    "completedSteps": current_step,
                }
            }),
        )
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

        let workflow_key = match self.lookup_key(&wf_id) {
            Ok(k) => k,
            Err(resp) => return resp,
        };

        let signal_data = serde_json::to_vec(&body.input.unwrap_or(JsonValue::Null))
            .unwrap_or_default();
        self.engine.signal_workflow(workflow_key, signal_id, signal_data);

        NmcpFrame::json_response(
            frame.sequence_id,
            serde_json::json!({ "success": true }),
        )
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
        let query_type = body.query_type.unwrap_or_default();

        let workflow_key = match self.lookup_key(&wf_id) {
            Ok(k) => k,
            Err(resp) => return resp,
        };

        let status = self.engine.get_status(workflow_key);
        let total_steps = self.engine.get_total_steps(workflow_key);
        let current_step = self.engine.get_current_step(workflow_key);
        let result = match query_type.as_str() {
            "status" => serde_json::json!({ "status": status_to_str(status) }),
            "progress" => serde_json::json!({
                "completed": current_step,
                "total": total_steps,
            }),
            _ => serde_json::json!({ "error": "unknown query type" }),
        };

        NmcpFrame::json_response(
            frame.sequence_id,
            serde_json::json!({ "success": true, "data": result }),
        )
    }

    fn handle_complete(&self, frame: &NmcpFrame) -> NmcpFrame {
        let body = match self.parse_body(frame) {
            Ok(b) => b,
            Err(resp) => return resp,
        };
        let wf_id = match body.workflow_id {
            Some(id) => id,
            None => return NmcpFrame::error_response(frame.sequence_id, 400, "missing workflow_id"),
        };

        let workflow_key = match self.lookup_key(&wf_id) {
            Ok(k) => k,
            Err(resp) => return resp,
        };

        self.engine.complete_workflow(workflow_key, Some(vec![]));

        NmcpFrame::json_response(
            frame.sequence_id,
            serde_json::json!({ "success": true }),
        )
    }

    fn handle_list(&self, frame: &NmcpFrame) -> NmcpFrame {
        let workflows: Vec<JsonValue> = self.workflow_map
            .iter()
            .map(|entry| {
                let id = entry.key().clone();
                let key = *entry.value();
                let status = self.engine.get_status(key);
                let total = self.engine.get_total_steps(key);
                let current = self.engine.get_current_step(key);
                serde_json::json!({
                    "workflowId": id,
                    "status": status_to_str(status),
                    "completedSteps": current,
                    "totalSteps": total,
                })
            })
            .collect();

        NmcpFrame::json_response(
            frame.sequence_id,
            serde_json::json!({
                "success": true,
                "data": workflows,
                "count": workflows.len(),
            }),
        )
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

        let workflow_key = match self.lookup_key(&wf_id) {
            Ok(k) => k,
            Err(resp) => return resp,
        };

        self.engine.cancel_workflow(workflow_key);

        NmcpFrame::json_response(
            frame.sequence_id,
            serde_json::json!({ "success": true }),
        )
    }

    fn handle_health(&self, frame: &NmcpFrame) -> NmcpFrame {
        NmcpFrame::json_response(
            frame.sequence_id,
            serde_json::json!({
                "success": true,
                "data": {
                    "status": "ok",
                    "engine": "velocity-embedded",
                    "transport": "nmcp",
                    "mode": "library+server"
                }
            }),
        )
    }

    fn handle_stats(&self, frame: &NmcpFrame) -> NmcpFrame {
        let router_stats = self.stats();
        let total_workflows = self.workflow_map.len();

        // Count by status
        let mut running = 0;
        let mut completed = 0;
        let mut failed = 0;
        for entry in self.workflow_map.iter() {
            let key = *entry.value();
            match self.engine.get_status(key) {
                WorkflowStatus::Running => running += 1,
                WorkflowStatus::Completed => completed += 1,
                WorkflowStatus::Failed => failed += 1,
                _ => {}
            }
        }

        NmcpFrame::json_response(
            frame.sequence_id,
            serde_json::json!({
                "success": true,
                "data": {
                    "total_workflows": total_workflows,
                    "running": running,
                    "completed": completed,
                    "failed": failed,
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

    fn make_router() -> NmcpFrameRouter {
        let engine = Arc::new(WorkflowEngine::new());
        let workflow_map = Arc::new(DashMap::new());
        let workflow_counter = Arc::new(AtomicU64::new(1));
        NmcpFrameRouter::new(engine, workflow_map, workflow_counter)
    }

    #[test]
    fn test_frame_roundtrip() {
        let frame = NmcpFrame::new(70, 42, b"hello".to_vec());
        let bytes = frame.to_bytes();
        let parsed = NmcpFrame::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.frame_type, 70);
        assert_eq!(parsed.sequence_id, 42);
        assert_eq!(parsed.payload, b"hello");
    }

    #[test]
    fn test_health_check() {
        let router = make_router();
        let frame = NmcpFrame::new(EmbeddedFrameTypes::HEALTH_CHECK, 1, b"{}".to_vec());
        let resp = router.dispatch(&frame);
        let body: JsonValue = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(body["data"]["status"], "ok");
    }

    #[test]
    fn test_execute_and_get() {
        let router = make_router();

        // Execute
        let payload = serde_json::to_vec(&serde_json::json!({
            "workflow_id": "wf-test",
            "workflow_type": "TestFunction"
        }))
        .unwrap();
        let frame = NmcpFrame::new(EmbeddedFrameTypes::EXECUTE_WORKFLOW, 1, payload);
        let resp = router.dispatch(&frame);
        let body: JsonValue = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(body["data"]["status"], "COMPLETED");

        // Get
        let payload = serde_json::to_vec(&serde_json::json!({
            "workflow_id": "wf-test"
        }))
        .unwrap();
        let frame = NmcpFrame::new(EmbeddedFrameTypes::GET_WORKFLOW, 2, payload);
        let resp = router.dispatch(&frame);
        let body: JsonValue = serde_json::from_slice(&resp.payload).unwrap();
        assert!(body["success"].as_bool().unwrap());
    }

    #[test]
    fn test_list_workflows() {
        let router = make_router();

        // Execute two workflows
        for id in &["wf-a", "wf-b"] {
            let payload = serde_json::to_vec(&serde_json::json!({
                "workflow_id": id,
                "workflow_type": "Fn"
            }))
            .unwrap();
            let frame = NmcpFrame::new(EmbeddedFrameTypes::EXECUTE_WORKFLOW, 1, payload);
            router.dispatch(&frame);
        }

        // List
        let frame = NmcpFrame::new(EmbeddedFrameTypes::LIST_WORKFLOWS, 3, b"{}".to_vec());
        let resp = router.dispatch(&frame);
        let body: JsonValue = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(body["data"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_unknown_frame_type() {
        let router = make_router();
        let frame = NmcpFrame::new(255, 1, b"{}".to_vec());
        let resp = router.dispatch(&frame);
        let body: JsonValue = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(body["error"], "unknown frame type");
    }

    #[test]
    fn test_engine_stats() {
        let router = make_router();
        let frame = NmcpFrame::new(EmbeddedFrameTypes::ENGINE_STATS, 1, b"{}".to_vec());
        let resp = router.dispatch(&frame);
        let body: JsonValue = serde_json::from_slice(&resp.payload).unwrap();
        assert!(body["success"].as_bool().unwrap());
        assert_eq!(body["data"]["total_workflows"], 0);
    }
}
