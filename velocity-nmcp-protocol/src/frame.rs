//! NMCP frame types, parsing, and shared protocol definitions.
//!
//! The NMCP wire format: 16-byte binary header + JSON payload.
//!   magic(4) + frame_type(4) + payload_len(4) + sequence_id(4) + payload(N)

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

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

// ─── Router Stats ────────────────────────────────────────────────────────────

/// Statistics for the NMCP frame router.
#[derive(Debug, Clone, Default, Serialize)]
pub struct NmcpRouterStats {
    pub frames_received: u64,
    pub frames_dispatched: u64,
    pub errors: u64,
    pub unknown_types: u64,
}

// ─── Dispatch Trait ──────────────────────────────────────────────────────────

/// Trait for NMCP frame dispatch. Implemented by each flavor's frame router.
///
/// The shmem and WebSocket servers are generic over this trait, allowing
/// them to work with any router implementation (Classic, Embedded, etc.).
pub trait NmcpDispatch: Send + Sync + 'static {
    /// Dispatch an NMCP frame and return a response frame.
    fn dispatch(&self, frame: &NmcpFrame) -> NmcpFrame;
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
    fn test_frame_bad_magic() {
        let mut bytes = NmcpFrame::new(1, 1, vec![]).to_bytes();
        bytes[0] = 0xFF;
        assert!(NmcpFrame::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_frame_too_short() {
        assert!(NmcpFrame::from_bytes(&[0u8; 10]).is_none());
    }

    #[test]
    fn test_error_response() {
        let frame = NmcpFrame::error_response(3, 404, "not found");
        let body: JsonValue = serde_json::from_slice(&frame.payload).unwrap();
        assert_eq!(body["success"], false);
        assert_eq!(body["status"], 404);
    }

    #[test]
    fn test_json_response() {
        let body = serde_json::json!({"success": true, "data": 42});
        let frame = NmcpFrame::json_response(1, body);
        let parsed: JsonValue = serde_json::from_slice(&frame.payload).unwrap();
        assert_eq!(parsed["success"], true);
        assert_eq!(parsed["data"], 42);
    }

    #[test]
    fn test_request_body_deserialize() {
        let json = r#"{"workflow_id": "test-wf", "workflow_type": "bench"}"#;
        let body: NmcpRequestBody = serde_json::from_str(json).unwrap();
        assert_eq!(body.workflow_id, Some("test-wf".to_string()));
        assert_eq!(body.workflow_type, Some("bench".to_string()));
        assert_eq!(body.signal_name, None);
    }
}
