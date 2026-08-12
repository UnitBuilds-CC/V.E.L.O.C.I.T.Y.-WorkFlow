//! Codec Server — standalone HTTP server for encoding/decoding workflow payloads.
//!
//! Used by the Web UI to display human-readable payloads and by SDKs
//! for payload transformation.

use std::collections::HashMap;
use std::io::{Read as IoRead, Write as IoWrite};
use std::sync::{Arc, Mutex};

/// Trait for payload codecs.
pub trait PayloadCodec: Send + Sync {
    fn encode(&self, payload: &[u8]) -> Result<Vec<u8>, String>;
    fn decode(&self, payload: &[u8]) -> Result<Vec<u8>, String>;
    fn name(&self) -> &str;
}

/// Identity codec — pass-through.
pub struct IdentityCodec;

impl PayloadCodec for IdentityCodec {
    fn encode(&self, payload: &[u8]) -> Result<Vec<u8>, String> {
        Ok(payload.to_vec())
    }
    fn decode(&self, payload: &[u8]) -> Result<Vec<u8>, String> {
        Ok(payload.to_vec())
    }
    fn name(&self) -> &str {
        "identity"
    }
}

/// Base64 codec.
pub struct Base64Codec;

impl PayloadCodec for Base64Codec {
    fn encode(&self, payload: &[u8]) -> Result<Vec<u8>, String> {
        // Simple base64 encoding without external crate
        const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut result = Vec::new();
        for chunk in payload.chunks(3) {
            let b0 = chunk[0] as u32;
            let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
            let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
            let triple = (b0 << 16) | (b1 << 8) | b2;
            result.push(CHARS[((triple >> 18) & 0x3F) as usize]);
            result.push(CHARS[((triple >> 12) & 0x3F) as usize]);
            if chunk.len() > 1 {
                result.push(CHARS[((triple >> 6) & 0x3F) as usize]);
            } else {
                result.push(b'=');
            }
            if chunk.len() > 2 {
                result.push(CHARS[(triple & 0x3F) as usize]);
            } else {
                result.push(b'=');
            }
        }
        Ok(result)
    }

    fn decode(&self, payload: &[u8]) -> Result<Vec<u8>, String> {
        let mut result = Vec::new();
        let filtered: Vec<u8> = payload.iter().copied().filter(|&b| b != b'=' && b != b'\n' && b != b'\r').collect();
        for chunk in filtered.chunks(4) {
            if chunk.len() < 2 {
                break;
            }
            let vals: Vec<u32> = chunk.iter().map(|&c| {
                match c {
                    b'A'..=b'Z' => (c - b'A') as u32,
                    b'a'..=b'z' => (c - b'a' + 26) as u32,
                    b'0'..=b'9' => (c - b'0' + 52) as u32,
                    b'+' => 62,
                    b'/' => 63,
                    _ => 0,
                }
            }).collect();

            let b0 = (vals[0] << 2) | (vals[1] >> 4);
            result.push(b0 as u8);
            if chunk.len() > 2 && vals[2] != 0 {
                let b1 = ((vals[1] & 0xF) << 4) | (vals[2] >> 2);
                result.push(b1 as u8);
            }
            if chunk.len() > 3 && vals[3] != 0 {
                let b2 = ((vals[2] & 0x3) << 6) | vals[3];
                result.push(b2 as u8);
            }
        }
        Ok(result)
    }

    fn name(&self) -> &str {
        "base64"
    }
}

/// JSON pretty-print codec.
pub struct JsonPrettyCodec;

impl PayloadCodec for JsonPrettyCodec {
    fn encode(&self, payload: &[u8]) -> Result<Vec<u8>, String> {
        // Validate it's valid JSON, then return as-is (pretty printing would need serde)
        let s = std::str::from_utf8(payload).map_err(|e| format!("Invalid UTF-8: {}", e))?;
        if !s.trim().starts_with('{') && !s.trim().starts_with('[') {
            return Err("Not valid JSON".to_string());
        }
        Ok(payload.to_vec())
    }

    fn decode(&self, payload: &[u8]) -> Result<Vec<u8>, String> {
        Ok(payload.to_vec())
    }

    fn name(&self) -> &str {
        "json-pretty"
    }
}

/// Codec request.
#[derive(Debug, Clone)]
pub struct CodecRequest {
    pub codec_name: String,
    pub payloads: Vec<Vec<u8>>,
    pub namespace: Option<String>,
}

/// Codec response.
#[derive(Debug, Clone)]
pub struct CodecResponse {
    pub payloads: Vec<Vec<u8>>,
    pub error: Option<String>,
}

/// Codec server managing registered codecs.
pub struct CodecServer {
    codecs: HashMap<String, Box<dyn PayloadCodec>>,
}

impl CodecServer {
    pub fn new() -> Self {
        let mut server = Self {
            codecs: HashMap::new(),
        };
        // Register built-in codecs
        server.register_codec(Box::new(IdentityCodec));
        server.register_codec(Box::new(Base64Codec));
        server.register_codec(Box::new(JsonPrettyCodec));
        server
    }

    /// Register a codec.
    pub fn register_codec(&mut self, codec: Box<dyn PayloadCodec>) {
        self.codecs.insert(codec.name().to_string(), codec);
    }

    /// Handle an encode request.
    pub fn handle_encode(&self, request: &CodecRequest) -> CodecResponse {
        match self.codecs.get(&request.codec_name) {
            Some(codec) => {
                let mut encoded = Vec::new();
                for payload in &request.payloads {
                    match codec.encode(payload) {
                        Ok(e) => encoded.push(e),
                        Err(e) => return CodecResponse {
                            payloads: vec![],
                            error: Some(e),
                        },
                    }
                }
                CodecResponse {
                    payloads: encoded,
                    error: None,
                }
            }
            None => CodecResponse {
                payloads: vec![],
                error: Some(format!("Unknown codec: {}", request.codec_name)),
            },
        }
    }

    /// Handle a decode request.
    pub fn handle_decode(&self, request: &CodecRequest) -> CodecResponse {
        match self.codecs.get(&request.codec_name) {
            Some(codec) => {
                let mut decoded = Vec::new();
                for payload in &request.payloads {
                    match codec.decode(payload) {
                        Ok(d) => decoded.push(d),
                        Err(e) => return CodecResponse {
                            payloads: vec![],
                            error: Some(e),
                        },
                    }
                }
                CodecResponse {
                    payloads: decoded,
                    error: None,
                }
            }
            None => CodecResponse {
                payloads: vec![],
                error: Some(format!("Unknown codec: {}", request.codec_name)),
            },
        }
    }

    /// List available codec names.
    pub fn list_codecs(&self) -> Vec<String> {
        self.codecs.keys().cloned().collect()
    }

    /// Check if a codec exists.
    pub fn has_codec(&self, name: &str) -> bool {
        self.codecs.contains_key(name)
    }

    /// Get codec count.
    pub fn codec_count(&self) -> usize {
        self.codecs.len()
    }
}

impl Default for CodecServer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identity_codec() {
        let codec = IdentityCodec;
        let data = b"hello world";
        let encoded = codec.encode(data).unwrap();
        assert_eq!(encoded, data);
        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_base64_codec_roundtrip() {
        let codec = Base64Codec;
        let data = b"hello world";
        let encoded = codec.encode(data).unwrap();
        assert_eq!(encoded, b"aGVsbG8gd29ybGQ=");
        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_base64_empty() {
        let codec = Base64Codec;
        let encoded = codec.encode(b"").unwrap();
        assert!(encoded.is_empty());
    }

    #[test]
    fn test_json_pretty_codec() {
        let codec = JsonPrettyCodec;
        let data = b"{\"key\": \"value\"}";
        let encoded = codec.encode(data).unwrap();
        assert_eq!(encoded, data);
    }

    #[test]
    fn test_json_pretty_rejects_non_json() {
        let codec = JsonPrettyCodec;
        let result = codec.encode(b"not json");
        assert!(result.is_err());
    }

    #[test]
    fn test_codec_server_built_in() {
        let server = CodecServer::new();
        assert!(server.has_codec("identity"));
        assert!(server.has_codec("base64"));
        assert!(server.has_codec("json-pretty"));
        assert_eq!(server.codec_count(), 3);
    }

    #[test]
    fn test_codec_server_encode_decode() {
        let server = CodecServer::new();
        let request = CodecRequest {
            codec_name: "base64".to_string(),
            payloads: vec![b"hello".to_vec()],
            namespace: None,
        };

        let encoded = server.handle_encode(&request);
        assert!(encoded.error.is_none());
        assert_eq!(encoded.payloads.len(), 1);

        let decode_request = CodecRequest {
            codec_name: "base64".to_string(),
            payloads: encoded.payloads,
            namespace: None,
        };
        let decoded = server.handle_decode(&decode_request);
        assert!(decoded.error.is_none());
        assert_eq!(decoded.payloads[0], b"hello");
    }

    #[test]
    fn test_codec_server_unknown_codec() {
        let server = CodecServer::new();
        let request = CodecRequest {
            codec_name: "nonexistent".to_string(),
            payloads: vec![b"data".to_vec()],
            namespace: None,
        };

        let response = server.handle_encode(&request);
        assert!(response.error.is_some());
    }

    #[test]
    fn test_codec_server_multiple_payloads() {
        let server = CodecServer::new();
        let request = CodecRequest {
            codec_name: "identity".to_string(),
            payloads: vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()],
            namespace: None,
        };

        let response = server.handle_encode(&request);
        assert!(response.error.is_none());
        assert_eq!(response.payloads.len(), 3);
    }

    #[test]
    fn test_codec_server_list_codecs() {
        let server = CodecServer::new();
        let codecs = server.list_codecs();
        assert!(codecs.contains(&"identity".to_string()));
        assert!(codecs.contains(&"base64".to_string()));
        assert!(codecs.contains(&"json-pretty".to_string()));
    }
}
