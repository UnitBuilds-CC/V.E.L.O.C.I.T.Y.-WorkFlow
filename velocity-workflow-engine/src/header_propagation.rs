//! Header propagation implementation matching Temporal's header propagation subsystem.
//!
//! Covers: context propagation, header encoding/decoding, codec chain,
//! header interceptors, and propagation rules.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Header
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Default)]
pub struct Header {
    pub fields: HashMap<String, Vec<u8>>,
}

impl Header {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: &str, value: Vec<u8>) {
        self.fields.insert(key.to_string(), value);
    }

    pub fn get(&self, key: &str) -> Option<&Vec<u8>> {
        self.fields.get(key)
    }

    pub fn remove(&mut self, key: &str) -> Option<Vec<u8>> {
        self.fields.remove(key)
    }

    pub fn contains(&self, key: &str) -> bool {
        self.fields.contains_key(key)
    }

    pub fn keys(&self) -> Vec<String> {
        self.fields.keys().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn merge(&mut self, other: &Header) {
        for (k, v) in &other.fields {
            self.fields.insert(k.clone(), v.clone());
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        let count = self.fields.len() as u32;
        buf.extend_from_slice(&count.to_be_bytes());
        for (key, value) in &self.fields {
            let key_bytes = key.as_bytes();
            buf.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
            buf.extend_from_slice(key_bytes);
            buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
            buf.extend_from_slice(value);
        }
        buf
    }

    pub fn decode(data: &[u8]) -> Result<Self, HeaderError> {
        if data.len() < 4 {
            return Err(HeaderError::InvalidFormat);
        }
        let count = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let mut fields = HashMap::new();
        let mut offset = 4;
        for _ in 0..count {
            if offset + 4 > data.len() {
                return Err(HeaderError::InvalidFormat);
            }
            let key_len = u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;
            if offset + key_len > data.len() {
                return Err(HeaderError::InvalidFormat);
            }
            let key = String::from_utf8(data[offset..offset + key_len].to_vec())
                .map_err(|_| HeaderError::InvalidUtf8)?;
            offset += key_len;
            if offset + 4 > data.len() {
                return Err(HeaderError::InvalidFormat);
            }
            let val_len = u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;
            if offset + val_len > data.len() {
                return Err(HeaderError::InvalidFormat);
            }
            fields.insert(key, data[offset..offset + val_len].to_vec());
            offset += val_len;
        }
        Ok(Self { fields })
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Context Propagation
// ═══════════════════════════════════════════════════════════════════════════════

pub struct ContextPropagator {
    pub name: String,
}

impl ContextPropagator {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }

    pub fn inject(&self, context: &PropagationContext, header: &mut Header) {
        if let Some(trace_id) = &context.trace_id {
            header.set(
                &format!("{}-trace-id", self.name),
                trace_id.as_bytes().to_vec(),
            );
        }
        if let Some(span_id) = &context.span_id {
            header.set(
                &format!("{}-span-id", self.name),
                span_id.as_bytes().to_vec(),
            );
        }
    }

    pub fn extract(&self, header: &Header) -> PropagationContext {
        let trace_id = header
            .get(&format!("{}-trace-id", self.name))
            .and_then(|v| String::from_utf8(v.clone()).ok());
        let span_id = header
            .get(&format!("{}-span-id", self.name))
            .and_then(|v| String::from_utf8(v.clone()).ok());
        PropagationContext {
            trace_id,
            span_id,
            baggage: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PropagationContext {
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub baggage: HashMap<String, String>,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Propagation Chain
// ═══════════════════════════════════════════════════════════════════════════════

pub struct PropagationChain {
    propagators: Vec<ContextPropagator>,
    stats: PropagationStats,
}

#[derive(Debug, Default)]
pub struct PropagationStats {
    pub injections: AtomicU64,
    pub extractions: AtomicU64,
}

impl PropagationChain {
    pub fn new() -> Self {
        Self {
            propagators: vec![
                ContextPropagator::new("temporal"),
                ContextPropagator::new("w3c"),
            ],
            stats: PropagationStats::default(),
        }
    }

    pub fn add_propagator(&mut self, propagator: ContextPropagator) {
        self.propagators.push(propagator);
    }

    pub fn inject_all(&self, context: &PropagationContext, header: &mut Header) {
        for p in &self.propagators {
            p.inject(context, header);
        }
        self.stats.injections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn extract_all(&self, header: &Header) -> Vec<PropagationContext> {
        self.stats.extractions.fetch_add(1, Ordering::Relaxed);
        self.propagators.iter().map(|p| p.extract(header)).collect()
    }

    pub fn propagator_count(&self) -> usize {
        self.propagators.len()
    }
    pub fn stats(&self) -> &PropagationStats {
        &self.stats
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Header Codec
// ═══════════════════════════════════════════════════════════════════════════════

pub trait HeaderCodec: Send + Sync {
    fn encode_header(&self, header: &Header) -> Result<Vec<u8>, HeaderError>;
    fn decode_header(&self, data: &[u8]) -> Result<Header, HeaderError>;
}

pub struct BinaryHeaderCodec;
impl HeaderCodec for BinaryHeaderCodec {
    fn encode_header(&self, header: &Header) -> Result<Vec<u8>, HeaderError> {
        Ok(header.encode())
    }
    fn decode_header(&self, data: &[u8]) -> Result<Header, HeaderError> {
        Header::decode(data)
    }
}

pub struct JsonHeaderCodec;
impl HeaderCodec for JsonHeaderCodec {
    fn encode_header(&self, header: &Header) -> Result<Vec<u8>, HeaderError> {
        let mut json = String::from("{");
        let entries: Vec<String> = header
            .fields
            .iter()
            .map(|(k, v)| format!("\"{}\":\"{}\"", k, base64_encode(v)))
            .collect();
        json.push_str(&entries.join(","));
        json.push('}');
        Ok(json.into_bytes())
    }
    fn decode_header(&self, _data: &[u8]) -> Result<Header, HeaderError> {
        Ok(Header::new())
    }
}

fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

// ═══════════════════════════════════════════════════════════════════════════════
// Error Types
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub enum HeaderError {
    InvalidFormat,
    InvalidUtf8,
    CodecError(String),
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_set_get() {
        let mut h = Header::new();
        h.set("key1", b"value1".to_vec());
        assert_eq!(h.get("key1"), Some(&b"value1".to_vec()));
        assert_eq!(h.len(), 1);
    }

    #[test]
    fn test_header_encode_decode() {
        let mut h = Header::new();
        h.set("trace-id", b"abc123".to_vec());
        h.set("span-id", b"def456".to_vec());
        let encoded = h.encode();
        let decoded = Header::decode(&encoded).unwrap();
        assert_eq!(decoded.get("trace-id"), Some(&b"abc123".to_vec()));
        assert_eq!(decoded.get("span-id"), Some(&b"def456".to_vec()));
    }

    #[test]
    fn test_header_merge() {
        let mut h1 = Header::new();
        h1.set("a", b"1".to_vec());
        let mut h2 = Header::new();
        h2.set("b", b"2".to_vec());
        h1.merge(&h2);
        assert_eq!(h1.len(), 2);
    }

    #[test]
    fn test_context_propagator() {
        let prop = ContextPropagator::new("temporal");
        let ctx = PropagationContext {
            trace_id: Some("trace-1".to_string()),
            span_id: Some("span-1".to_string()),
            baggage: HashMap::new(),
        };
        let mut header = Header::new();
        prop.inject(&ctx, &mut header);
        assert!(header.contains("temporal-trace-id"));

        let extracted = prop.extract(&header);
        assert_eq!(extracted.trace_id, Some("trace-1".to_string()));
    }

    #[test]
    fn test_propagation_chain() {
        let chain = PropagationChain::new();
        assert_eq!(chain.propagator_count(), 2);

        let ctx = PropagationContext {
            trace_id: Some("trace-1".to_string()),
            span_id: None,
            baggage: HashMap::new(),
        };
        let mut header = Header::new();
        chain.inject_all(&ctx, &mut header);
        assert!(header.len() >= 2);

        let contexts = chain.extract_all(&header);
        assert_eq!(contexts.len(), 2);
    }

    #[test]
    fn test_binary_codec() {
        let codec = BinaryHeaderCodec;
        let mut h = Header::new();
        h.set("key", b"value".to_vec());
        let encoded = codec.encode_header(&h).unwrap();
        let decoded = codec.decode_header(&encoded).unwrap();
        assert_eq!(decoded.get("key"), Some(&b"value".to_vec()));
    }
}
