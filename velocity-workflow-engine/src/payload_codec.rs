//! Payload codec — encoding/decoding pipeline for workflow payloads.
//! Mirrors Temporal's `common/codec` and payload codec system with:
//! - Codec chaining (compression → encryption → custom)
//! - Compression codec (simulated deflate)
//! - Encryption codec (simulated AES-GCM)
//! - Payload metadata (content type, encoding, message type)
//! - Codec registry (register/lookup codecs by name)
//! - Payload validation and size limits
//! - Codec metrics

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

// ─── Codec Error ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum CodecError {
    EncodingFailed(String),
    DecodingFailed(String),
    InvalidPayload(String),
    SizeLimitExceeded { actual: usize, max: usize },
    UnknownCodec(String),
    ChainBroken { codec_name: String, step: usize },
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EncodingFailed(m)
            | Self::DecodingFailed(m)
            | Self::InvalidPayload(m)
            | Self::UnknownCodec(m) => write!(f, "{}", m),
            Self::SizeLimitExceeded { actual, max } => {
                write!(f, "payload size {} exceeds limit {}", actual, max)
            }
            Self::ChainBroken { codec_name, step } => {
                write!(f, "codec chain broken at step {} ({})", step, codec_name)
            }
        }
    }
}

// ─── Payload Metadata ────────────────────────────────────────────────────────

/// Metadata associated with a payload.
#[derive(Debug, Clone, Default)]
pub struct PayloadMetadata {
    /// Content type (e.g., "application/json", "application/protobuf").
    pub content_type: Option<String>,
    /// Encoding (e.g., "binary/plain", "binary/gzip", "binary/encrypted").
    pub encoding: Option<String>,
    /// Message type name (for protobuf).
    pub message_type: Option<String>,
    /// Custom metadata entries.
    pub entries: HashMap<String, Vec<u8>>,
}

impl PayloadMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_content_type(mut self, ct: &str) -> Self {
        self.content_type = Some(ct.to_string());
        self
    }
    pub fn with_encoding(mut self, enc: &str) -> Self {
        self.encoding = Some(enc.to_string());
        self
    }
    pub fn with_message_type(mut self, mt: &str) -> Self {
        self.message_type = Some(mt.to_string());
        self
    }
    pub fn with_entry(mut self, key: &str, value: &[u8]) -> Self {
        self.entries.insert(key.to_string(), value.to_vec());
        self
    }

    pub fn is_encrypted(&self) -> bool {
        self.encoding
            .as_deref()
            .is_some_and(|e| e.contains("encrypted"))
    }

    pub fn is_compressed(&self) -> bool {
        self.encoding
            .as_deref()
            .is_some_and(|e| e.contains("gzip") || e.contains("deflate"))
    }
}

// ─── Payload ─────────────────────────────────────────────────────────────────

/// A payload with data and metadata.
#[derive(Debug, Clone)]
pub struct Payload {
    pub data: Vec<u8>,
    pub metadata: PayloadMetadata,
}

impl Payload {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            metadata: PayloadMetadata::default(),
        }
    }

    pub fn with_metadata(data: Vec<u8>, metadata: PayloadMetadata) -> Self {
        Self { data, metadata }
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

// ─── PayloadCodec trait ──────────────────────────────────────────────────────

/// Trait for payload codecs. Each codec transforms payload data.
pub trait PayloadCodec: Send + Sync {
    /// Name of this codec.
    fn name(&self) -> &str;
    /// Encode (transform) the payload data.
    fn encode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError>;
    /// Decode (reverse transform) the payload data.
    fn decode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError>;
}

// ─── Legacy compat ───────────────────────────────────────────────────────────

/// Legacy trait (kept for backward compatibility).
pub trait PayloadCodecLegacy: Send + Sync {
    fn encode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError>;
    fn decode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError>;
}

// ─── Identity Codec ──────────────────────────────────────────────────────────

/// Passes through unchanged.
pub struct IdentityCodec;
impl PayloadCodec for IdentityCodec {
    fn name(&self) -> &str {
        "identity"
    }
    fn encode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(payload.to_vec())
    }
    fn decode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(payload.to_vec())
    }
}

// ─── XOR Codec (demo) ────────────────────────────────────────────────────────

/// XOR cipher codec (demonstration — real impl would use AES-GCM).
pub struct XorCodec {
    pub key: u8,
}
impl PayloadCodec for XorCodec {
    fn name(&self) -> &str {
        "xor"
    }
    fn encode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(payload.iter().map(|b| b ^ self.key).collect())
    }
    fn decode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(payload.iter().map(|b| b ^ self.key).collect())
    }
}

// ─── Compression Codec ───────────────────────────────────────────────────────

/// Simulated compression codec (run-length encoding for demo).
/// In production, this would use gzip/deflate/zstd.
pub struct CompressionCodec {
    pub min_size_to_compress: usize,
}

impl CompressionCodec {
    pub fn new() -> Self {
        Self {
            min_size_to_compress: 32,
        }
    }
    pub fn with_min_size(mut self, min: usize) -> Self {
        self.min_size_to_compress = min;
        self
    }

    /// Simple RLE compression: runs of the same byte are encoded as [count, byte].
    fn rle_encode(data: &[u8]) -> Vec<u8> {
        if data.is_empty() {
            return vec![];
        }
        let mut result = Vec::new();
        let mut current = data[0];
        let mut count = 1u8;
        for &b in &data[1..] {
            if b == current && count < 255 {
                count += 1;
            } else {
                result.push(count);
                result.push(current);
                current = b;
                count = 1;
            }
        }
        result.push(count);
        result.push(current);
        result
    }

    fn rle_decode(data: &[u8]) -> Result<Vec<u8>, CodecError> {
        if !data.len().is_multiple_of(2) {
            return Err(CodecError::DecodingFailed(
                "invalid RLE data (odd length)".into(),
            ));
        }
        let mut result = Vec::new();
        for chunk in data.chunks_exact(2) {
            let count = chunk[0];
            let byte = chunk[1];
            for _ in 0..count {
                result.push(byte);
            }
        }
        Ok(result)
    }
}

impl Default for CompressionCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl PayloadCodec for CompressionCodec {
    fn name(&self) -> &str {
        "compression"
    }

    fn encode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError> {
        if payload.len() < self.min_size_to_compress {
            // Too small to compress — prepend 0x00 marker (uncompressed)
            let mut result = vec![0x00];
            result.extend_from_slice(payload);
            return Ok(result);
        }
        let compressed = Self::rle_encode(payload);
        // Only use compression if it actually reduces size
        if compressed.len() < payload.len() {
            let mut result = vec![0x01]; // compressed marker
            result.extend_from_slice(&compressed);
            Ok(result)
        } else {
            let mut result = vec![0x00]; // uncompressed marker
            result.extend_from_slice(payload);
            Ok(result)
        }
    }

    fn decode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError> {
        if payload.is_empty() {
            return Err(CodecError::DecodingFailed("empty payload".into()));
        }
        match payload[0] {
            0x00 => Ok(payload[1..].to_vec()), // uncompressed
            0x01 => Self::rle_decode(&payload[1..]),
            _ => Err(CodecError::DecodingFailed(format!(
                "unknown compression marker: {}",
                payload[0]
            ))),
        }
    }
}

// ─── Encryption Codec ────────────────────────────────────────────────────────

/// Simulated encryption codec (XOR-based for demo).
/// In production, this would use AES-256-GCM.
pub struct EncryptionCodec {
    key: [u8; 32],
}

impl EncryptionCodec {
    pub fn new(key: &[u8; 32]) -> Self {
        Self { key: *key }
    }

    /// Create with a passphrase (simplified — real impl would use key derivation).
    pub fn from_passphrase(passphrase: &str) -> Self {
        let mut key = [0u8; 32];
        for (i, b) in passphrase.bytes().enumerate() {
            key[i % 32] ^= b;
        }
        Self { key }
    }
}

impl PayloadCodec for EncryptionCodec {
    fn name(&self) -> &str {
        "encryption"
    }

    fn encode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError> {
        // XOR with key (cycling)
        let encrypted: Vec<u8> = payload
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ self.key[i % 32])
            .collect();
        Ok(encrypted)
    }

    fn decode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError> {
        // XOR is symmetric
        self.encode(payload)
    }
}

// ─── Size-Limiting Codec ─────────────────────────────────────────────────────

/// Wrapper that enforces a maximum payload size.
pub struct SizeLimitCodec {
    inner: Arc<dyn PayloadCodec>,
    max_size: usize,
}

impl SizeLimitCodec {
    pub fn new(inner: Arc<dyn PayloadCodec>, max_size: usize) -> Self {
        Self { inner, max_size }
    }
}

impl PayloadCodec for SizeLimitCodec {
    fn name(&self) -> &str {
        "size-limit"
    }

    fn encode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError> {
        if payload.len() > self.max_size {
            return Err(CodecError::SizeLimitExceeded {
                actual: payload.len(),
                max: self.max_size,
            });
        }
        self.inner.encode(payload)
    }

    fn decode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError> {
        let decoded = self.inner.decode(payload)?;
        if decoded.len() > self.max_size {
            return Err(CodecError::SizeLimitExceeded {
                actual: decoded.len(),
                max: self.max_size,
            });
        }
        Ok(decoded)
    }
}

// ─── Codec Chain ─────────────────────────────────────────────────────────────

/// Applies multiple codecs in sequence. Encode: first → last. Decode: last → first.
pub struct CodecChain {
    codecs: Vec<Arc<dyn PayloadCodec>>,
    stats: CodecChainStats,
}

#[derive(Debug, Default)]
pub struct CodecChainStats {
    pub encode_count: AtomicU64,
    pub decode_count: AtomicU64,
    pub encode_errors: AtomicU64,
    pub decode_errors: AtomicU64,
}

impl CodecChain {
    pub fn new() -> Self {
        Self {
            codecs: Vec::new(),
            stats: CodecChainStats::default(),
        }
    }

    pub fn add(&mut self, codec: Arc<dyn PayloadCodec>) {
        self.codecs.push(codec);
    }

    pub fn encode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError> {
        let mut result = payload.to_vec();
        for (i, codec) in self.codecs.iter().enumerate() {
            result = codec.encode(&result).map_err(|_e| {
                self.stats.encode_errors.fetch_add(1, Ordering::Relaxed);
                CodecError::ChainBroken {
                    codec_name: codec.name().to_string(),
                    step: i,
                }
            })?;
        }
        self.stats.encode_count.fetch_add(1, Ordering::Relaxed);
        Ok(result)
    }

    pub fn decode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError> {
        let mut result = payload.to_vec();
        for (i, codec) in self.codecs.iter().rev().enumerate() {
            result = codec.decode(&result).map_err(|_e| {
                self.stats.decode_errors.fetch_add(1, Ordering::Relaxed);
                CodecError::ChainBroken {
                    codec_name: codec.name().to_string(),
                    step: i,
                }
            })?;
        }
        self.stats.decode_count.fetch_add(1, Ordering::Relaxed);
        Ok(result)
    }

    /// Encode a full Payload (data + metadata).
    pub fn encode_payload(&self, payload: &Payload) -> Result<Payload, CodecError> {
        let encoded_data = self.encode(&payload.data)?;
        Ok(Payload::with_metadata(
            encoded_data,
            payload.metadata.clone(),
        ))
    }

    /// Decode a full Payload.
    pub fn decode_payload(&self, payload: &Payload) -> Result<Payload, CodecError> {
        let decoded_data = self.decode(&payload.data)?;
        Ok(Payload::with_metadata(
            decoded_data,
            payload.metadata.clone(),
        ))
    }

    pub fn codec_count(&self) -> usize {
        self.codecs.len()
    }
    pub fn codec_names(&self) -> Vec<&str> {
        self.codecs.iter().map(|c| c.name()).collect()
    }
    pub fn encode_count(&self) -> u64 {
        self.stats.encode_count.load(Ordering::Relaxed)
    }
    pub fn decode_count(&self) -> u64 {
        self.stats.decode_count.load(Ordering::Relaxed)
    }
}

impl Default for CodecChain {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Codec Registry ──────────────────────────────────────────────────────────

/// Registry of named codecs. Allows looking up codecs by name.
pub struct CodecRegistry {
    codecs: Mutex<HashMap<String, Arc<dyn PayloadCodec>>>,
}

impl CodecRegistry {
    pub fn new() -> Self {
        let mut codecs = HashMap::new();
        codecs.insert(
            "identity".to_string(),
            Arc::new(IdentityCodec) as Arc<dyn PayloadCodec>,
        );
        Self {
            codecs: Mutex::new(codecs),
        }
    }

    /// Register a codec.
    pub fn register(&self, codec: Arc<dyn PayloadCodec>) {
        let name = codec.name().to_string();
        self.codecs.lock().unwrap().insert(name, codec);
    }

    /// Look up a codec by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn PayloadCodec>> {
        self.codecs.lock().unwrap().get(name).cloned()
    }

    /// Build a codec chain from a list of codec names.
    pub fn build_chain(&self, names: &[&str]) -> Result<CodecChain, CodecError> {
        let mut chain = CodecChain::new();
        for name in names {
            let codec = self
                .get(name)
                .ok_or_else(|| CodecError::UnknownCodec(name.to_string()))?;
            chain.add(codec);
        }
        Ok(chain)
    }

    /// List all registered codec names.
    pub fn list_codecs(&self) -> Vec<String> {
        self.codecs.lock().unwrap().keys().cloned().collect()
    }

    /// Number of registered codecs.
    pub fn codec_count(&self) -> usize {
        self.codecs.lock().unwrap().len()
    }
}

impl Default for CodecRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Payload Validator ───────────────────────────────────────────────────────

/// Validates payloads against size limits and content type rules.
pub struct PayloadValidator {
    max_payload_size: usize,
    allowed_content_types: Vec<String>,
}

impl PayloadValidator {
    pub fn new(max_size: usize) -> Self {
        Self {
            max_payload_size: max_size,
            allowed_content_types: Vec::new(),
        }
    }

    pub fn with_allowed_content_types(mut self, types: Vec<String>) -> Self {
        self.allowed_content_types = types;
        self
    }

    /// Validate a payload. Returns Ok(()) or an error.
    pub fn validate(&self, payload: &Payload) -> Result<(), CodecError> {
        if payload.size() > self.max_payload_size {
            return Err(CodecError::SizeLimitExceeded {
                actual: payload.size(),
                max: self.max_payload_size,
            });
        }
        if !self.allowed_content_types.is_empty() {
            if let Some(ref ct) = payload.metadata.content_type {
                if !self.allowed_content_types.contains(ct) {
                    return Err(CodecError::InvalidPayload(format!(
                        "content type '{}' not allowed",
                        ct
                    )));
                }
            }
        }
        Ok(())
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xor_codec() {
        let codec = XorCodec { key: 0x42 };
        let encoded = codec.encode(b"hello").unwrap();
        assert_ne!(encoded, b"hello");
        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, b"hello");
    }

    #[test]
    fn test_identity_codec() {
        let codec = IdentityCodec;
        assert_eq!(codec.encode(b"data").unwrap(), b"data");
        assert_eq!(codec.decode(b"data").unwrap(), b"data");
    }

    #[test]
    fn test_codec_chain() {
        let mut chain = CodecChain::new();
        chain.add(Arc::new(XorCodec { key: 0xAA }));
        chain.add(Arc::new(XorCodec { key: 0x55 }));
        let encoded = chain.encode(b"test").unwrap();
        let decoded = chain.decode(&encoded).unwrap();
        assert_eq!(decoded, b"test");
        assert_eq!(chain.encode_count(), 1);
    }

    #[test]
    fn test_compression_codec_small() {
        let codec = CompressionCodec::new();
        // Small payload — not compressed
        let data = b"hi";
        let encoded = codec.encode(data).unwrap();
        assert_eq!(encoded[0], 0x00); // uncompressed marker
        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_compression_codec_compressible() {
        let codec = CompressionCodec::new().with_min_size(4);
        // Highly compressible data
        let data = vec![0xAA; 100];
        let encoded = codec.encode(&data).unwrap();
        assert_eq!(encoded[0], 0x01); // compressed marker
        assert!(encoded.len() < data.len()); // actually smaller
        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_encryption_codec() {
        let codec = EncryptionCodec::from_passphrase("secret-key");
        let data = b"sensitive data here";
        let encrypted = codec.encode(data).unwrap();
        assert_ne!(encrypted, data);
        let decrypted = codec.decode(&encrypted).unwrap();
        assert_eq!(decrypted, data);
    }

    #[test]
    fn test_size_limit_codec() {
        let inner = Arc::new(IdentityCodec);
        let codec = SizeLimitCodec::new(inner, 10);
        assert!(codec.encode(b"small").is_ok());
        assert!(codec
            .encode(b"this is way too large for the limit")
            .is_err());
    }

    #[test]
    fn test_codec_registry() {
        let registry = CodecRegistry::new();
        assert!(registry.get("identity").is_some());
        assert!(registry.get("nonexistent").is_none());
        registry.register(Arc::new(XorCodec { key: 0xFF }));
        assert!(registry.get("xor").is_some());
    }

    #[test]
    fn test_codec_registry_build_chain() {
        let registry = CodecRegistry::new();
        registry.register(Arc::new(XorCodec { key: 0xAA }));
        let chain = registry.build_chain(&["identity", "xor"]).unwrap();
        assert_eq!(chain.codec_count(), 2);
        let encoded = chain.encode(b"test").unwrap();
        let decoded = chain.decode(&encoded).unwrap();
        assert_eq!(decoded, b"test");
    }

    #[test]
    fn test_codec_registry_unknown_codec() {
        let registry = CodecRegistry::new();
        assert!(registry.build_chain(&["nonexistent"]).is_err());
    }

    #[test]
    fn test_payload_metadata() {
        let meta = PayloadMetadata::new()
            .with_content_type("application/json")
            .with_encoding("binary/gzip")
            .with_message_type("WorkflowExecution");
        assert!(meta.is_compressed());
        assert!(!meta.is_encrypted());
        assert_eq!(meta.content_type.as_deref(), Some("application/json"));
    }

    #[test]
    fn test_payload_metadata_encrypted() {
        let meta = PayloadMetadata::new().with_encoding("binary/encrypted");
        assert!(meta.is_encrypted());
    }

    #[test]
    fn test_payload() {
        let p = Payload::new(b"hello".to_vec());
        assert_eq!(p.size(), 5);
        assert!(!p.is_empty());
        let empty = Payload::new(vec![]);
        assert!(empty.is_empty());
    }

    #[test]
    fn test_chain_encode_decode_payload() {
        let mut chain = CodecChain::new();
        chain.add(Arc::new(XorCodec { key: 0x42 }));
        let payload = Payload::with_metadata(
            b"workflow data".to_vec(),
            PayloadMetadata::new().with_content_type("application/json"),
        );
        let encoded = chain.encode_payload(&payload).unwrap();
        assert_ne!(encoded.data, payload.data);
        assert_eq!(
            encoded.metadata.content_type.as_deref(),
            Some("application/json")
        );
        let decoded = chain.decode_payload(&encoded).unwrap();
        assert_eq!(decoded.data, b"workflow data");
    }

    #[test]
    fn test_payload_validator_size() {
        let v = PayloadValidator::new(10);
        let ok = Payload::new(b"small".to_vec());
        assert!(v.validate(&ok).is_ok());
        let big = Payload::new(vec![0; 20]);
        assert!(v.validate(&big).is_err());
    }

    #[test]
    fn test_payload_validator_content_type() {
        let v = PayloadValidator::new(1000)
            .with_allowed_content_types(vec!["application/json".to_string()]);
        let ok = Payload::with_metadata(
            b"data".to_vec(),
            PayloadMetadata::new().with_content_type("application/json"),
        );
        assert!(v.validate(&ok).is_ok());
        let bad = Payload::with_metadata(
            b"data".to_vec(),
            PayloadMetadata::new().with_content_type("text/plain"),
        );
        assert!(v.validate(&bad).is_err());
    }

    #[test]
    fn test_codec_chain_names() {
        let mut chain = CodecChain::new();
        chain.add(Arc::new(IdentityCodec));
        chain.add(Arc::new(XorCodec { key: 1 }));
        assert_eq!(chain.codec_names(), vec!["identity", "xor"]);
    }

    #[test]
    fn test_compression_roundtrip_varied() {
        let codec = CompressionCodec::new().with_min_size(4);
        // Mixed data — may not compress well
        let data: Vec<u8> = (0..50).map(|i| (i % 7) as u8).collect();
        let encoded = codec.encode(&data).unwrap();
        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_encryption_different_keys() {
        let c1 = EncryptionCodec::from_passphrase("key1");
        let c2 = EncryptionCodec::from_passphrase("key2");
        let data = b"secret";
        let enc1 = c1.encode(data).unwrap();
        let enc2 = c2.encode(data).unwrap();
        assert_ne!(enc1, enc2); // Different keys → different output
    }

    #[test]
    fn test_registry_list() {
        let registry = CodecRegistry::new();
        registry.register(Arc::new(CompressionCodec::new()));
        registry.register(Arc::new(EncryptionCodec::from_passphrase("test")));
        let codecs = registry.list_codecs();
        assert!(codecs.len() >= 3); // identity + compression + encryption
    }
}
