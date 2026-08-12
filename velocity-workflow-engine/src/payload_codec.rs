//! Payload codec — encoding/decoding pipeline for workflow payloads.
//! Supports chaining multiple codecs (compression, encryption, custom encoding).

use std::sync::Arc;

pub trait PayloadCodec: Send + Sync {
    fn encode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError>;
    fn decode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError>;
}

#[derive(Debug, Clone)]
pub enum CodecError {
    EncodingFailed(String),
    DecodingFailed(String),
    InvalidPayload(String),
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self { Self::EncodingFailed(m) | Self::DecodingFailed(m) | Self::InvalidPayload(m) => write!(f, "{}", m) }
    }
}

/// XOR cipher codec (demonstration — real impl would use AES-GCM).
pub struct XorCodec { pub key: u8 }
impl PayloadCodec for XorCodec {
    fn encode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError> { Ok(payload.iter().map(|b| b ^ self.key).collect()) }
    fn decode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError> { Ok(payload.iter().map(|b| b ^ self.key).collect()) }
}

/// Identity codec — passes through unchanged.
pub struct IdentityCodec;
impl PayloadCodec for IdentityCodec {
    fn encode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError> { Ok(payload.to_vec()) }
    fn decode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError> { Ok(payload.to_vec()) }
}

/// Codec chain — applies multiple codecs in sequence.
pub struct CodecChain { codecs: Vec<Arc<dyn PayloadCodec>> }
impl CodecChain {
    pub fn new() -> Self { Self { codecs: Vec::new() } }
    pub fn add(&mut self, codec: Arc<dyn PayloadCodec>) { self.codecs.push(codec); }
    pub fn encode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError> {
        let mut result = payload.to_vec();
        for codec in &self.codecs { result = codec.encode(&result)?; }
        Ok(result)
    }
    pub fn decode(&self, payload: &[u8]) -> Result<Vec<u8>, CodecError> {
        let mut result = payload.to_vec();
        for codec in self.codecs.iter().rev() { result = codec.decode(&result)?; }
        Ok(result)
    }
    pub fn codec_count(&self) -> usize { self.codecs.len() }
}
impl Default for CodecChain { fn default() -> Self { Self::new() } }

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
    fn test_codec_chain() {
        let mut chain = CodecChain::new();
        chain.add(Arc::new(XorCodec { key: 0xAA }));
        chain.add(Arc::new(XorCodec { key: 0x55 }));
        let encoded = chain.encode(b"test").unwrap();
        let decoded = chain.decode(&encoded).unwrap();
        assert_eq!(decoded, b"test");
    }
    #[test]
    fn test_identity_codec() {
        let codec = IdentityCodec;
        assert_eq!(codec.encode(b"data").unwrap(), b"data");
        assert_eq!(codec.decode(b"data").unwrap(), b"data");
    }
}
