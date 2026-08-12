//! Payload encoding/decoding codecs.
//!
//! Provides the `PayloadCodec` trait and built-in implementations:
//! - `JsonCodec` — JSON serialization via `serde_json`
//! - `BinaryCodec` — raw byte passthrough
//! - `NullCodec` — encodes everything as empty bytes
//!
//! # Example
//!
//! ```rust
//! use velocity_sdk::codec::{PayloadCodec, JsonCodec};
//!
//! let codec = JsonCodec;
//! let encoded = codec.encode(&"hello").unwrap();
//! let decoded: String = codec.decode(&encoded).unwrap();
//! assert_eq!(decoded, "hello");
//! ```

use std::fmt;

/// Trait for payload encoding and decoding.
pub trait PayloadCodec: Send + Sync {
    /// Encode a value to bytes.
    fn encode(&self, data: &[u8]) -> Result<Vec<u8>, CodecError>;

    /// Decode bytes to a value.
    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, CodecError>;
}

/// Error type for codec operations.
#[derive(Debug, Clone)]
pub struct CodecError {
    pub message: String,
}

impl CodecError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self { message: msg.into() }
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CodecError: {}", self.message)
    }
}

impl std::error::Error for CodecError {}

/// JSON codec — passes through raw bytes (JSON encoding handled at application layer).
///
/// In a full implementation, this would use `serde_json` for serialization.
/// Since the engine works with raw `Vec<u8>`, this codec validates UTF-8
/// and provides a convenient wrapper.
#[derive(Debug, Clone, Copy)]
pub struct JsonCodec;

impl PayloadCodec for JsonCodec {
    fn encode(&self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
        // Validate that data is valid UTF-8 (JSON requirement)
        if !data.is_empty() {
            std::str::from_utf8(data)
                .map_err(|e| CodecError::new(format!("Invalid UTF-8 for JSON: {}", e)))?;
        }
        Ok(data.to_vec())
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
        if !data.is_empty() {
            std::str::from_utf8(data)
                .map_err(|e| CodecError::new(format!("Invalid UTF-8 for JSON: {}", e)))?;
        }
        Ok(data.to_vec())
    }
}

impl JsonCodec {
    /// Encode a serializable value to JSON bytes.
    ///
    /// Requires `serde_json` to be available. This is a convenience method
    /// that serializes any `serde::Serialize` type.
    #[cfg(feature = "serde_json")]
    pub fn encode_value<T: serde::Serialize>(&self, value: &T) -> Result<Vec<u8>, CodecError> {
        serde_json::to_vec(value)
            .map_err(|e| CodecError::new(format!("JSON encode failed: {}", e)))
    }

    /// Decode JSON bytes to a deserializable value.
    #[cfg(feature = "serde_json")]
    pub fn decode_value<T: serde::de::DeserializeOwned>(&self, data: &[u8]) -> Result<T, CodecError> {
        serde_json::from_slice(data)
            .map_err(|e| CodecError::new(format!("JSON decode failed: {}", e)))
    }
}

/// Binary codec — raw byte passthrough.
#[derive(Debug, Clone, Copy)]
pub struct BinaryCodec;

impl PayloadCodec for BinaryCodec {
    fn encode(&self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(data.to_vec())
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(data.to_vec())
    }
}

/// Null codec — encodes everything as empty bytes.
///
/// Useful for workflows that take no input and return no output.
#[derive(Debug, Clone, Copy)]
pub struct NullCodec;

impl PayloadCodec for NullCodec {
    fn encode(&self, _data: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(Vec::new())
    }

    fn decode(&self, _data: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(Vec::new())
    }
}

/// Chain multiple codecs together.
///
/// Encoding applies codecs left-to-right; decoding applies right-to-left.
pub struct CodecChain {
    codecs: Vec<Box<dyn PayloadCodec>>,
}

impl CodecChain {
    /// Create a new codec chain.
    pub fn new(codecs: Vec<Box<dyn PayloadCodec>>) -> Result<Self, CodecError> {
        if codecs.is_empty() {
            return Err(CodecError::new("CodecChain requires at least one codec"));
        }
        Ok(Self { codecs })
    }
}

impl PayloadCodec for CodecChain {
    fn encode(&self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
        let mut current = data.to_vec();
        for codec in &self.codecs {
            current = codec.encode(&current)?;
        }
        Ok(current)
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
        let mut current = data.to_vec();
        for codec in self.codecs.iter().rev() {
            current = codec.decode(&current)?;
        }
        Ok(current)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_codec_roundtrip() {
        let codec = JsonCodec;
        let data = br#"{"key": "value"}"#;
        let encoded = codec.encode(data).unwrap();
        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_json_codec_invalid_utf8() {
        let codec = JsonCodec;
        let data = &[0xFF, 0xFE];
        assert!(codec.encode(data).is_err());
    }

    #[test]
    fn test_binary_codec_roundtrip() {
        let codec = BinaryCodec;
        let data = vec![0u8, 1, 2, 3, 255];
        let encoded = codec.encode(&data).unwrap();
        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_null_codec() {
        let codec = NullCodec;
        let data = b"anything";
        let encoded = codec.encode(data).unwrap();
        assert!(encoded.is_empty());
        let decoded = codec.decode(data).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_codec_chain() {
        let chain = CodecChain::new(vec![
            Box::new(JsonCodec),
            Box::new(BinaryCodec),
        ]).unwrap();

        let data = br#"{"test": true}"#;
        let encoded = chain.encode(data).unwrap();
        let decoded = chain.decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_empty_chain_error() {
        assert!(CodecChain::new(vec![]).is_err());
    }
}
