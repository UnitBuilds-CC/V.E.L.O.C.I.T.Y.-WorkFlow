//! Persistence serialization implementation matching Temporal's serialization subsystem.
//!
//! Covers: event serialization, data encoding, protobuf-like binary format,
//! schema versioning, and type conversion.

use std::collections::HashMap;
use std::sync::{Arc, RwLock, atomic::{AtomicU64, Ordering}};

// ═══════════════════════════════════════════════════════════════════════════════
// Serialization Types
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodingType {
    Proto3 = 0,
    Json = 1,
    MsgPack = 2,
    Thrift = 3,
}

#[derive(Debug, Clone)]
pub struct SerializedData {
    pub data: Vec<u8>,
    pub encoding: EncodingType,
    pub schema_version: i32,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Event Serializer
// ═══════════════════════════════════════════════════════════════════════════════

pub struct EventSerializer {
    encoding: EncodingType,
    stats: SerializerStats,
}

#[derive(Debug, Default)]
pub struct SerializerStats {
    pub serialized_count: AtomicU64,
    pub deserialized_count: AtomicU64,
    pub total_bytes_written: AtomicU64,
    pub total_bytes_read: AtomicU64,
    pub errors: AtomicU64,
}

impl EventSerializer {
    pub fn new(encoding: EncodingType) -> Self {
        Self { encoding, stats: SerializerStats::default() }
    }

    pub fn serialize_event(&self, event: &SerializableEvent) -> Result<SerializedData, SerializationError> {
        let mut buf = Vec::new();
        // Magic bytes
        buf.extend_from_slice(&[0x56, 0x45]); // "VE"
        // Schema version
        buf.extend_from_slice(&event.schema_version.to_be_bytes());
        // Event type
        buf.extend_from_slice(&(event.event_type as u32).to_be_bytes());
        // Event ID
        buf.extend_from_slice(&event.event_id.to_be_bytes());
        // Timestamp
        buf.extend_from_slice(&event.timestamp.to_be_bytes());
        // Attributes count
        let attr_count = event.attributes.len() as u32;
        buf.extend_from_slice(&attr_count.to_be_bytes());
        // Attributes
        for (key, value) in &event.attributes {
            let key_bytes = key.as_bytes();
            buf.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
            buf.extend_from_slice(key_bytes);
            buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
            buf.extend_from_slice(value);
        }

        self.stats.serialized_count.fetch_add(1, Ordering::Relaxed);
        self.stats.total_bytes_written.fetch_add(buf.len() as u64, Ordering::Relaxed);

        Ok(SerializedData {
            data: buf,
            encoding: self.encoding,
            schema_version: event.schema_version,
        })
    }

    pub fn deserialize_event(&self, data: &SerializedData) -> Result<SerializableEvent, SerializationError> {
        let buf = &data.data;
        if buf.len() < 26 { return Err(SerializationError::InvalidData("too short".to_string())); }
        if buf[0] != 0x56 || buf[1] != 0x45 { return Err(SerializationError::InvalidData("bad magic".to_string())); }

        let mut offset = 2;
        let schema_version = i32::from_be_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]]);
        offset += 4;
        let event_type = u32::from_be_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]]);
        offset += 4;
        let event_id = i64::from_be_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3], buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7]]);
        offset += 8;
        let timestamp = i64::from_be_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3], buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7]]);
        offset += 8;
        let attr_count = u32::from_be_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]]) as usize;
        offset += 4;

        let mut attributes = HashMap::new();
        for _ in 0..attr_count {
            if offset + 4 > buf.len() { return Err(SerializationError::InvalidData("truncated key len".to_string())); }
            let key_len = u32::from_be_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]]) as usize;
            offset += 4;
            if offset + key_len > buf.len() { return Err(SerializationError::InvalidData("truncated key".to_string())); }
            let key = String::from_utf8(buf[offset..offset+key_len].to_vec()).map_err(|_| SerializationError::InvalidUtf8)?;
            offset += key_len;
            if offset + 4 > buf.len() { return Err(SerializationError::InvalidData("truncated val len".to_string())); }
            let val_len = u32::from_be_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]]) as usize;
            offset += 4;
            if offset + val_len > buf.len() { return Err(SerializationError::InvalidData("truncated val".to_string())); }
            attributes.insert(key, buf[offset..offset+val_len].to_vec());
            offset += val_len;
        }

        self.stats.deserialized_count.fetch_add(1, Ordering::Relaxed);
        self.stats.total_bytes_read.fetch_add(data.data.len() as u64, Ordering::Relaxed);

        Ok(SerializableEvent {
            event_type: event_type as i32,
            event_id,
            timestamp,
            attributes,
            schema_version,
        })
    }

    pub fn stats(&self) -> &SerializerStats { &self.stats }
}

#[derive(Debug, Clone)]
pub struct SerializableEvent {
    pub event_type: i32,
    pub event_id: i64,
    pub timestamp: i64,
    pub attributes: HashMap<String, Vec<u8>>,
    pub schema_version: i32,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Batch Serializer
// ═══════════════════════════════════════════════════════════════════════════════

pub struct BatchSerializer {
    encoding: EncodingType,
}

impl BatchSerializer {
    pub fn new(encoding: EncodingType) -> Self {
        Self { encoding }
    }

    pub fn serialize_batch(&self, events: &[SerializableEvent]) -> Result<SerializedData, SerializationError> {
        let serializer = EventSerializer::new(self.encoding);
        let mut combined = Vec::new();
        combined.extend_from_slice(&(events.len() as u32).to_be_bytes());
        for event in events {
            let serialized = serializer.serialize_event(event)?;
            combined.extend_from_slice(&(serialized.data.len() as u32).to_be_bytes());
            combined.extend_from_slice(&serialized.data);
        }
        Ok(SerializedData {
            data: combined,
            encoding: self.encoding,
            schema_version: events.first().map(|e| e.schema_version).unwrap_or(0),
        })
    }

    pub fn deserialize_batch(&self, data: &SerializedData) -> Result<Vec<SerializableEvent>, SerializationError> {
        let buf = &data.data;
        if buf.len() < 4 { return Err(SerializationError::InvalidData("too short".to_string())); }
        let count = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let serializer = EventSerializer::new(self.encoding);
        let mut events = Vec::new();
        let mut offset = 4;
        for _ in 0..count {
            if offset + 4 > buf.len() { break; }
            let len = u32::from_be_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]]) as usize;
            offset += 4;
            if offset + len > buf.len() { break; }
            let single = SerializedData { data: buf[offset..offset+len].to_vec(), encoding: data.encoding, schema_version: data.schema_version };
            events.push(serializer.deserialize_event(&single)?);
            offset += len;
        }
        Ok(events)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Schema Registry
// ═══════════════════════════════════════════════════════════════════════════════

pub struct SchemaRegistry {
    schemas: RwLock<HashMap<String, SchemaEntry>>,
}

#[derive(Debug, Clone)]
pub struct SchemaEntry {
    pub name: String,
    pub version: i32,
    pub fields: Vec<SchemaField>,
}

#[derive(Debug, Clone)]
pub struct SchemaField {
    pub name: String,
    pub field_type: FieldType,
    pub required: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    String,
    Int32,
    Int64,
    Float64,
    Bool,
    Bytes,
    Timestamp,
    Enum,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self { schemas: RwLock::new(HashMap::new()) }
    }

    pub fn register(&self, entry: SchemaEntry) {
        self.schemas.write().unwrap().insert(entry.name.clone(), entry);
    }

    pub fn get(&self, name: &str) -> Option<SchemaEntry> {
        self.schemas.read().unwrap().get(name).cloned()
    }

    pub fn get_version(&self, name: &str) -> Option<i32> {
        self.schemas.read().unwrap().get(name).map(|s| s.version)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Error Types
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub enum SerializationError {
    InvalidData(String),
    InvalidUtf8,
    UnsupportedEncoding,
    SchemaNotFound(String),
    VersionMismatch { expected: i32, actual: i32 },
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event() -> SerializableEvent {
        let mut attrs = HashMap::new();
        attrs.insert("workflow_id".to_string(), b"wf-1".to_vec());
        attrs.insert("run_id".to_string(), b"run-1".to_vec());
        SerializableEvent {
            event_type: 1,
            event_id: 42,
            timestamp: 1700000000000,
            attributes: attrs,
            schema_version: 1,
        }
    }

    #[test]
    fn test_serialize_deserialize_event() {
        let serializer = EventSerializer::new(EncodingType::Proto3);
        let event = make_event();
        let serialized = serializer.serialize_event(&event).unwrap();
        assert_eq!(serialized.encoding, EncodingType::Proto3);
        assert!(!serialized.data.is_empty());

        let deserialized = serializer.deserialize_event(&serialized).unwrap();
        assert_eq!(deserialized.event_type, 1);
        assert_eq!(deserialized.event_id, 42);
        assert_eq!(deserialized.timestamp, 1700000000000);
        assert_eq!(deserialized.attributes.get("workflow_id"), Some(&b"wf-1".to_vec()));
    }

    #[test]
    fn test_batch_serialize_deserialize() {
        let batch = BatchSerializer::new(EncodingType::Proto3);
        let events = vec![make_event(), make_event()];
        let serialized = batch.serialize_batch(&events).unwrap();
        let deserialized = batch.deserialize_batch(&serialized).unwrap();
        assert_eq!(deserialized.len(), 2);
    }

    #[test]
    fn test_serializer_stats() {
        let serializer = EventSerializer::new(EncodingType::Proto3);
        serializer.serialize_event(&make_event()).unwrap();
        serializer.serialize_event(&make_event()).unwrap();
        assert_eq!(serializer.stats().serialized_count.load(Ordering::Relaxed), 2);
        assert!(serializer.stats().total_bytes_written.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn test_schema_registry() {
        let registry = SchemaRegistry::new();
        registry.register(SchemaEntry {
            name: "HistoryEvent".to_string(),
            version: 2,
            fields: vec![
                SchemaField { name: "event_id".to_string(), field_type: FieldType::Int64, required: true },
                SchemaField { name: "event_type".to_string(), field_type: FieldType::Enum, required: true },
            ],
        });

        let entry = registry.get("HistoryEvent").unwrap();
        assert_eq!(entry.version, 2);
        assert_eq!(entry.fields.len(), 2);
        assert_eq!(registry.get_version("HistoryEvent"), Some(2));
    }

    #[test]
    fn test_deserialize_invalid() {
        let serializer = EventSerializer::new(EncodingType::Proto3);
        let bad_data = SerializedData { data: vec![0, 0, 0], encoding: EncodingType::Proto3, schema_version: 1 };
        assert!(serializer.deserialize_event(&bad_data).is_err());
    }
}
