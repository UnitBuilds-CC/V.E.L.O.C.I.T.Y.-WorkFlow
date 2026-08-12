//! File-based cold storage archival backend.
//! Writes archived workflow data to the filesystem in a structured directory layout.
//! Supports listing, retrieval, and garbage collection of archived workflows.

use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

use crate::engine::WorkflowStatus;

#[cfg(feature = "cloud-s3")]
use sha2::Digest;

/// A record stored in cold storage.
#[derive(Debug, Clone)]
pub struct ColdStorageRecord {
    pub workflow_key: u64,
    pub workflow_id: u64,
    pub run_id: u64,
    pub workflow_type_id: u64,
    pub namespace_id: u64,
    pub status: WorkflowStatus,
    pub input_data: Option<Vec<u8>>,
    pub result_data: Option<Vec<u8>>,
    pub step_results: HashMap<u32, Vec<u8>>,
    pub event_history: Vec<(u64, Vec<u8>)>, // (event_type, payload)
    pub archived_at_ms: u64,
    pub file_path: String,
}

/// File-based cold storage backend.
pub struct FileColdStorage {
    base_dir: PathBuf,
    index: RwLock<HashMap<u64, ColdStorageRecord>>,
    next_id: AtomicU64,
}

impl FileColdStorage {
    /// Create a new file-based cold storage at the given directory.
    pub fn new(base_dir: &str) -> io::Result<Self> {
        let path = PathBuf::from(base_dir);
        fs::create_dir_all(&path)?;
        fs::create_dir_all(path.join("data"))?;
        fs::create_dir_all(path.join("index"))?;

        Ok(Self {
            base_dir: path,
            index: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        })
    }

    /// Archive a workflow to cold storage.
    pub fn archive(&self, record: ColdStorageRecord) -> io::Result<()> {
        let file_name = format!("wf_{}_{}.bin", record.namespace_id, record.workflow_key);
        let file_path = self.base_dir.join("data").join(&file_name);

        // Serialize the record to binary
        let data = self.serialize_record(&record)?;

        // Write to file
        let mut file = fs::File::create(&file_path)?;
        file.write_all(&data)?;
        file.sync_all()?;

        // Update index
        let mut rec = record;
        rec.file_path = file_path.to_string_lossy().to_string();
        self.index.write().unwrap().insert(rec.workflow_key, rec);

        Ok(())
    }

    /// Retrieve an archived workflow from cold storage.
    pub fn retrieve(&self, workflow_key: u64) -> io::Result<Option<ColdStorageRecord>> {
        // Check in-memory index first
        if let Some(rec) = self.index.read().unwrap().get(&workflow_key) {
            return Ok(Some(rec.clone()));
        }

        // Try to load from file
        let file_name = format!("wf_*_{}.bin", workflow_key);
        let data_dir = self.base_dir.join("data");
        if let Ok(entries) = fs::read_dir(&data_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.contains(&format!("_{}.bin", workflow_key)) {
                    let data = fs::read(entry.path())?;
                    if let Some(record) = self.deserialize_record(&data)? {
                        return Ok(Some(record));
                    }
                }
            }
        }

        Ok(None)
    }

    /// List all archived workflow keys.
    pub fn list_keys(&self) -> Vec<u64> {
        self.index.read().unwrap().keys().cloned().collect()
    }

    /// List archived workflows by namespace.
    pub fn list_by_namespace(&self, namespace_id: u64) -> Vec<ColdStorageRecord> {
        self.index
            .read()
            .unwrap()
            .values()
            .filter(|r| r.namespace_id == namespace_id)
            .cloned()
            .collect()
    }

    /// Get the total number of archived workflows.
    pub fn count(&self) -> usize {
        self.index.read().unwrap().len()
    }

    /// Delete an archived workflow from cold storage.
    pub fn delete(&self, workflow_key: u64) -> io::Result<bool> {
        let record = self.index.write().unwrap().remove(&workflow_key);
        if let Some(rec) = record {
            let path = PathBuf::from(&rec.file_path);
            if path.exists() {
                fs::remove_file(path)?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Garbage collect archives older than the given retention period (in milliseconds).
    pub fn gc_older_than(&self, retention_ms: u64, now_ms: u64) -> io::Result<usize> {
        let cutoff = now_ms.saturating_sub(retention_ms);
        let keys_to_delete: Vec<u64> = self
            .index
            .read()
            .unwrap()
            .iter()
            .filter(|(_, r)| r.archived_at_ms <= cutoff)
            .map(|(k, _)| *k)
            .collect();

        let count = keys_to_delete.len();
        for key in keys_to_delete {
            self.delete(key)?;
        }
        Ok(count)
    }

    fn serialize_record(&self, record: &ColdStorageRecord) -> io::Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(256);
        // Magic bytes
        buf.extend_from_slice(b"VELC");
        // Version
        buf.push(1);
        // Workflow key
        buf.extend_from_slice(&record.workflow_key.to_le_bytes());
        // Workflow ID
        buf.extend_from_slice(&record.workflow_id.to_le_bytes());
        // Run ID
        buf.extend_from_slice(&record.run_id.to_le_bytes());
        // Workflow type ID
        buf.extend_from_slice(&record.workflow_type_id.to_le_bytes());
        // Namespace ID
        buf.extend_from_slice(&record.namespace_id.to_le_bytes());
        // Status
        buf.push(record.status as u8);
        // Archived at
        buf.extend_from_slice(&record.archived_at_ms.to_le_bytes());
        // Input data
        self.write_opt_bytes(&mut buf, &record.input_data);
        // Result data
        self.write_opt_bytes(&mut buf, &record.result_data);
        // Step count + step results
        let step_count = record.step_results.len() as u32;
        buf.extend_from_slice(&step_count.to_le_bytes());
        for (step, data) in &record.step_results {
            buf.extend_from_slice(&step.to_le_bytes());
            self.write_opt_bytes(&mut buf, &Some(data.clone()));
        }
        // Event count + events
        let event_count = record.event_history.len() as u32;
        buf.extend_from_slice(&event_count.to_le_bytes());
        for (event_type, payload) in &record.event_history {
            buf.extend_from_slice(&event_type.to_le_bytes());
            self.write_opt_bytes(&mut buf, &Some(payload.clone()));
        }
        Ok(buf)
    }

    fn deserialize_record(&self, data: &[u8]) -> io::Result<Option<ColdStorageRecord>> {
        if data.len() < 53 {
            return Ok(None);
        } // Minimum size
        if &data[0..4] != b"VELC" {
            return Ok(None);
        }

        let mut pos = 5; // Skip magic + version
        let workflow_key = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let workflow_id = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let run_id = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let workflow_type_id = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let namespace_id = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let status = match data[pos] {
            1 => WorkflowStatus::Running,
            2 => WorkflowStatus::Completed,
            3 => WorkflowStatus::Failed,
            4 => WorkflowStatus::Canceled,
            5 => WorkflowStatus::Terminated,
            6 => WorkflowStatus::ContinuedAsNew,
            7 => WorkflowStatus::TimedOut,
            _ => WorkflowStatus::Void,
        };
        pos += 1;
        let archived_at_ms = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        let (input_data, new_pos) = self.read_opt_bytes(data, pos);
        pos = new_pos;
        let (result_data, new_pos) = self.read_opt_bytes(data, pos);
        pos = new_pos;

        // Step results
        let mut step_results = HashMap::new();
        if pos + 4 <= data.len() {
            let step_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            for _ in 0..step_count {
                if pos + 4 > data.len() {
                    break;
                }
                let step = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                pos += 4;
                let (step_data, new_pos) = self.read_opt_bytes(data, pos);
                pos = new_pos;
                if let Some(d) = step_data {
                    step_results.insert(step, d);
                }
            }
        }

        // Event history
        let mut event_history = Vec::new();
        if pos + 4 <= data.len() {
            let event_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            for _ in 0..event_count {
                if pos + 8 > data.len() {
                    break;
                }
                let event_type = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let (payload, new_pos) = self.read_opt_bytes(data, pos);
                pos = new_pos;
                event_history.push((event_type, payload.unwrap_or_default()));
            }
        }

        Ok(Some(ColdStorageRecord {
            workflow_key,
            workflow_id,
            run_id,
            workflow_type_id,
            namespace_id,
            status,
            input_data,
            result_data,
            step_results,
            event_history,
            archived_at_ms,
            file_path: String::new(),
        }))
    }

    fn write_opt_bytes(&self, buf: &mut Vec<u8>, data: &Option<Vec<u8>>) {
        match data {
            Some(d) => {
                buf.extend_from_slice(&(d.len() as u32).to_le_bytes());
                buf.extend_from_slice(d);
            }
            None => {
                buf.extend_from_slice(&0u32.to_le_bytes());
            }
        }
    }

    fn read_opt_bytes(&self, data: &[u8], pos: usize) -> (Option<Vec<u8>>, usize) {
        if pos + 4 > data.len() {
            return (None, pos);
        }
        let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        let new_pos = pos + 4 + len;
        if len == 0 || new_pos > data.len() {
            (None, pos + 4)
        } else {
            (Some(data[pos + 4..new_pos].to_vec()), new_pos)
        }
    }
}

// ─── Cloud Storage Adapter Trait ────────────────────────────────────────────

/// Abstract interface for cloud cold storage backends (S3, GCS, Azure Blob).
/// Implementations handle upload, download, listing, and deletion of archived workflows.
pub trait CloudStorageAdapter: Send + Sync {
    /// Archive a workflow record to cloud storage.
    fn archive(&self, record: &ColdStorageRecord) -> io::Result<()>;
    /// Retrieve a workflow record from cloud storage.
    fn retrieve(&self, workflow_key: u64) -> io::Result<ColdStorageRecord>;
    /// Delete a workflow record from cloud storage.
    fn delete(&self, workflow_key: u64) -> io::Result<bool>;
    /// List records by namespace.
    fn list_by_namespace(&self, namespace_id: u64) -> io::Result<Vec<ColdStorageRecord>>;
    /// Garbage collect records older than the given retention period.
    fn gc_older_than(&self, retention_ms: u64, now_ms: u64) -> io::Result<usize>;
    /// Get the total count of archived records.
    fn count(&self) -> io::Result<usize>;
    /// Get the name of this backend (e.g., "s3", "gcs", "memory").
    fn backend_name(&self) -> &str;
}

/// In-memory mock S3 adapter for testing.
pub struct MockS3Adapter {
    records: RwLock<HashMap<u64, ColdStorageRecord>>,
    bucket: String,
    region: String,
}

impl MockS3Adapter {
    pub fn new(bucket: &str, region: &str) -> Self {
        Self {
            records: RwLock::new(HashMap::new()),
            bucket: bucket.to_string(),
            region: region.to_string(),
        }
    }
}

impl CloudStorageAdapter for MockS3Adapter {
    fn archive(&self, record: &ColdStorageRecord) -> io::Result<()> {
        self.records
            .write()
            .unwrap()
            .insert(record.workflow_key, record.clone());
        Ok(())
    }
    fn retrieve(&self, workflow_key: u64) -> io::Result<ColdStorageRecord> {
        self.records
            .read()
            .unwrap()
            .get(&workflow_key)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "workflow not found in S3"))
    }
    fn delete(&self, workflow_key: u64) -> io::Result<bool> {
        Ok(self
            .records
            .write()
            .unwrap()
            .remove(&workflow_key)
            .is_some())
    }
    fn list_by_namespace(&self, namespace_id: u64) -> io::Result<Vec<ColdStorageRecord>> {
        Ok(self
            .records
            .read()
            .unwrap()
            .values()
            .filter(|r| r.namespace_id == namespace_id)
            .cloned()
            .collect())
    }
    fn gc_older_than(&self, retention_ms: u64, now_ms: u64) -> io::Result<usize> {
        let cutoff = now_ms.saturating_sub(retention_ms);
        let mut records = self.records.write().unwrap();
        let before = records.len();
        records.retain(|_, r| r.archived_at_ms >= cutoff);
        Ok(before - records.len())
    }
    fn count(&self) -> io::Result<usize> {
        Ok(self.records.read().unwrap().len())
    }
    fn backend_name(&self) -> &str {
        "mock-s3"
    }
}

/// In-memory mock GCS adapter for testing.
pub struct MockGcsAdapter {
    records: RwLock<HashMap<u64, ColdStorageRecord>>,
    bucket: String,
}

impl MockGcsAdapter {
    pub fn new(bucket: &str) -> Self {
        Self {
            records: RwLock::new(HashMap::new()),
            bucket: bucket.to_string(),
        }
    }
}

impl CloudStorageAdapter for MockGcsAdapter {
    fn archive(&self, record: &ColdStorageRecord) -> io::Result<()> {
        self.records
            .write()
            .unwrap()
            .insert(record.workflow_key, record.clone());
        Ok(())
    }
    fn retrieve(&self, workflow_key: u64) -> io::Result<ColdStorageRecord> {
        self.records
            .read()
            .unwrap()
            .get(&workflow_key)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "workflow not found in GCS"))
    }
    fn delete(&self, workflow_key: u64) -> io::Result<bool> {
        Ok(self
            .records
            .write()
            .unwrap()
            .remove(&workflow_key)
            .is_some())
    }
    fn list_by_namespace(&self, namespace_id: u64) -> io::Result<Vec<ColdStorageRecord>> {
        Ok(self
            .records
            .read()
            .unwrap()
            .values()
            .filter(|r| r.namespace_id == namespace_id)
            .cloned()
            .collect())
    }
    fn gc_older_than(&self, retention_ms: u64, now_ms: u64) -> io::Result<usize> {
        let cutoff = now_ms.saturating_sub(retention_ms);
        let mut records = self.records.write().unwrap();
        let before = records.len();
        records.retain(|_, r| r.archived_at_ms >= cutoff);
        Ok(before - records.len())
    }
    fn count(&self) -> io::Result<usize> {
        Ok(self.records.read().unwrap().len())
    }
    fn backend_name(&self) -> &str {
        "mock-gcs"
    }
}

// ─── Binary Serialization for Cloud Records ─────────────────────────────────

/// Serialize a ColdStorageRecord to a binary format for cloud upload.
fn serialize_record_binary(record: &ColdStorageRecord) -> io::Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::with_capacity(256);
    buf.extend_from_slice(&record.workflow_key.to_le_bytes());
    buf.extend_from_slice(&record.workflow_id.to_le_bytes());
    buf.extend_from_slice(&record.run_id.to_le_bytes());
    buf.extend_from_slice(&record.workflow_type_id.to_le_bytes());
    buf.extend_from_slice(&record.namespace_id.to_le_bytes());
    buf.extend_from_slice(&(record.status as i32).to_le_bytes());
    // Optional input_data
    match &record.input_data {
        Some(d) => {
            buf.extend_from_slice(&(d.len() as u32).to_le_bytes());
            buf.extend_from_slice(d);
        }
        None => {
            buf.extend_from_slice(&0u32.to_le_bytes());
        }
    }
    // Optional result_data
    match &record.result_data {
        Some(d) => {
            buf.extend_from_slice(&(d.len() as u32).to_le_bytes());
            buf.extend_from_slice(d);
        }
        None => {
            buf.extend_from_slice(&0u32.to_le_bytes());
        }
    }
    // step_results
    buf.extend_from_slice(&(record.step_results.len() as u32).to_le_bytes());
    for (&step, data) in &record.step_results {
        buf.extend_from_slice(&step.to_le_bytes());
        buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
        buf.extend_from_slice(data);
    }
    // event_history
    buf.extend_from_slice(&(record.event_history.len() as u32).to_le_bytes());
    for (evt_type, payload) in &record.event_history {
        buf.extend_from_slice(&evt_type.to_le_bytes());
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(payload);
    }
    buf.extend_from_slice(&record.archived_at_ms.to_le_bytes());
    // file_path (metadata only)
    let fp = record.file_path.as_bytes();
    buf.extend_from_slice(&(fp.len() as u32).to_le_bytes());
    buf.extend_from_slice(fp);
    Ok(buf)
}

/// Deserialize a ColdStorageRecord from binary format downloaded from cloud.
fn deserialize_record_binary(data: &[u8]) -> io::Result<ColdStorageRecord> {
    struct Reader<'a> {
        data: &'a [u8],
        pos: usize,
    }
    impl<'a> Reader<'a> {
        fn read_u64(&mut self) -> io::Result<u64> {
            if self.pos + 8 > self.data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated u64",
                ));
            }
            let v = u64::from_le_bytes(self.data[self.pos..self.pos + 8].try_into().unwrap());
            self.pos += 8;
            Ok(v)
        }
        fn read_u32(&mut self) -> io::Result<u32> {
            if self.pos + 4 > self.data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated u32",
                ));
            }
            let v = u32::from_le_bytes(self.data[self.pos..self.pos + 4].try_into().unwrap());
            self.pos += 4;
            Ok(v)
        }
        fn read_bytes(&mut self, len: usize) -> io::Result<Vec<u8>> {
            if self.pos + len > self.data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated bytes",
                ));
            }
            let v = self.data[self.pos..self.pos + len].to_vec();
            self.pos += len;
            Ok(v)
        }
    }
    let mut r = Reader { data, pos: 0 };

    let workflow_key = r.read_u64()?;
    let workflow_id = r.read_u64()?;
    let run_id = r.read_u64()?;
    let workflow_type_id = r.read_u64()?;
    let namespace_id = r.read_u64()?;
    let status_i32 = r.read_u32()? as i32;
    let status = match status_i32 {
        0 => WorkflowStatus::Running,
        1 => WorkflowStatus::Completed,
        2 => WorkflowStatus::Failed,
        3 => WorkflowStatus::Canceled,
        4 => WorkflowStatus::Terminated,
        5 => WorkflowStatus::ContinuedAsNew,
        _ => WorkflowStatus::Running,
    };
    let input_len = r.read_u32()? as usize;
    let input_data = if input_len > 0 {
        Some(r.read_bytes(input_len)?)
    } else {
        None
    };
    let result_len = r.read_u32()? as usize;
    let result_data = if result_len > 0 {
        Some(r.read_bytes(result_len)?)
    } else {
        None
    };
    let step_count = r.read_u32()? as usize;
    let mut step_results = HashMap::new();
    for _ in 0..step_count {
        let step = r.read_u32()?;
        let len = r.read_u32()? as usize;
        let d = r.read_bytes(len)?;
        step_results.insert(step, d);
    }
    let evt_count = r.read_u32()? as usize;
    let mut event_history = Vec::with_capacity(evt_count);
    for _ in 0..evt_count {
        let evt_type = r.read_u64()?;
        let len = r.read_u32()? as usize;
        let payload = r.read_bytes(len)?;
        event_history.push((evt_type, payload));
    }
    let archived_at_ms = r.read_u64()?;
    let fp_len = r.read_u32()? as usize;
    let file_path = String::from_utf8(r.read_bytes(fp_len)?).unwrap_or_default();

    Ok(ColdStorageRecord {
        workflow_key,
        workflow_id,
        run_id,
        workflow_type_id,
        namespace_id,
        status,
        input_data,
        result_data,
        step_results,
        event_history,
        archived_at_ms,
        file_path,
    })
}

// ─── Real AWS S3 Adapter (feature-gated) ────────────────────────────────────

#[cfg(feature = "cloud-s3")]
pub struct S3Adapter {
    client: reqwest::blocking::Client,
    bucket: String,
    region: String,
    access_key: String,
    secret_key: String,
    endpoint: String, // e.g., "https://s3.us-east-1.amazonaws.com"
}

#[cfg(feature = "cloud-s3")]
impl S3Adapter {
    pub fn new(bucket: &str, region: &str, access_key: &str, secret_key: &str) -> Self {
        let endpoint = format!("https://s3.{}.amazonaws.com", region);
        Self {
            client: reqwest::blocking::Client::new(),
            bucket: bucket.to_string(),
            region: region.to_string(),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            endpoint,
        }
    }

    /// Create with a custom endpoint (for S3-compatible stores like MinIO).
    pub fn with_endpoint(
        bucket: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
        endpoint: &str,
    ) -> Self {
        Self {
            client: reqwest::blocking::Client::new(),
            bucket: bucket.to_string(),
            region: region.to_string(),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            endpoint: endpoint.to_string(),
        }
    }

    fn object_key(workflow_key: u64) -> String {
        format!("workflows/{}/record.bin", workflow_key)
    }

    fn namespace_prefix(namespace_id: u64) -> String {
        format!("workflows/")
    }

    /// Sign a request using AWS Signature Version 4.
    fn sign_request(
        &self,
        method: &str,
        path: &str,
        headers: &[(String, String)],
        payload_hash: &str,
    ) -> Vec<(String, String)> {
        use hmac::{Hmac, Mac};
        use sha2::{Digest, Sha256};
        use std::time::SystemTime;

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        let date_stamp = format!("{}", now.as_secs() / 86400 * 86400);
        // Simplified: use YYYYMMDD format
        let dt: chrono_like::DateTime = chrono_like::DateTime::from_timestamp(now.as_secs());
        let amz_date = dt.to_amz_date();
        let date_short = dt.to_date_short();

        let service = "s3";
        let credential_scope = format!("{}/{}/{}/aws4_request", date_short, self.region, service);

        // Canonical headers (always include host, x-amz-date, x-amz-content-sha256)
        let host = self.endpoint.replace("https://", "").replace("http://", "");
        let mut canon_headers = vec![
            (format!("host"), host.clone()),
            (format!("x-amz-content-sha256"), payload_hash.to_string()),
            (format!("x-amz-date"), amz_date.clone()),
        ];
        for (k, v) in headers {
            canon_headers.push((k.to_lowercase(), v.clone()));
        }
        canon_headers.sort_by(|a, b| a.0.cmp(&b.0));
        canon_headers.dedup_by(|a, b| a.0 == b.0);

        let signed_headers: Vec<&str> = canon_headers.iter().map(|(k, _)| k.as_str()).collect();
        let signed_headers_str = signed_headers.join(";");
        let canonical_headers: String = canon_headers
            .iter()
            .map(|(k, v)| format!("{}:{}\n", k, v))
            .collect();

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method,
            path,
            "", /* query string */
            canonical_headers,
            signed_headers_str,
            payload_hash
        );

        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            amz_date,
            credential_scope,
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );

        // Signing key
        type HmacSha256 = Hmac<Sha256>;
        let k_date = {
            let mut mac =
                HmacSha256::new_from_slice(format!("AWS4{}", self.secret_key).as_bytes()).unwrap();
            mac.update(date_short.as_bytes());
            mac.finalize().into_bytes()
        };
        let k_region = {
            let mut mac = HmacSha256::new_from_slice(&k_date).unwrap();
            mac.update(self.region.as_bytes());
            mac.finalize().into_bytes()
        };
        let k_service = {
            let mut mac = HmacSha256::new_from_slice(&k_region).unwrap();
            mac.update(service.as_bytes());
            mac.finalize().into_bytes()
        };
        let signing_key = {
            let mut mac = HmacSha256::new_from_slice(&k_service).unwrap();
            mac.update(b"aws4_request");
            mac.finalize().into_bytes()
        };

        let signature = hex::encode({
            let mut mac = HmacSha256::new_from_slice(&signing_key).unwrap();
            mac.update(string_to_sign.as_bytes());
            mac.finalize().into_bytes()
        });

        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.access_key, credential_scope, signed_headers_str, signature
        );

        let mut auth_headers = vec![
            ("x-amz-date".to_string(), amz_date),
            ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
            ("Authorization".to_string(), authorization),
        ];
        auth_headers
    }

    fn full_url(&self, path: &str) -> String {
        format!("{}/{}{}", self.endpoint, self.bucket, path)
    }
}

#[cfg(feature = "cloud-s3")]
impl CloudStorageAdapter for S3Adapter {
    fn archive(&self, record: &ColdStorageRecord) -> io::Result<()> {
        let key = Self::object_key(record.workflow_key);
        let data = serialize_record_binary(record)?;
        let payload_hash = hex::encode(sha2::Sha256::digest(&data));
        let path = format!("/{}", key);
        let auth_headers = self.sign_request("PUT", &path, &[], &payload_hash);

        let mut req = self
            .client
            .put(self.full_url(&path))
            .header("Content-Type", "application/octet-stream")
            .body(data);
        for (k, v) in &auth_headers {
            req = req.header(k.as_str(), v.as_str());
        }
        let resp = req
            .send()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!("S3 PUT failed: {}", resp.status()),
            ))
        }
    }

    fn retrieve(&self, workflow_key: u64) -> io::Result<ColdStorageRecord> {
        let key = Self::object_key(workflow_key);
        let path = format!("/{}", key);
        let payload_hash = hex::encode(sha2::Sha256::digest(b""));
        let auth_headers = self.sign_request("GET", &path, &[], &payload_hash);

        let mut req = self.client.get(self.full_url(&path));
        for (k, v) in &auth_headers {
            req = req.header(k.as_str(), v.as_str());
        }
        let resp = req
            .send()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "workflow not found in S3",
            ));
        }
        if !resp.status().is_success() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("S3 GET failed: {}", resp.status()),
            ));
        }
        let bytes = resp
            .bytes()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        deserialize_record_binary(&bytes)
    }

    fn delete(&self, workflow_key: u64) -> io::Result<bool> {
        let key = Self::object_key(workflow_key);
        let path = format!("/{}", key);
        let payload_hash = hex::encode(sha2::Sha256::digest(b""));
        let auth_headers = self.sign_request("DELETE", &path, &[], &payload_hash);

        let mut req = self.client.delete(self.full_url(&path));
        for (k, v) in &auth_headers {
            req = req.header(k.as_str(), v.as_str());
        }
        let resp = req
            .send()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(resp.status().is_success() || resp.status() == reqwest::StatusCode::NO_CONTENT)
    }

    fn list_by_namespace(&self, namespace_id: u64) -> io::Result<Vec<ColdStorageRecord>> {
        // S3 LIST with prefix to find objects, then retrieve each
        // For simplicity, list all objects under the workflows/ prefix
        let path = format!("/?list-type=2&prefix=workflows/");
        let payload_hash = hex::encode(sha2::Sha256::digest(b""));
        let auth_headers = self.sign_request("GET", &path, &[], &payload_hash);

        let mut req = self.client.get(self.full_url(&path));
        for (k, v) in &auth_headers {
            req = req.header(k.as_str(), v.as_str());
        }
        let resp = req
            .send()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        if !resp.status().is_success() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("S3 LIST failed: {}", resp.status()),
            ));
        }
        let body = resp
            .text()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // Parse XML response for <Key> elements
        let mut records = Vec::new();
        let mut search_from = 0;
        while let Some(start) = body[search_from..].find("<Key>") {
            let key_start = search_from + start + 5;
            if let Some(end) = body[key_start..].find("</Key>") {
                let key = &body[key_start..key_start + end];
                // Extract workflow_key from the key path: workflows/{key}/record.bin
                if let Some(wk_str) = key
                    .strip_prefix("workflows/")
                    .and_then(|s| s.strip_suffix("/record.bin"))
                {
                    if let Ok(wk) = wk_str.parse::<u64>() {
                        if let Ok(record) = self.retrieve(wk) {
                            if record.namespace_id == namespace_id {
                                records.push(record);
                            }
                        }
                    }
                }
                search_from = key_start + end + 6;
            } else {
                break;
            }
        }
        Ok(records)
    }

    fn gc_older_than(&self, retention_ms: u64, now_ms: u64) -> io::Result<usize> {
        let cutoff = now_ms.saturating_sub(retention_ms);
        // List all objects, check their Last-Modified, delete old ones
        let path = format!("/?list-type=2&prefix=workflows/");
        let payload_hash = hex::encode(sha2::Sha256::digest(b""));
        let auth_headers = self.sign_request("GET", &path, &[], &payload_hash);

        let mut req = self.client.get(self.full_url(&path));
        for (k, v) in &auth_headers {
            req = req.header(k.as_str(), v.as_str());
        }
        let resp = req
            .send()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        if !resp.status().is_success() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("S3 LIST failed: {}", resp.status()),
            ));
        }
        let body = resp
            .text()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let mut deleted = 0;
        let mut search_from = 0;
        while let Some(start) = body[search_from..].find("<Key>") {
            let key_start = search_from + start + 5;
            if let Some(end) = body[key_start..].find("</Key>") {
                let key = &body[key_start..key_start + end];
                if let Some(wk_str) = key
                    .strip_prefix("workflows/")
                    .and_then(|s| s.strip_suffix("/record.bin"))
                {
                    if let Ok(wk) = wk_str.parse::<u64>() {
                        // Try to retrieve and check archived_at_ms
                        if let Ok(record) = self.retrieve(wk) {
                            if record.archived_at_ms < cutoff {
                                if self.delete(wk).unwrap_or(false) {
                                    deleted += 1;
                                }
                            }
                        }
                    }
                }
                search_from = key_start + end + 6;
            } else {
                break;
            }
        }
        Ok(deleted)
    }

    fn count(&self) -> io::Result<usize> {
        let path = format!("/?list-type=2&prefix=workflows/&max-keys=1000");
        let payload_hash = hex::encode(sha2::Sha256::digest(b""));
        let auth_headers = self.sign_request("GET", &path, &[], &payload_hash);

        let mut req = self.client.get(self.full_url(&path));
        for (k, v) in &auth_headers {
            req = req.header(k.as_str(), v.as_str());
        }
        let resp = req
            .send()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        if !resp.status().is_success() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("S3 LIST failed: {}", resp.status()),
            ));
        }
        let body = resp
            .text()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // Count <Key> occurrences
        let count = body.matches("<Key>").count();
        Ok(count)
    }

    fn backend_name(&self) -> &str {
        "s3"
    }
}

// ─── Real Google Cloud Storage Adapter (feature-gated) ──────────────────────

#[cfg(feature = "cloud-gcs")]
pub struct GcsAdapter {
    client: reqwest::blocking::Client,
    bucket: String,
    token: String, // OAuth2 bearer token or service account JWT
}

#[cfg(feature = "cloud-gcs")]
impl GcsAdapter {
    pub fn new(bucket: &str, token: &str) -> Self {
        Self {
            client: reqwest::blocking::Client::new(),
            bucket: bucket.to_string(),
            token: token.to_string(),
        }
    }

    fn object_name(workflow_key: u64) -> String {
        format!("workflows/{}/record.bin", workflow_key)
    }

    fn api_base(&self) -> String {
        format!(
            "https://storage.googleapis.com/storage/v1/b/{}/o",
            self.bucket
        )
    }

    fn upload_base(&self) -> String {
        format!(
            "https://storage.googleapis.com/upload/storage/v1/b/{}/o",
            self.bucket
        )
    }
}

#[cfg(feature = "cloud-gcs")]
impl CloudStorageAdapter for GcsAdapter {
    fn archive(&self, record: &ColdStorageRecord) -> io::Result<()> {
        let name = Self::object_name(record.workflow_key);
        let data = serialize_record_binary(record)?;
        let url = format!("{}?uploadType=media&name={}", self.upload_base(), name);
        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Content-Type", "application/octet-stream")
            .body(data)
            .send()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!("GCS upload failed: {}", resp.status()),
            ))
        }
    }

    fn retrieve(&self, workflow_key: u64) -> io::Result<ColdStorageRecord> {
        let name = Self::object_name(workflow_key);
        let url = format!("{}?alt=media", self.api_base())
            .replace("/o", &format!("/o/{}", urlencoding(&name)));
        let resp = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "workflow not found in GCS",
            ));
        }
        if !resp.status().is_success() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("GCS GET failed: {}", resp.status()),
            ));
        }
        let bytes = resp
            .bytes()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        deserialize_record_binary(&bytes)
    }

    fn delete(&self, workflow_key: u64) -> io::Result<bool> {
        let name = Self::object_name(workflow_key);
        let url = format!("{}/{}", self.api_base(), urlencoding(&name));
        let resp = self
            .client
            .delete(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(resp.status().is_success() || resp.status() == reqwest::StatusCode::NO_CONTENT)
    }

    fn list_by_namespace(&self, namespace_id: u64) -> io::Result<Vec<ColdStorageRecord>> {
        let url = format!("{}?prefix=workflows/", self.api_base());
        let resp = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        if !resp.status().is_success() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("GCS LIST failed: {}", resp.status()),
            ));
        }
        let body = resp
            .text()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // Parse JSON response for object names
        let mut records = Vec::new();
        let mut search_from = 0;
        while let Some(start) = body[search_from..].find("\"name\":\"") {
            let name_start = search_from + start + 8;
            if let Some(end) = body[name_start..].find('"') {
                let name = &body[name_start..name_start + end];
                if let Some(wk_str) = name
                    .strip_prefix("workflows/")
                    .and_then(|s| s.strip_suffix("/record.bin"))
                {
                    if let Ok(wk) = wk_str.parse::<u64>() {
                        if let Ok(record) = self.retrieve(wk) {
                            if record.namespace_id == namespace_id {
                                records.push(record);
                            }
                        }
                    }
                }
                search_from = name_start + end + 1;
            } else {
                break;
            }
        }
        Ok(records)
    }

    fn gc_older_than(&self, retention_ms: u64, now_ms: u64) -> io::Result<usize> {
        let cutoff = now_ms.saturating_sub(retention_ms);
        // List all, retrieve each, delete if older than cutoff
        let url = format!("{}?prefix=workflows/", self.api_base());
        let resp = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        if !resp.status().is_success() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("GCS LIST failed: {}", resp.status()),
            ));
        }
        let body = resp
            .text()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let mut deleted = 0;
        let mut search_from = 0;
        while let Some(start) = body[search_from..].find("\"name\":\"") {
            let name_start = search_from + start + 8;
            if let Some(end) = body[name_start..].find('"') {
                let name = &body[name_start..name_start + end];
                if let Some(wk_str) = name
                    .strip_prefix("workflows/")
                    .and_then(|s| s.strip_suffix("/record.bin"))
                {
                    if let Ok(wk) = wk_str.parse::<u64>() {
                        if let Ok(record) = self.retrieve(wk) {
                            if record.archived_at_ms < cutoff {
                                if self.delete(wk).unwrap_or(false) {
                                    deleted += 1;
                                }
                            }
                        }
                    }
                }
                search_from = name_start + end + 1;
            } else {
                break;
            }
        }
        Ok(deleted)
    }

    fn count(&self) -> io::Result<usize> {
        let url = format!("{}?prefix=workflows/", self.api_base());
        let resp = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        if !resp.status().is_success() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("GCS LIST failed: {}", resp.status()),
            ));
        }
        let body = resp
            .text()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let count = body.matches("\"name\":").count();
        Ok(count)
    }

    fn backend_name(&self) -> &str {
        "gcs"
    }
}

/// Simple URL encoding for GCS object names (encode '/' as '%2F').
#[cfg(feature = "cloud-gcs")]
fn urlencoding(s: &str) -> String {
    s.replace('%', "%25").replace('/', "%2F")
}

/// Minimal date/time helper for AWS SigV4 (avoids chrono dependency).
#[cfg(feature = "cloud-s3")]
mod chrono_like {
    pub struct DateTime {
        secs: u64,
    }
    impl DateTime {
        pub fn from_timestamp(secs: u64) -> Self {
            Self { secs }
        }
        pub fn to_amz_date(&self) -> String {
            // Format: YYYYMMDDTHHMMSSZ
            let (y, m, d, hh, mm, ss) = self.to_components();
            format!("{:04}{:02}{:02}T{:02}{:02}{:02}Z", y, m, d, hh, mm, ss)
        }
        pub fn to_date_short(&self) -> String {
            let (y, m, d, _, _, _) = self.to_components();
            format!("{:04}{:02}{:02}", y, m, d)
        }
        fn to_components(&self) -> (u32, u32, u32, u32, u32, u32) {
            let secs = self.secs;
            let ss = (secs % 60) as u32;
            let mm = ((secs / 60) % 60) as u32;
            let hh = ((secs / 3600) % 24) as u32;
            // Days since epoch
            let mut days = (secs / 86400) as u64;
            // Year
            let mut y = 1970u32;
            loop {
                let days_in_year = if is_leap(y) { 366 } else { 365 };
                if days < days_in_year as u64 {
                    break;
                }
                days -= days_in_year as u64;
                y += 1;
            }
            // Month
            let leap = is_leap(y);
            let month_days = [
                31,
                if leap { 29 } else { 28 },
                31,
                30,
                31,
                30,
                31,
                31,
                30,
                31,
                30,
                31,
            ];
            let mut m = 0u32;
            for (i, &md) in month_days.iter().enumerate() {
                if days < md as u64 {
                    m = i as u32 + 1;
                    break;
                }
                days -= md as u64;
            }
            if m == 0 {
                m = 12;
            }
            let d = days as u32 + 1;
            (y, m, d, hh, mm, ss)
        }
    }
    fn is_leap(y: u32) -> bool {
        (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(suffix: &str) -> String {
        let dir =
            std::env::temp_dir().join(format!("vel_cold_test_{}_{}", std::process::id(), suffix));
        let dir_str = dir.to_string_lossy().to_string();
        let _ = fs::remove_dir_all(&dir_str);
        dir_str
    }

    #[test]
    fn test_archive_and_retrieve() {
        let dir = temp_dir("archive");
        let storage = FileColdStorage::new(&dir).unwrap();

        let record = ColdStorageRecord {
            workflow_key: 42,
            workflow_id: 1001,
            run_id: 2001,
            workflow_type_id: 10,
            namespace_id: 0,
            status: WorkflowStatus::Completed,
            input_data: Some(vec![1, 2, 3]),
            result_data: Some(vec![4, 5, 6]),
            step_results: HashMap::from([(0, vec![7, 8])]),
            event_history: vec![(1, vec![9])],
            archived_at_ms: 1000,
            file_path: String::new(),
        };

        storage.archive(record).unwrap();
        assert_eq!(storage.count(), 1);

        let retrieved = storage.retrieve(42).unwrap().unwrap();
        assert_eq!(retrieved.workflow_key, 42);
        assert_eq!(retrieved.workflow_id, 1001);
        assert_eq!(retrieved.status, WorkflowStatus::Completed);
        assert_eq!(retrieved.input_data, Some(vec![1, 2, 3]));
        assert_eq!(retrieved.result_data, Some(vec![4, 5, 6]));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_delete() {
        let dir = temp_dir("delete");
        let storage = FileColdStorage::new(&dir).unwrap();

        let record = ColdStorageRecord {
            workflow_key: 99,
            workflow_id: 1,
            run_id: 2,
            workflow_type_id: 1,
            namespace_id: 0,
            status: WorkflowStatus::Completed,
            input_data: None,
            result_data: None,
            step_results: HashMap::new(),
            event_history: vec![],
            archived_at_ms: 1000,
            file_path: String::new(),
        };

        storage.archive(record).unwrap();
        assert_eq!(storage.count(), 1);
        assert!(storage.delete(99).unwrap());
        assert_eq!(storage.count(), 0);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_gc_older_than() {
        let dir = temp_dir("gc");
        let storage = FileColdStorage::new(&dir).unwrap();

        for i in 0..5 {
            let record = ColdStorageRecord {
                workflow_key: i,
                workflow_id: i,
                run_id: i + 100,
                workflow_type_id: 1,
                namespace_id: 0,
                status: WorkflowStatus::Completed,
                input_data: None,
                result_data: None,
                step_results: HashMap::new(),
                event_history: vec![],
                archived_at_ms: i * 1000,
                file_path: String::new(),
            };
            storage.archive(record).unwrap();
        }

        assert_eq!(storage.count(), 5);
        let deleted = storage.gc_older_than(3000, 5000).unwrap();
        assert_eq!(deleted, 3); // 0ms, 1000ms, 2000ms are older than 5000-3000=2000
        assert_eq!(storage.count(), 2);

        let _ = fs::remove_dir_all(&dir);
    }
}
