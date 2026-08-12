//! File-based Write-Ahead Log (WAL) for durable workflow state persistence.
//! Every state-changing event is appended to the log before being applied to memory.
//! On recovery, the log is replayed to reconstruct the in-memory state.
//!
//! File header: [magic: 4 bytes "VELO"][version: u32 LE] = 8 bytes
//! Record format: [event_type: u8][workflow_key: u64][data_len: u32][data: bytes][crc32: u32]
//! Total record header: 1 + 8 + 4 = 13 bytes + data + 4 bytes CRC

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// WAL file magic bytes — identifies a valid Velocity WAL file.
pub const WAL_MAGIC: [u8; 4] = *b"VELO";

/// Current WAL format version. Increment on breaking format changes.
pub const WAL_VERSION: u32 = 1;

/// Maximum supported WAL version for forward-compatibility checks.
pub const WAL_VERSION_MAX: u32 = 1;

// ─── WAL Event Types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalEventType {
    WorkflowStarted = 1,
    StepCompleted = 2,
    WorkflowCompleted = 3,
    WorkflowFailed = 4,
    WorkflowCanceled = 5,
    WorkflowTerminated = 6,
    SignalReceived = 7,
    TimerScheduled = 8,
    ActivityScheduled = 9,
    ChildWorkflowStarted = 10,
}

impl WalEventType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::WorkflowStarted),
            2 => Some(Self::StepCompleted),
            3 => Some(Self::WorkflowCompleted),
            4 => Some(Self::WorkflowFailed),
            5 => Some(Self::WorkflowCanceled),
            6 => Some(Self::WorkflowTerminated),
            7 => Some(Self::SignalReceived),
            8 => Some(Self::TimerScheduled),
            9 => Some(Self::ActivityScheduled),
            10 => Some(Self::ChildWorkflowStarted),
            _ => None,
        }
    }
}

// ─── WAL Record ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct WalRecord {
    pub event_type: WalEventType,
    pub workflow_key: u64,
    pub data: Vec<u8>,
}

impl WalRecord {
    pub fn new(event_type: WalEventType, workflow_key: u64, data: Vec<u8>) -> Self {
        Self {
            event_type,
            workflow_key,
            data,
        }
    }

    /// Encode to bytes: [event_type: u8][workflow_key: u64][data_len: u32][data: bytes][crc32: u32]
    pub fn encode(&self) -> Vec<u8> {
        let data_len = self.data.len() as u32;
        let total = 1 + 8 + 4 + self.data.len() + 4;
        let mut buf = Vec::with_capacity(total);

        buf.push(self.event_type as u8);
        buf.extend_from_slice(&self.workflow_key.to_le_bytes());
        buf.extend_from_slice(&data_len.to_le_bytes());
        buf.extend_from_slice(&self.data);

        let crc = crc32_fast(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        buf
    }

    /// Decode from a reader. Returns None if EOF.
    pub fn decode<R: Read>(reader: &mut R) -> io::Result<Option<Self>> {
        let mut event_byte = [0u8; 1];
        match reader.read_exact(&mut event_byte) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        let event_type = WalEventType::from_u8(event_byte[0])
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid event type"))?;

        let mut key_buf = [0u8; 8];
        reader.read_exact(&mut key_buf)?;
        let workflow_key = u64::from_le_bytes(key_buf);

        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let data_len = u32::from_le_bytes(len_buf) as usize;

        let mut data = vec![0u8; data_len];
        reader.read_exact(&mut data)?;

        let mut crc_buf = [0u8; 4];
        reader.read_exact(&mut crc_buf)?;

        // Verify CRC: compute over everything before the CRC
        let mut check_buf = Vec::with_capacity(1 + 8 + 4 + data_len);
        check_buf.push(event_byte[0]);
        check_buf.extend_from_slice(&workflow_key.to_le_bytes());
        check_buf.extend_from_slice(&(data_len as u32).to_le_bytes());
        check_buf.extend_from_slice(&data);

        let expected_crc = crc32_fast(&check_buf);
        let actual_crc = u32::from_le_bytes(crc_buf);
        if expected_crc != actual_crc {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "CRC mismatch"));
        }

        Ok(Some(Self {
            event_type,
            workflow_key,
            data,
        }))
    }
}

// ─── WAL Writer ───────────────────────────────────────────────────────────────

/// Append-only WAL writer. Each `append` writes to the OS buffer.
/// Use `sync()` for explicit durability (group commit pattern).
pub struct WalWriter {
    writer: BufWriter<File>,
    path: PathBuf,
    record_count: u64,
    unsynced_count: u64,
}

impl WalWriter {
    /// Open or create a WAL file at the given path.
    /// New files get a versioned header; existing files are validated.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let is_new = !path.exists() || fs::metadata(&path)?.len() == 0;

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let mut writer = BufWriter::new(file);

        if is_new {
            // Write versioned header: [magic: 4][version: u32]
            writer.write_all(&WAL_MAGIC)?;
            writer.write_all(&WAL_VERSION.to_le_bytes())?;
            writer.flush()?;
        }

        Ok(Self {
            writer,
            path,
            record_count: 0,
            unsynced_count: 0,
        })
    }

    /// Append a record to the WAL. Data is flushed to the OS buffer but NOT fsynced.
    /// Call `sync()` for group-commit durability (amortizes fsync across many records).
    pub fn append(&mut self, record: &WalRecord) -> io::Result<()> {
        let encoded = record.encode();
        self.writer.write_all(&encoded)?;
        self.writer.flush()?; // push to OS kernel buffer — no fsync
        self.record_count += 1;
        self.unsynced_count += 1;
        Ok(())
    }

    /// Fsync the WAL file — ensures all previously appended records are durable.
    /// Call this after a batch of appends for group-commit semantics.
    pub fn sync(&mut self) -> io::Result<()> {
        if self.unsynced_count > 0 {
            self.writer.get_ref().sync_all()?;
            self.unsynced_count = 0;
        }
        Ok(())
    }

    /// Append a record with a convenience builder.
    pub fn append_event(
        &mut self,
        event_type: WalEventType,
        workflow_key: u64,
        data: Vec<u8>,
    ) -> io::Result<()> {
        let record = WalRecord::new(event_type, workflow_key, data);
        self.append(&record)
    }

    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

// ─── WAL READER ───────────────────────────────────────────────────────────────

/// Read all records from a WAL file (for recovery/replay).
/// Validates the versioned header before reading records.
pub fn read_wal_records(path: impl AsRef<Path>) -> io::Result<Vec<WalRecord>> {
    let file = File::open(path.as_ref())?;
    let mut reader = BufReader::new(file);

    // Validate header: [magic: 4][version: u32]
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic)?;
    if magic != WAL_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Not a Velocity WAL file (bad magic: {:?})", magic),
        ));
    }
    let mut ver_bytes = [0u8; 4];
    reader.read_exact(&mut ver_bytes)?;
    let version = u32::from_le_bytes(ver_bytes);
    if version > WAL_VERSION_MAX {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "WAL version {} is newer than maximum supported version {} — upgrade the engine",
                version, WAL_VERSION_MAX
            ),
        ));
    }

    let mut records = Vec::new();
    while let Some(record) = WalRecord::decode(&mut reader)? {
        records.push(record);
    }

    Ok(records)
}

// ─── WAL MANAGER ──────────────────────────────────────────────────────────────

/// Thread-safe WAL manager. Wraps the writer in a Mutex for concurrent access.
/// Supports log rotation when the file exceeds a size threshold.
pub struct WalManager {
    writer: Arc<Mutex<WalWriter>>,
    path: PathBuf,
    max_file_size: u64,
}

impl WalManager {
    pub fn new(path: impl AsRef<Path>, max_file_size: u64) -> io::Result<Self> {
        let writer = WalWriter::open(path.as_ref())?;
        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            path: path.as_ref().to_path_buf(),
            max_file_size,
        })
    }

    /// Append an event to the WAL (no fsync — data is in OS buffer).
    pub fn append(
        &self,
        event_type: WalEventType,
        workflow_key: u64,
        data: Vec<u8>,
    ) -> io::Result<()> {
        let mut writer = self.writer.lock().unwrap();
        writer.append_event(event_type, workflow_key, data)?;

        // Check if rotation is needed
        if let Ok(metadata) = writer.writer.get_ref().metadata() {
            if metadata.len() > self.max_file_size {
                drop(writer);
                self.rotate()?;
            }
        }

        Ok(())
    }

    /// Fsync the WAL — group commit: ensures all pending records are durable.
    /// Call after a batch of `append()` calls for amortized fsync.
    pub fn sync(&self) -> io::Result<()> {
        self.writer.lock().unwrap().sync()
    }

    /// Rotate the WAL file: rename current to .old, open fresh file.
    fn rotate(&self) -> io::Result<()> {
        let old_path = self.path.with_extension("wal.old");
        if old_path.exists() {
            fs::remove_file(&old_path)?;
        }
        fs::rename(&self.path, &old_path)?;

        let mut writer = self.writer.lock().unwrap();
        *writer = WalWriter::open(&self.path)?;
        Ok(())
    }

    /// Replay all records from the current WAL file.
    pub fn replay(&self) -> io::Result<Vec<WalRecord>> {
        read_wal_records(&self.path)
    }

    /// Replay records from the old (rotated) WAL file if it exists.
    pub fn replay_old(&self) -> io::Result<Vec<WalRecord>> {
        let old_path = self.path.with_extension("wal.old");
        if old_path.exists() {
            read_wal_records(&old_path)
        } else {
            Ok(Vec::new())
        }
    }

    /// Full replay: old WAL + current WAL (in order).
    pub fn replay_all(&self) -> io::Result<Vec<WalRecord>> {
        let mut records = self.replay_old()?;
        records.extend(self.replay()?);
        Ok(records)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Create a timestamped snapshot (copy) of the current WAL file.
    /// Returns the path to the snapshot file.
    ///
    /// The snapshot is fsynced to ensure it's durable.
    pub fn snapshot(&self, snapshot_dir: impl AsRef<Path>) -> io::Result<PathBuf> {
        let dir = snapshot_dir.as_ref();
        fs::create_dir_all(dir)?;

        // Sync current WAL before snapshotting
        self.sync()?;

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let snapshot_name = format!("wal_snapshot_{}.wal", timestamp);
        let snapshot_path = dir.join(&snapshot_name);

        fs::copy(&self.path, &snapshot_path)?;

        // Fsync the snapshot to ensure durability
        let snap_file = std::fs::OpenOptions::new().read(true).open(&snapshot_path)?;
        snap_file.sync_all()?;

        Ok(snapshot_path)
    }

    /// List all available snapshot files in a directory, sorted newest first.
    pub fn list_snapshots(snapshot_dir: impl AsRef<Path>) -> io::Result<Vec<PathBuf>> {
        let dir = snapshot_dir.as_ref();
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut snapshots: Vec<PathBuf> = fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.file_name().is_some_and(|n| n.to_string_lossy().starts_with("wal_snapshot_")))
            .collect();
        snapshots.sort_by(|a, b| b.cmp(a)); // newest first
        Ok(snapshots)
    }
}

// ─── CRC32 (simple, dependency-free) ──────────────────────────────────────────

/// Fast CRC32 using a precomputed lookup table (IEEE polynomial).
fn crc32_fast(data: &[u8]) -> u32 {
    let table = make_crc32_table();
    let mut crc: u32 = 0xFFFFFFFF;
    for &byte in data {
        let index = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = table[index] ^ (crc >> 8);
    }
    crc ^ 0xFFFFFFFF
}

fn make_crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    for i in 0..256 {
        let mut crc = i as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = 0xEDB88320 ^ (crc >> 1);
            } else {
                crc >>= 1;
            }
        }
        table[i] = crc;
    }
    table
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    fn temp_wal_path(name: &str) -> PathBuf {
        let dir = env::temp_dir().join("velocity_wal_tests");
        fs::create_dir_all(&dir).unwrap();
        dir.join(format!("{}.wal", name))
    }

    fn cleanup(path: &Path) {
        let _ = fs::remove_file(path);
        let old = path.with_extension("wal.old");
        let _ = fs::remove_file(old);
    }

    #[test]
    fn test_wal_write_and_read() {
        let path = temp_wal_path("write_read");
        cleanup(&path);

        {
            let mut writer = WalWriter::open(&path).unwrap();
            writer
                .append_event(WalEventType::WorkflowStarted, 1001, vec![1, 2, 3])
                .unwrap();
            writer
                .append_event(WalEventType::StepCompleted, 1001, vec![4, 5])
                .unwrap();
            writer
                .append_event(WalEventType::WorkflowCompleted, 1001, vec![42])
                .unwrap();
            assert_eq!(writer.record_count(), 3);
        }

        let records = read_wal_records(&path).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].event_type, WalEventType::WorkflowStarted);
        assert_eq!(records[0].workflow_key, 1001);
        assert_eq!(records[0].data, vec![1, 2, 3]);
        assert_eq!(records[1].event_type, WalEventType::StepCompleted);
        assert_eq!(records[2].event_type, WalEventType::WorkflowCompleted);

        cleanup(&path);
    }

    #[test]
    fn test_wal_crc_integrity() {
        let path = temp_wal_path("crc_integrity");
        cleanup(&path);

        {
            let mut writer = WalWriter::open(&path).unwrap();
            writer
                .append_event(WalEventType::SignalReceived, 2001, vec![7, 8, 9])
                .unwrap();
        }

        // Corrupt a byte in the data section
        let mut bytes = fs::read(&path).unwrap();
        let len = bytes.len();
        if len > 6 {
            bytes[len - 6] ^= 0xFF;
        }
        fs::write(&path, &bytes).unwrap();

        // Reading should fail with CRC mismatch
        let result = read_wal_records(&path);
        assert!(result.is_err());

        cleanup(&path);
    }

    #[test]
    fn test_wal_manager_append_and_replay() {
        let path = temp_wal_path("manager_replay");
        cleanup(&path);

        let manager = WalManager::new(&path, 1024 * 1024).unwrap();
        manager
            .append(WalEventType::WorkflowStarted, 3001, vec![10])
            .unwrap();
        manager
            .append(WalEventType::StepCompleted, 3001, vec![20])
            .unwrap();
        manager
            .append(WalEventType::ActivityScheduled, 3001, vec![30])
            .unwrap();

        let records = manager.replay().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].event_type, WalEventType::WorkflowStarted);
        assert_eq!(records[1].event_type, WalEventType::StepCompleted);
        assert_eq!(records[2].event_type, WalEventType::ActivityScheduled);

        cleanup(&path);
    }

    #[test]
    fn test_wal_empty_file() {
        let path = temp_wal_path("empty");
        cleanup(&path);

        // Create empty file
        {
            let _writer = WalWriter::open(&path).unwrap();
        }

        let records = read_wal_records(&path).unwrap();
        assert_eq!(records.len(), 0);

        cleanup(&path);
    }

    #[test]
    fn test_crc32_known_values() {
        // CRC32 of empty data
        assert_eq!(crc32_fast(&[]), 0x00000000);
        // CRC32 of "123456789" (standard test vector)
        assert_eq!(crc32_fast(b"123456789"), 0xCBF43926);
    }

    #[test]
    fn test_wal_encode_decode_roundtrip() {
        let record = WalRecord::new(WalEventType::WorkflowStarted, 1001, vec![1, 2, 3]);
        let encoded = record.encode();

        let mut cursor = std::io::Cursor::new(&encoded);
        let decoded = WalRecord::decode(&mut cursor).unwrap().unwrap();

        assert_eq!(decoded.event_type, WalEventType::WorkflowStarted);
        assert_eq!(decoded.workflow_key, 1001);
        assert_eq!(decoded.data, vec![1, 2, 3]);
    }

    #[test]
    fn test_wal_raw_file_write_read() {
        let path = temp_wal_path("raw_write_read");
        cleanup(&path);

        // Write versioned header + manually encoded record
        let record = WalRecord::new(WalEventType::WorkflowStarted, 1001, vec![1, 2, 3]);
        let encoded = record.encode();
        let mut file_data = Vec::new();
        file_data.extend_from_slice(&WAL_MAGIC);
        file_data.extend_from_slice(&WAL_VERSION.to_le_bytes());
        file_data.extend_from_slice(&encoded);

        // Write raw bytes to file
        fs::write(&path, &file_data).unwrap();

        // Read back
        let records = read_wal_records(&path).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].event_type, WalEventType::WorkflowStarted);
        assert_eq!(records[0].workflow_key, 1001);
        assert_eq!(records[0].data, vec![1, 2, 3]);

        cleanup(&path);
    }
}
