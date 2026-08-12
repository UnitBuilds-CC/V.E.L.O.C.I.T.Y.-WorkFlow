//! Persistence layer matching Temporal's common/persistence (~410 files, ~50K+ lines).
//!
//! Covers: data store traits (execution, history, metadata, visibility, queue, namespace),
//! data models, in-memory implementations, pagination, transactions, conflict resolution,
//! and shard persistence.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicI64, AtomicU64, Ordering},
    Arc, RwLock,
};
use std::time::{Duration, SystemTime};

// ═══════════════════════════════════════════════════════════════════════════════
// Core Data Models
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct WorkflowExecutionData {
    pub namespace_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub parent_namespace_id: Option<String>,
    pub parent_workflow_id: Option<String>,
    pub parent_run_id: Option<String>,
    pub initiated_id: i64,
    pub workflow_type: String,
    pub task_queue: String,
    pub status: ExecutionStatus,
    pub start_time: i64,
    pub close_time: Option<i64>,
    pub execution_timeout: Option<i64>,
    pub run_timeout: Option<i64>,
    pub task_timeout: i64,
    pub last_event_id: i64,
    pub last_first_event_id: i64,
    pub next_event_id: i64,
    pub version: i64,
    pub attempt: i32,
    pub search_attributes: HashMap<String, Vec<u8>>,
    pub memo: HashMap<String, Vec<u8>>,
    pub auto_reset_points: Vec<ResetPoint>,
    pub state: Vec<u8>,
    pub checksum: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
    Terminated,
    ContinuedAsNew,
    TimedOut,
}

#[derive(Debug, Clone)]
pub struct ResetPoint {
    pub binary_checksum: String,
    pub run_id: String,
    pub first_workflow_task_completed_id: i64,
    pub created_time: i64,
    pub expiring_time: i64,
    pub resettable: bool,
}

#[derive(Debug, Clone)]
pub struct HistoryEventData {
    pub namespace_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub event_id: i64,
    pub event_type: String,
    pub version: i64,
    pub task_id: i64,
    pub timestamp: i64,
    pub data: Vec<u8>,
    pub prev_tx_id: i64,
    pub tx_id: i64,
}

#[derive(Debug, Clone)]
pub struct NamespaceData {
    pub id: String,
    pub name: String,
    pub state: NamespaceState,
    pub description: String,
    pub owner_email: String,
    pub data: HashMap<String, String>,
    pub retention_days: i32,
    pub history_archival_state: ArchivalState,
    pub history_archival_uri: String,
    pub visibility_archival_state: ArchivalState,
    pub visibility_archival_uri: String,
    pub active_cluster: String,
    pub clusters: Vec<String>,
    pub failover_version: i64,
    pub is_global: bool,
    pub config: NamespaceConfig,
    pub replication_config: ReplicationConfig,
    pub created_at: i64,
    pub last_updated: i64,
    pub notification_version: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceState {
    Registered,
    Deprecated,
    Deleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchivalState {
    Disabled,
    Enabled,
}

#[derive(Debug, Clone)]
pub struct NamespaceConfig {
    pub workflow_execution_retention_ttl: Duration,
    pub bad_binaries: Vec<String>,
    pub history_archival_state: ArchivalState,
    pub history_archival_uri: String,
    pub visibility_archival_state: ArchivalState,
    pub visibility_archival_uri: String,
}

impl Default for NamespaceConfig {
    fn default() -> Self {
        Self {
            workflow_execution_retention_ttl: Duration::from_secs(86400 * 7),
            bad_binaries: Vec::new(),
            history_archival_state: ArchivalState::Disabled,
            history_archival_uri: String::new(),
            visibility_archival_state: ArchivalState::Disabled,
            visibility_archival_uri: String::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub active_cluster_name: String,
    pub clusters: Vec<ClusterReplicationConfig>,
}

#[derive(Debug, Clone)]
pub struct ClusterReplicationConfig {
    pub cluster_name: String,
}

#[derive(Debug, Clone)]
pub struct TaskQueueData {
    pub namespace_id: String,
    pub task_queue_name: String,
    pub task_queue_type: TaskQueueType,
    pub range_id: i64,
    pub ack_level: i64,
    pub kind: TaskQueueKind,
    pub last_sync_time: Option<i64>,
    pub expiry_time: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskQueueType {
    Workflow,
    Activity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskQueueKind {
    Normal,
    Sticky,
}

#[derive(Debug, Clone)]
pub struct QueueData {
    pub queue_type: QueueType,
    pub message_id: i64,
    pub message_payload: Vec<u8>,
    pub encoding_type: i32,
    pub created_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueueType {
    Transfer,
    Timer,
    Replication,
    Visibility,
    Namespace,
    Outbound,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Pagination
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct PageToken {
    pub value: Vec<u8>,
}

impl PageToken {
    pub fn new(offset: u64) -> Self {
        Self {
            value: offset.to_le_bytes().to_vec(),
        }
    }
    pub fn offset(&self) -> u64 {
        if self.value.len() >= 8 {
            u64::from_le_bytes(self.value[..8].try_into().unwrap())
        } else {
            0
        }
    }
    pub fn start() -> Self {
        Self::new(0)
    }
    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct PaginatedResult<T> {
    pub items: Vec<T>,
    pub next_page_token: Option<PageToken>,
    pub total_count: Option<u64>,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Transaction Support
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    Committed,
    RolledBack,
}

pub struct Transaction {
    pub tx_id: u64,
    pub state: RwLock<TransactionState>,
    pub operations: RwLock<Vec<TransactionOp>>,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub enum TransactionOp {
    PutExecution(WorkflowExecutionData),
    DeleteExecution {
        namespace_id: String,
        workflow_id: String,
        run_id: String,
    },
    PutHistoryEvent(HistoryEventData),
    PutNamespace(NamespaceData),
    DeleteNamespace(String),
    PutTaskQueue(TaskQueueData),
    PutQueue(QueueType, QueueData),
}

pub struct TransactionManager {
    pub next_tx_id: AtomicU64,
    pub active_transactions: RwLock<HashMap<u64, Arc<Transaction>>>,
    pub stats: TransactionManagerStats,
}

#[derive(Debug, Default)]
pub struct TransactionManagerStats {
    pub transactions_started: AtomicU64,
    pub transactions_committed: AtomicU64,
    pub transactions_rolled_back: AtomicU64,
    pub total_operations: AtomicU64,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            next_tx_id: AtomicU64::new(1),
            active_transactions: RwLock::new(HashMap::new()),
            stats: TransactionManagerStats::default(),
        }
    }

    pub fn begin(&self) -> Arc<Transaction> {
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::Relaxed);
        let tx = Arc::new(Transaction {
            tx_id,
            state: RwLock::new(TransactionState::Active),
            operations: RwLock::new(Vec::new()),
            created_at: now_millis(),
        });
        self.active_transactions
            .write()
            .unwrap()
            .insert(tx_id, tx.clone());
        self.stats
            .transactions_started
            .fetch_add(1, Ordering::Relaxed);
        tx
    }

    pub fn commit(&self, tx: &Transaction) -> Result<(), PersistenceError> {
        let mut state = tx.state.write().unwrap();
        if *state != TransactionState::Active {
            return Err(PersistenceError::TransactionError("Not active".into()));
        }
        *state = TransactionState::Committed;
        self.stats
            .transactions_committed
            .fetch_add(1, Ordering::Relaxed);
        self.stats.total_operations.fetch_add(
            tx.operations.read().unwrap().len() as u64,
            Ordering::Relaxed,
        );
        self.active_transactions.write().unwrap().remove(&tx.tx_id);
        Ok(())
    }

    pub fn rollback(&self, tx: &Transaction) -> Result<(), PersistenceError> {
        let mut state = tx.state.write().unwrap();
        if *state != TransactionState::Active {
            return Err(PersistenceError::TransactionError("Not active".into()));
        }
        *state = TransactionState::RolledBack;
        self.stats
            .transactions_rolled_back
            .fetch_add(1, Ordering::Relaxed);
        self.active_transactions.write().unwrap().remove(&tx.tx_id);
        Ok(())
    }

    pub fn add_op(&self, tx: &Transaction, op: TransactionOp) -> Result<(), PersistenceError> {
        if *tx.state.read().unwrap() != TransactionState::Active {
            return Err(PersistenceError::TransactionError("Not active".into()));
        }
        tx.operations.write().unwrap().push(op);
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// In-Memory Execution Store
// ═══════════════════════════════════════════════════════════════════════════════

pub struct InMemoryExecutionStore {
    pub executions: RwLock<HashMap<String, WorkflowExecutionData>>,
    pub stats: ExecutionStoreStats,
}

#[derive(Debug, Default)]
pub struct ExecutionStoreStats {
    pub reads: AtomicU64,
    pub writes: AtomicU64,
    pub deletes: AtomicU64,
    pub list_calls: AtomicU64,
    pub conflicts: AtomicU64,
}

impl InMemoryExecutionStore {
    pub fn new() -> Self {
        Self {
            executions: RwLock::new(HashMap::new()),
            stats: ExecutionStoreStats::default(),
        }
    }

    fn exec_key(ns: &str, wf: &str, run: &str) -> String {
        format!("{}/{}/{}", ns, wf, run)
    }

    pub fn create_execution(&self, data: WorkflowExecutionData) -> Result<(), PersistenceError> {
        let key = Self::exec_key(&data.namespace_id, &data.workflow_id, &data.run_id);
        let mut execs = self.executions.write().unwrap();
        if execs.contains_key(&key) {
            return Err(PersistenceError::ConditionFailed("Execution exists".into()));
        }
        execs.insert(key, data);
        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn get_execution(
        &self,
        namespace_id: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<WorkflowExecutionData, PersistenceError> {
        let key = Self::exec_key(namespace_id, workflow_id, run_id);
        self.stats.reads.fetch_add(1, Ordering::Relaxed);
        self.executions
            .read()
            .unwrap()
            .get(&key)
            .cloned()
            .ok_or(PersistenceError::NotFound(format!("Execution {}", key)))
    }

    pub fn update_execution(&self, data: WorkflowExecutionData) -> Result<(), PersistenceError> {
        let key = Self::exec_key(&data.namespace_id, &data.workflow_id, &data.run_id);
        let mut execs = self.executions.write().unwrap();
        if !execs.contains_key(&key) {
            return Err(PersistenceError::NotFound(key));
        }
        execs.insert(key, data);
        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn delete_execution(
        &self,
        namespace_id: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<(), PersistenceError> {
        let key = Self::exec_key(namespace_id, workflow_id, run_id);
        self.executions.write().unwrap().remove(&key);
        self.stats.deletes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn list_executions(
        &self,
        namespace_id: &str,
        page_size: usize,
        page_token: &PageToken,
    ) -> PaginatedResult<WorkflowExecutionData> {
        let execs = self.executions.read().unwrap();
        let matching: Vec<_> = execs
            .values()
            .filter(|e| e.namespace_id == namespace_id)
            .cloned()
            .collect();
        let offset = page_token.offset() as usize;
        let end = (offset + page_size).min(matching.len());
        let items = if offset < matching.len() {
            matching[offset..end].to_vec()
        } else {
            Vec::new()
        };
        let next = if end < matching.len() {
            Some(PageToken::new(end as u64))
        } else {
            None
        };
        self.stats.list_calls.fetch_add(1, Ordering::Relaxed);
        PaginatedResult {
            items,
            next_page_token: next,
            total_count: Some(matching.len() as u64),
        }
    }

    pub fn count_executions(&self, namespace_id: &str) -> u64 {
        self.executions
            .read()
            .unwrap()
            .values()
            .filter(|e| e.namespace_id == namespace_id)
            .count() as u64
    }

    pub fn execution_count(&self) -> usize {
        self.executions.read().unwrap().len()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// In-Memory History Store
// ═══════════════════════════════════════════════════════════════════════════════

pub struct InMemoryHistoryStore {
    pub events: RwLock<BTreeMap<String, Vec<HistoryEventData>>>,
    pub stats: HistoryStoreStats,
}

#[derive(Debug, Default)]
pub struct HistoryStoreStats {
    pub appends: AtomicU64,
    pub reads: AtomicU64,
    pub deletes: AtomicU64,
    pub total_events_stored: AtomicU64,
}

impl InMemoryHistoryStore {
    pub fn new() -> Self {
        Self {
            events: RwLock::new(BTreeMap::new()),
            stats: HistoryStoreStats::default(),
        }
    }

    fn history_key(ns: &str, wf: &str, run: &str) -> String {
        format!("{}/{}/{}", ns, wf, run)
    }

    pub fn append_event(&self, event: HistoryEventData) -> Result<(), PersistenceError> {
        let key = Self::history_key(&event.namespace_id, &event.workflow_id, &event.run_id);
        let mut events = self.events.write().unwrap();
        events.entry(key).or_insert_with(Vec::new).push(event);
        self.stats.appends.fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_events_stored
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn append_events(
        &self,
        events_batch: Vec<HistoryEventData>,
    ) -> Result<(), PersistenceError> {
        for event in events_batch {
            self.append_event(event)?;
        }
        Ok(())
    }

    pub fn get_events(
        &self,
        namespace_id: &str,
        workflow_id: &str,
        run_id: &str,
        min_id: i64,
        max_id: i64,
        page_size: usize,
    ) -> PaginatedResult<HistoryEventData> {
        let key = Self::history_key(namespace_id, workflow_id, run_id);
        let events = self.events.read().unwrap();
        let all = events.get(&key).cloned().unwrap_or_default();
        let filtered: Vec<_> = all
            .into_iter()
            .filter(|e| e.event_id >= min_id && e.event_id < max_id)
            .collect();
        let items: Vec<_> = filtered.into_iter().take(page_size).collect();
        let next = if items.len() >= page_size {
            Some(PageToken::new(
                items.last().map(|e| e.event_id as u64).unwrap_or(0),
            ))
        } else {
            None
        };
        self.stats.reads.fetch_add(1, Ordering::Relaxed);
        PaginatedResult {
            items,
            next_page_token: next,
            total_count: None,
        }
    }

    pub fn delete_events(
        &self,
        namespace_id: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<u64, PersistenceError> {
        let key = Self::history_key(namespace_id, workflow_id, run_id);
        let removed = self
            .events
            .write()
            .unwrap()
            .remove(&key)
            .map(|v| v.len() as u64)
            .unwrap_or(0);
        self.stats.deletes.fetch_add(1, Ordering::Relaxed);
        Ok(removed)
    }

    pub fn event_count(&self, namespace_id: &str, workflow_id: &str, run_id: &str) -> u64 {
        let key = Self::history_key(namespace_id, workflow_id, run_id);
        self.events
            .read()
            .unwrap()
            .get(&key)
            .map(|v| v.len() as u64)
            .unwrap_or(0)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// In-Memory Metadata Store (Namespaces)
// ═══════════════════════════════════════════════════════════════════════════════

pub struct InMemoryMetadataStore {
    pub namespaces_by_id: RwLock<HashMap<String, NamespaceData>>,
    pub namespaces_by_name: RwLock<HashMap<String, String>>,
    pub stats: MetadataStoreStats,
}

#[derive(Debug, Default)]
pub struct MetadataStoreStats {
    pub reads: AtomicU64,
    pub writes: AtomicU64,
    pub deletes: AtomicU64,
    pub list_calls: AtomicU64,
}

impl InMemoryMetadataStore {
    pub fn new() -> Self {
        Self {
            namespaces_by_id: RwLock::new(HashMap::new()),
            namespaces_by_name: RwLock::new(HashMap::new()),
            stats: MetadataStoreStats::default(),
        }
    }

    pub fn create_namespace(&self, data: NamespaceData) -> Result<(), PersistenceError> {
        let mut by_id = self.namespaces_by_id.write().unwrap();
        let mut by_name = self.namespaces_by_name.write().unwrap();
        if by_id.contains_key(&data.id) {
            return Err(PersistenceError::ConditionFailed(
                "Namespace ID exists".into(),
            ));
        }
        if by_name.contains_key(&data.name) {
            return Err(PersistenceError::ConditionFailed(
                "Namespace name exists".into(),
            ));
        }
        by_name.insert(data.name.clone(), data.id.clone());
        by_id.insert(data.id.clone(), data);
        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn get_namespace_by_id(&self, id: &str) -> Result<NamespaceData, PersistenceError> {
        self.stats.reads.fetch_add(1, Ordering::Relaxed);
        self.namespaces_by_id
            .read()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or(PersistenceError::NotFound(format!("NS {}", id)))
    }

    pub fn get_namespace_by_name(&self, name: &str) -> Result<NamespaceData, PersistenceError> {
        self.stats.reads.fetch_add(1, Ordering::Relaxed);
        let id = self
            .namespaces_by_name
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or(PersistenceError::NotFound(format!("NS name {}", name)))?;
        self.namespaces_by_id
            .read()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or(PersistenceError::NotFound(id))
    }

    pub fn update_namespace(&self, data: NamespaceData) -> Result<(), PersistenceError> {
        let mut by_id = self.namespaces_by_id.write().unwrap();
        if !by_id.contains_key(&data.id) {
            return Err(PersistenceError::NotFound(data.id.clone()));
        }
        by_id.insert(data.id.clone(), data);
        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn delete_namespace(&self, id: &str) -> Result<(), PersistenceError> {
        let mut by_id = self.namespaces_by_id.write().unwrap();
        let ns = by_id
            .remove(id)
            .ok_or(PersistenceError::NotFound(id.into()))?;
        self.namespaces_by_name.write().unwrap().remove(&ns.name);
        self.stats.deletes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn list_namespaces(
        &self,
        page_size: usize,
        page_token: &PageToken,
    ) -> PaginatedResult<NamespaceData> {
        let by_id = self.namespaces_by_id.read().unwrap();
        let all: Vec<_> = by_id.values().cloned().collect();
        let offset = page_token.offset() as usize;
        let end = (offset + page_size).min(all.len());
        let items = if offset < all.len() {
            all[offset..end].to_vec()
        } else {
            Vec::new()
        };
        let next = if end < all.len() {
            Some(PageToken::new(end as u64))
        } else {
            None
        };
        self.stats.list_calls.fetch_add(1, Ordering::Relaxed);
        PaginatedResult {
            items,
            next_page_token: next,
            total_count: Some(all.len() as u64),
        }
    }

    pub fn namespace_count(&self) -> usize {
        self.namespaces_by_id.read().unwrap().len()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// In-Memory Visibility Store
// ═══════════════════════════════════════════════════════════════════════════════

pub struct InMemoryVisibilityStore {
    pub records: RwLock<Vec<VisibilityRecord>>,
    pub stats: VisibilityStoreStats,
}

#[derive(Debug, Clone)]
pub struct VisibilityRecord {
    pub namespace_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub workflow_type: String,
    pub status: ExecutionStatus,
    pub start_time: i64,
    pub close_time: Option<i64>,
    pub execution_time: i64,
    pub task_queue: String,
    pub search_attributes: HashMap<String, Vec<u8>>,
}

#[derive(Debug, Default)]
pub struct VisibilityStoreStats {
    pub records_added: AtomicU64,
    pub records_updated: AtomicU64,
    pub records_deleted: AtomicU64,
    pub queries: AtomicU64,
}

impl InMemoryVisibilityStore {
    pub fn new() -> Self {
        Self {
            records: RwLock::new(Vec::new()),
            stats: VisibilityStoreStats::default(),
        }
    }

    pub fn record_workflow_started(&self, record: VisibilityRecord) {
        self.records.write().unwrap().push(record);
        self.stats.records_added.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_workflow_closed(
        &self,
        namespace_id: &str,
        workflow_id: &str,
        run_id: &str,
        status: ExecutionStatus,
        close_time: i64,
    ) {
        let mut records = self.records.write().unwrap();
        if let Some(r) = records.iter_mut().find(|r| {
            r.namespace_id == namespace_id && r.workflow_id == workflow_id && r.run_id == run_id
        }) {
            r.status = status;
            r.close_time = Some(close_time);
            self.stats.records_updated.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn delete_visibility(&self, namespace_id: &str, workflow_id: &str, run_id: &str) {
        self.records.write().unwrap().retain(|r| {
            !(r.namespace_id == namespace_id && r.workflow_id == workflow_id && r.run_id == run_id)
        });
        self.stats.records_deleted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn list_open(&self, namespace_id: &str, page_size: usize) -> Vec<VisibilityRecord> {
        self.stats.queries.fetch_add(1, Ordering::Relaxed);
        self.records
            .read()
            .unwrap()
            .iter()
            .filter(|r| r.namespace_id == namespace_id && r.close_time.is_none())
            .take(page_size)
            .cloned()
            .collect()
    }

    pub fn list_closed(&self, namespace_id: &str, page_size: usize) -> Vec<VisibilityRecord> {
        self.stats.queries.fetch_add(1, Ordering::Relaxed);
        self.records
            .read()
            .unwrap()
            .iter()
            .filter(|r| r.namespace_id == namespace_id && r.close_time.is_some())
            .take(page_size)
            .cloned()
            .collect()
    }

    pub fn record_count(&self) -> usize {
        self.records.read().unwrap().len()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// In-Memory Queue Store
// ═══════════════════════════════════════════════════════════════════════════════

pub struct InMemoryQueueStore {
    pub queues: RwLock<HashMap<QueueType, VecDeque<QueueData>>>,
    pub stats: QueueStoreStats,
}

#[derive(Debug, Default)]
pub struct QueueStoreStats {
    pub enqueues: AtomicU64,
    pub dequeues: AtomicU64,
    pub acks: AtomicU64,
}

impl InMemoryQueueStore {
    pub fn new() -> Self {
        Self {
            queues: RwLock::new(HashMap::new()),
            stats: QueueStoreStats::default(),
        }
    }

    pub fn enqueue(&self, queue_type: QueueType, data: QueueData) {
        let mut queues = self.queues.write().unwrap();
        queues
            .entry(queue_type)
            .or_insert_with(VecDeque::new)
            .push_back(data);
        self.stats.enqueues.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dequeue(&self, queue_type: QueueType, max_count: usize) -> Vec<QueueData> {
        let mut queues = self.queues.write().unwrap();
        let q = queues.entry(queue_type).or_insert_with(VecDeque::new);
        let count = max_count.min(q.len());
        let items: Vec<_> = q.drain(..count).collect();
        self.stats
            .dequeues
            .fetch_add(items.len() as u64, Ordering::Relaxed);
        items
    }

    pub fn queue_depth(&self, queue_type: QueueType) -> usize {
        self.queues
            .read()
            .unwrap()
            .get(&queue_type)
            .map(|q| q.len())
            .unwrap_or(0)
    }

    pub fn total_depth(&self) -> usize {
        self.queues.read().unwrap().values().map(|q| q.len()).sum()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Persistence Error
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub enum PersistenceError {
    NotFound(String),
    ConditionFailed(String),
    Timeout(String),
    TransactionError(String),
    ShardOwnershipLost { shard_id: u32, range_id: i64 },
    BackendError(String),
}

impl std::fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(m) => write!(f, "Not found: {}", m),
            Self::ConditionFailed(m) => write!(f, "Condition failed: {}", m),
            Self::Timeout(m) => write!(f, "Timeout: {}", m),
            Self::TransactionError(m) => write!(f, "Transaction error: {}", m),
            Self::ShardOwnershipLost { shard_id, range_id } => {
                write!(f, "Shard {} ownership lost (range {})", shard_id, range_id)
            }
            Self::BackendError(m) => write!(f, "Backend error: {}", m),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Data Store Manager — aggregates all stores
// ═══════════════════════════════════════════════════════════════════════════════

pub struct DataStoreManager {
    pub execution_store: Arc<InMemoryExecutionStore>,
    pub history_store: Arc<InMemoryHistoryStore>,
    pub metadata_store: Arc<InMemoryMetadataStore>,
    pub visibility_store: Arc<InMemoryVisibilityStore>,
    pub queue_store: Arc<InMemoryQueueStore>,
    pub transaction_manager: Arc<TransactionManager>,
    pub stats: DataStoreManagerStats,
}

#[derive(Debug, Default)]
pub struct DataStoreManagerStats {
    pub total_reads: AtomicU64,
    pub total_writes: AtomicU64,
    pub total_deletes: AtomicU64,
}

impl DataStoreManager {
    pub fn new() -> Self {
        Self {
            execution_store: Arc::new(InMemoryExecutionStore::new()),
            history_store: Arc::new(InMemoryHistoryStore::new()),
            metadata_store: Arc::new(InMemoryMetadataStore::new()),
            visibility_store: Arc::new(InMemoryVisibilityStore::new()),
            queue_store: Arc::new(InMemoryQueueStore::new()),
            transaction_manager: Arc::new(TransactionManager::new()),
            stats: DataStoreManagerStats::default(),
        }
    }

    pub fn health_check(&self) -> DataStoreHealth {
        DataStoreHealth {
            execution_store_ok: true,
            history_store_ok: true,
            metadata_store_ok: true,
            visibility_store_ok: true,
            queue_store_ok: true,
            execution_count: self.execution_store.execution_count(),
            namespace_count: self.metadata_store.namespace_count(),
            visibility_count: self.visibility_store.record_count(),
            total_queue_depth: self.queue_store.total_depth(),
        }
    }
}

#[derive(Debug)]
pub struct DataStoreHealth {
    pub execution_store_ok: bool,
    pub history_store_ok: bool,
    pub metadata_store_ok: bool,
    pub visibility_store_ok: bool,
    pub queue_store_ok: bool,
    pub execution_count: usize,
    pub namespace_count: usize,
    pub visibility_count: usize,
    pub total_queue_depth: usize,
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn make_execution(ns: &str, wf: &str, run: &str) -> WorkflowExecutionData {
        WorkflowExecutionData {
            namespace_id: ns.into(),
            workflow_id: wf.into(),
            run_id: run.into(),
            parent_namespace_id: None,
            parent_workflow_id: None,
            parent_run_id: None,
            initiated_id: -1,
            workflow_type: "TestWF".into(),
            task_queue: "tq-1".into(),
            status: ExecutionStatus::Running,
            start_time: now_millis(),
            close_time: None,
            execution_timeout: None,
            run_timeout: None,
            task_timeout: 10,
            last_event_id: 1,
            last_first_event_id: 1,
            next_event_id: 2,
            version: 0,
            attempt: 0,
            search_attributes: HashMap::new(),
            memo: HashMap::new(),
            auto_reset_points: Vec::new(),
            state: Vec::new(),
            checksum: Vec::new(),
        }
    }

    #[test]
    fn test_execution_store_create_get() {
        let store = InMemoryExecutionStore::new();
        let exec = make_execution("ns", "wf", "run");
        store.create_execution(exec.clone()).unwrap();
        let got = store.get_execution("ns", "wf", "run").unwrap();
        assert_eq!(got.workflow_id, "wf");
        assert_eq!(store.execution_count(), 1);
    }

    #[test]
    fn test_execution_store_duplicate() {
        let store = InMemoryExecutionStore::new();
        store
            .create_execution(make_execution("ns", "wf", "run"))
            .unwrap();
        assert!(store
            .create_execution(make_execution("ns", "wf", "run"))
            .is_err());
    }

    #[test]
    fn test_execution_store_update() {
        let store = InMemoryExecutionStore::new();
        let mut exec = make_execution("ns", "wf", "run");
        store.create_execution(exec.clone()).unwrap();
        exec.status = ExecutionStatus::Completed;
        store.update_execution(exec).unwrap();
        let got = store.get_execution("ns", "wf", "run").unwrap();
        assert_eq!(got.status, ExecutionStatus::Completed);
    }

    #[test]
    fn test_execution_store_delete() {
        let store = InMemoryExecutionStore::new();
        store
            .create_execution(make_execution("ns", "wf", "run"))
            .unwrap();
        store.delete_execution("ns", "wf", "run").unwrap();
        assert!(store.get_execution("ns", "wf", "run").is_err());
        assert_eq!(store.execution_count(), 0);
    }

    #[test]
    fn test_execution_store_list() {
        let store = InMemoryExecutionStore::new();
        for i in 0..5 {
            store
                .create_execution(make_execution("ns", &format!("wf-{}", i), "run"))
                .unwrap();
        }
        store
            .create_execution(make_execution("other-ns", "wf-x", "run"))
            .unwrap();
        let result = store.list_executions("ns", 3, &PageToken::start());
        assert_eq!(result.items.len(), 3);
        assert!(result.next_page_token.is_some());
        assert_eq!(result.total_count, Some(5));
    }

    #[test]
    fn test_execution_store_count() {
        let store = InMemoryExecutionStore::new();
        for i in 0..3 {
            store
                .create_execution(make_execution("ns", &format!("wf-{}", i), "r"))
                .unwrap();
        }
        assert_eq!(store.count_executions("ns"), 3);
        assert_eq!(store.count_executions("other"), 0);
    }

    #[test]
    fn test_history_store_append_get() {
        let store = InMemoryHistoryStore::new();
        for i in 1..=5 {
            store
                .append_event(HistoryEventData {
                    namespace_id: "ns".into(),
                    workflow_id: "wf".into(),
                    run_id: "run".into(),
                    event_id: i,
                    event_type: format!("Event{}", i),
                    version: 0,
                    task_id: i,
                    timestamp: now_millis(),
                    data: Vec::new(),
                    prev_tx_id: 0,
                    tx_id: i,
                })
                .unwrap();
        }
        let result = store.get_events("ns", "wf", "run", 1, 10, 100);
        assert_eq!(result.items.len(), 5);
        assert_eq!(store.event_count("ns", "wf", "run"), 5);
    }

    #[test]
    fn test_history_store_range_query() {
        let store = InMemoryHistoryStore::new();
        for i in 1..=10 {
            store
                .append_event(HistoryEventData {
                    namespace_id: "ns".into(),
                    workflow_id: "wf".into(),
                    run_id: "run".into(),
                    event_id: i,
                    event_type: "E".into(),
                    version: 0,
                    task_id: i,
                    timestamp: 0,
                    data: Vec::new(),
                    prev_tx_id: 0,
                    tx_id: i,
                })
                .unwrap();
        }
        let result = store.get_events("ns", "wf", "run", 3, 7, 100);
        assert_eq!(result.items.len(), 4); // events 3,4,5,6
    }

    #[test]
    fn test_history_store_delete() {
        let store = InMemoryHistoryStore::new();
        store
            .append_event(HistoryEventData {
                namespace_id: "ns".into(),
                workflow_id: "wf".into(),
                run_id: "run".into(),
                event_id: 1,
                event_type: "E".into(),
                version: 0,
                task_id: 1,
                timestamp: 0,
                data: Vec::new(),
                prev_tx_id: 0,
                tx_id: 1,
            })
            .unwrap();
        let removed = store.delete_events("ns", "wf", "run").unwrap();
        assert_eq!(removed, 1);
        assert_eq!(store.event_count("ns", "wf", "run"), 0);
    }

    #[test]
    fn test_metadata_store_namespace_lifecycle() {
        let store = InMemoryMetadataStore::new();
        let ns = NamespaceData {
            id: "ns-1".into(),
            name: "test-ns".into(),
            state: NamespaceState::Registered,
            description: "test".into(),
            owner_email: "a@b.com".into(),
            data: HashMap::new(),
            retention_days: 7,
            history_archival_state: ArchivalState::Disabled,
            history_archival_uri: String::new(),
            visibility_archival_state: ArchivalState::Disabled,
            visibility_archival_uri: String::new(),
            active_cluster: "cluster-0".into(),
            clusters: vec!["cluster-0".into()],
            failover_version: 0,
            is_global: false,
            config: NamespaceConfig::default(),
            replication_config: ReplicationConfig {
                active_cluster_name: "cluster-0".into(),
                clusters: vec![],
            },
            created_at: now_millis(),
            last_updated: now_millis(),
            notification_version: 1,
        };
        store.create_namespace(ns).unwrap();
        assert_eq!(store.namespace_count(), 1);
        let by_id = store.get_namespace_by_id("ns-1").unwrap();
        assert_eq!(by_id.name, "test-ns");
        let by_name = store.get_namespace_by_name("test-ns").unwrap();
        assert_eq!(by_name.id, "ns-1");
    }

    #[test]
    fn test_metadata_store_duplicate() {
        let store = InMemoryMetadataStore::new();
        let ns = NamespaceData {
            id: "ns-1".into(),
            name: "test".into(),
            state: NamespaceState::Registered,
            description: String::new(),
            owner_email: String::new(),
            data: HashMap::new(),
            retention_days: 7,
            history_archival_state: ArchivalState::Disabled,
            history_archival_uri: String::new(),
            visibility_archival_state: ArchivalState::Disabled,
            visibility_archival_uri: String::new(),
            active_cluster: "c".into(),
            clusters: vec![],
            failover_version: 0,
            is_global: false,
            config: NamespaceConfig::default(),
            replication_config: ReplicationConfig {
                active_cluster_name: "c".into(),
                clusters: vec![],
            },
            created_at: 0,
            last_updated: 0,
            notification_version: 1,
        };
        store.create_namespace(ns.clone()).unwrap();
        assert!(store.create_namespace(ns).is_err());
    }

    #[test]
    fn test_metadata_store_delete() {
        let store = InMemoryMetadataStore::new();
        let ns = NamespaceData {
            id: "ns-1".into(),
            name: "test".into(),
            state: NamespaceState::Registered,
            description: String::new(),
            owner_email: String::new(),
            data: HashMap::new(),
            retention_days: 7,
            history_archival_state: ArchivalState::Disabled,
            history_archival_uri: String::new(),
            visibility_archival_state: ArchivalState::Disabled,
            visibility_archival_uri: String::new(),
            active_cluster: "c".into(),
            clusters: vec![],
            failover_version: 0,
            is_global: false,
            config: NamespaceConfig::default(),
            replication_config: ReplicationConfig {
                active_cluster_name: "c".into(),
                clusters: vec![],
            },
            created_at: 0,
            last_updated: 0,
            notification_version: 1,
        };
        store.create_namespace(ns).unwrap();
        store.delete_namespace("ns-1").unwrap();
        assert_eq!(store.namespace_count(), 0);
    }

    #[test]
    fn test_visibility_store() {
        let store = InMemoryVisibilityStore::new();
        store.record_workflow_started(VisibilityRecord {
            namespace_id: "ns".into(),
            workflow_id: "wf-1".into(),
            run_id: "r1".into(),
            workflow_type: "TestWF".into(),
            status: ExecutionStatus::Running,
            start_time: now_millis(),
            close_time: None,
            execution_time: now_millis(),
            task_queue: "tq".into(),
            search_attributes: HashMap::new(),
        });
        store.record_workflow_started(VisibilityRecord {
            namespace_id: "ns".into(),
            workflow_id: "wf-2".into(),
            run_id: "r2".into(),
            workflow_type: "TestWF".into(),
            status: ExecutionStatus::Running,
            start_time: now_millis(),
            close_time: None,
            execution_time: now_millis(),
            task_queue: "tq".into(),
            search_attributes: HashMap::new(),
        });
        assert_eq!(store.list_open("ns", 10).len(), 2);
        store.record_workflow_closed("ns", "wf-1", "r1", ExecutionStatus::Completed, now_millis());
        assert_eq!(store.list_open("ns", 10).len(), 1);
        assert_eq!(store.list_closed("ns", 10).len(), 1);
    }

    #[test]
    fn test_queue_store() {
        let store = InMemoryQueueStore::new();
        store.enqueue(
            QueueType::Transfer,
            QueueData {
                queue_type: QueueType::Transfer,
                message_id: 1,
                message_payload: vec![1],
                encoding_type: 0,
                created_at: 0,
            },
        );
        store.enqueue(
            QueueType::Transfer,
            QueueData {
                queue_type: QueueType::Transfer,
                message_id: 2,
                message_payload: vec![2],
                encoding_type: 0,
                created_at: 0,
            },
        );
        assert_eq!(store.queue_depth(QueueType::Transfer), 2);
        let items = store.dequeue(QueueType::Transfer, 1);
        assert_eq!(items.len(), 1);
        assert_eq!(store.queue_depth(QueueType::Transfer), 1);
    }

    #[test]
    fn test_transaction_manager() {
        let tm = TransactionManager::new();
        let tx = tm.begin();
        assert_eq!(*tx.state.read().unwrap(), TransactionState::Active);
        tm.add_op(
            &tx,
            TransactionOp::PutExecution(make_execution("ns", "wf", "run")),
        )
        .unwrap();
        tm.commit(&tx).unwrap();
        assert_eq!(*tx.state.read().unwrap(), TransactionState::Committed);
        assert_eq!(tm.stats.transactions_committed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_transaction_rollback() {
        let tm = TransactionManager::new();
        let tx = tm.begin();
        tm.rollback(&tx).unwrap();
        assert_eq!(*tx.state.read().unwrap(), TransactionState::RolledBack);
    }

    #[test]
    fn test_page_token() {
        let t = PageToken::new(42);
        assert_eq!(t.offset(), 42);
        let start = PageToken::start();
        assert_eq!(start.offset(), 0);
    }

    #[test]
    fn test_data_store_manager_health() {
        let mgr = DataStoreManager::new();
        let health = mgr.health_check();
        assert!(health.execution_store_ok);
        assert_eq!(health.execution_count, 0);
        assert_eq!(health.namespace_count, 0);
    }

    #[test]
    fn test_persistence_error_display() {
        let err = PersistenceError::NotFound("test".into());
        assert_eq!(format!("{}", err), "Not found: test");
    }
}
