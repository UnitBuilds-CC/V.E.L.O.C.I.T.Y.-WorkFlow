//! Virtual Objects — Restate-style actor-model keyed state.
//!
//! A Virtual Object is a stateful entity keyed by an ID (e.g., a cart, a chat session,
//! an agent). Each key has isolated K/V state, single-writer concurrency (operations on
//! the same key are serialized), and parallel execution across different keys.
//!
//! This module provides:
//! - Virtual Object definitions with typed handlers
//! - Per-key isolated state (K/V store)
//! - Single-writer concurrency per key (serialized access)
//! - Parallel execution across keys
//! - Durable state that survives crashes
//! - Handler invocation with journaling
//! - Awakeable support (external resolution points)

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

/// Unique identifier for a virtual object instance.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ObjectKey {
    /// The object type (e.g., "ChatAgent", "ShoppingCart").
    pub object_type: String,
    /// The unique key within the type (e.g., "session-42", "cart-abc").
    pub key: String,
}

impl ObjectKey {
    pub fn new(object_type: &str, key: &str) -> Self {
        Self {
            object_type: object_type.to_string(),
            key: key.to_string(),
        }
    }

    /// Returns a combined string representation for hashing/display.
    pub fn full_key(&self) -> String {
        format!("{}/{}", self.object_type, self.key)
    }
}

/// State of a virtual object handler invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandlerState {
    /// Handler is queued, waiting for key lock.
    Queued,
    /// Handler is actively executing.
    Running,
    /// Handler is suspended (awaiting external input).
    Suspended,
    /// Handler completed successfully.
    Completed,
    /// Handler failed.
    Failed,
}

/// A journal entry for a durable step within a handler.
#[derive(Debug, Clone)]
pub struct JournalEntry {
    /// Sequence number within this handler invocation.
    pub sequence: u32,
    /// The type of journal entry.
    pub entry_type: JournalEntryType,
    /// Serialized input (if any).
    pub input: Vec<u8>,
    /// Serialized output (if any).
    pub output: Vec<u8>,
    /// Whether this entry has been completed.
    pub completed: bool,
}

/// Types of journal entries.
#[derive(Debug, Clone)]
pub enum JournalEntryType {
    /// A durable step (ctx.run() equivalent).
    DurableStep,
    /// A state get operation.
    StateGet { state_key: String },
    /// A state set operation.
    StateSet { state_key: String },
    /// A state clear operation.
    StateClear { state_key: String },
    /// A call to another virtual object.
    ObjectCall { target: ObjectKey, method: String },
    /// A sleep/timer.
    Sleep { duration_ms: u64 },
    /// An awakeable (external resolution point).
    Awakeable { awakeable_id: String },
    /// Resolving an awakeable.
    ResolveAwakeable { awakeable_id: String },
}

/// A registered handler method on a virtual object type.
#[derive(Debug, Clone)]
pub struct HandlerDefinition {
    /// The handler name (e.g., "message", "checkout").
    pub name: String,
    /// Whether this handler is a workflow (runs to completion) or service (one-shot).
    pub handler_kind: HandlerKind,
    /// Input schema (for validation).
    pub input_schema: Option<String>,
    /// Output schema (for validation).
    pub output_schema: Option<String>,
}

/// Kind of handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandlerKind {
    /// Workflow handler — long-running, durable to completion.
    Workflow,
    /// Service handler — one-shot request/response.
    Service,
    /// Shared handler — concurrent access allowed (read-only).
    Shared,
}

/// An active handler invocation.
#[derive(Debug, Clone)]
pub struct HandlerInvocation {
    /// Unique invocation ID.
    pub invocation_id: u64,
    /// The target object key.
    pub target: ObjectKey,
    /// The handler being invoked.
    pub handler_name: String,
    /// Current state.
    pub state: HandlerState,
    /// Input payload.
    pub input: Vec<u8>,
    /// Output payload (when completed).
    pub output: Option<Vec<u8>>,
    /// Error message (when failed).
    pub error: Option<String>,
    /// Journal entries for this invocation.
    pub journal: Vec<JournalEntry>,
    /// Next journal sequence number.
    pub next_sequence: u32,
    /// Idempotency key (if provided).
    pub idempotency_key: Option<String>,
    /// Creation timestamp (ms).
    pub created_ms: u64,
    /// Completion timestamp (ms).
    pub completed_ms: u64,
}

/// Per-key state store for a virtual object instance.
#[derive(Debug, Clone, Default)]
pub struct ObjectState {
    /// K/V state entries.
    pub entries: HashMap<String, Vec<u8>>,
    /// Version counter (incremented on each mutation).
    pub version: u64,
}

/// Statistics for the virtual object subsystem.
#[derive(Debug, Clone, Default)]
pub struct VirtualObjectStats {
    pub total_invocations: u64,
    pub active_invocations: u64,
    pub completed_invocations: u64,
    pub failed_invocations: u64,
    pub suspended_invocations: u64,
    pub total_object_keys: u64,
    pub total_state_entries: u64,
    pub total_journal_entries: u64,
    pub queue_depth: u64,
}

/// An awakeable — an external resolution point.
#[derive(Debug, Clone)]
pub struct Awakeable {
    /// Unique awakeable ID.
    pub id: String,
    /// The invocation that created it.
    pub owner_invocation_id: u64,
    /// The object key that owns it.
    pub owner_key: ObjectKey,
    /// Whether it has been resolved.
    pub resolved: bool,
    /// Resolution value (if resolved).
    pub value: Option<Vec<u8>>,
    /// Error (if rejected).
    pub error: Option<String>,
}

/// The Virtual Object runtime — manages all virtual objects, their state, and invocations.
///
/// This is the core of the Restate-compatible flavor. It provides:
/// - Object type registration
/// - Per-key state isolation
/// - Single-writer concurrency per key (invocations on same key are serialized)
/// - Parallel execution across keys
/// - Durable journal for crash recovery
/// - Awakeable management for external resolution
pub struct VirtualObjectRuntime {
    /// Registered object types and their handlers.
    object_types: HashMap<String, Vec<HandlerDefinition>>,
    /// Per-key state.
    key_state: HashMap<String, ObjectState>,
    /// Active invocations.
    invocations: HashMap<u64, HandlerInvocation>,
    /// Invocation queue per key (for single-writer serialization).
    key_queues: HashMap<String, VecDeque<u64>>,
    /// Currently running invocation per key (None if key is free).
    key_locks: HashMap<String, u64>,
    /// Awakeables by ID.
    awakeables: HashMap<String, Awakeable>,
    /// Idempotency key -> invocation ID mapping.
    idempotency_map: HashMap<String, u64>,
    /// Next invocation ID.
    next_invocation_id: AtomicU64,
    /// Statistics.
    stats: VirtualObjectStats,
}

impl VirtualObjectRuntime {
    /// Create a new virtual object runtime.
    pub fn new() -> Self {
        Self {
            object_types: HashMap::new(),
            key_state: HashMap::new(),
            invocations: HashMap::new(),
            key_queues: HashMap::new(),
            key_locks: HashMap::new(),
            awakeables: HashMap::new(),
            idempotency_map: HashMap::new(),
            next_invocation_id: AtomicU64::new(1),
            stats: VirtualObjectStats::default(),
        }
    }

    // ─── Object Type Registration ──────────────────────────────────────────

    /// Register a virtual object type with its handlers.
    pub fn register_object_type(&mut self, type_name: &str, handlers: Vec<HandlerDefinition>) {
        self.object_types.insert(type_name.to_string(), handlers);
    }

    /// Get the handlers for an object type.
    pub fn get_handlers(&self, type_name: &str) -> Option<&Vec<HandlerDefinition>> {
        self.object_types.get(type_name)
    }

    /// List all registered object types.
    pub fn list_object_types(&self) -> Vec<&String> {
        self.object_types.keys().collect()
    }

    // ─── Handler Invocation ────────────────────────────────────────────────

    /// Invoke a handler on a virtual object.
    ///
    /// If another invocation is running on the same key, this is queued
    /// (single-writer concurrency). If the key is free, execution starts immediately.
    pub fn invoke(
        &mut self,
        object_type: &str,
        key: &str,
        handler_name: &str,
        input: Vec<u8>,
        idempotency_key: Option<String>,
    ) -> Result<u64, VirtualObjectError> {
        // Check idempotency
        if let Some(ref idk) = idempotency_key {
            if let Some(&existing_id) = self.idempotency_map.get(idk) {
                return Ok(existing_id);
            }
        }

        // Validate object type and handler exist
        let handlers = self
            .object_types
            .get(object_type)
            .ok_or_else(|| VirtualObjectError::UnknownObjectType(object_type.to_string()))?;

        let _handler = handlers
            .iter()
            .find(|h| h.name == handler_name)
            .ok_or_else(|| {
                VirtualObjectError::UnknownHandler(format!("{}/{}", object_type, handler_name))
            })?;

        let object_key = ObjectKey::new(object_type, key);
        let full_key = object_key.full_key();
        let invocation_id = self.next_invocation_id.fetch_add(1, Ordering::Relaxed);

        let invocation = HandlerInvocation {
            invocation_id,
            target: object_key,
            handler_name: handler_name.to_string(),
            state: HandlerState::Queued,
            input,
            output: None,
            error: None,
            journal: Vec::new(),
            next_sequence: 0,
            idempotency_key: idempotency_key.clone(),
            created_ms: 0,
            completed_ms: 0,
        };

        // Store idempotency mapping
        if let Some(ref idk) = idempotency_key {
            self.idempotency_map.insert(idk.clone(), invocation_id);
        }

        self.invocations.insert(invocation_id, invocation);
        self.stats.total_invocations += 1;
        self.stats.active_invocations += 1;
        self.stats.total_object_keys = self.key_state.len() as u64;

        // Ensure key state exists
        self.key_state.entry(full_key.clone()).or_default();

        // Queue the invocation for this key
        self.key_queues
            .entry(full_key.clone())
            .or_default()
            .push_back(invocation_id);

        // Try to dispatch (if key is free)
        self.try_dispatch(&full_key);

        Ok(invocation_id)
    }

    /// Try to dispatch the next queued invocation for a key.
    fn try_dispatch(&mut self, full_key: &str) {
        // If key is already locked, don't dispatch
        if self.key_locks.contains_key(full_key) {
            return;
        }

        // Get next invocation from queue
        if let Some(queue) = self.key_queues.get_mut(full_key) {
            if let Some(invocation_id) = queue.pop_front() {
                if let Some(inv) = self.invocations.get_mut(&invocation_id) {
                    inv.state = HandlerState::Running;
                }
                self.key_locks.insert(full_key.to_string(), invocation_id);
            }
        }
    }

    // ─── Journal Operations ────────────────────────────────────────────────

    /// Append a journal entry for a running invocation.
    pub fn append_journal(
        &mut self,
        invocation_id: u64,
        entry_type: JournalEntryType,
        input: Vec<u8>,
    ) -> Result<u32, VirtualObjectError> {
        let inv = self
            .invocations
            .get_mut(&invocation_id)
            .ok_or(VirtualObjectError::UnknownInvocation(invocation_id))?;

        let sequence = inv.next_sequence;
        inv.next_sequence += 1;

        inv.journal.push(JournalEntry {
            sequence,
            entry_type,
            input,
            output: Vec::new(),
            completed: false,
        });

        self.stats.total_journal_entries += 1;
        Ok(sequence)
    }

    /// Complete a journal entry with output.
    pub fn complete_journal(
        &mut self,
        invocation_id: u64,
        sequence: u32,
        output: Vec<u8>,
    ) -> Result<(), VirtualObjectError> {
        let inv = self
            .invocations
            .get_mut(&invocation_id)
            .ok_or(VirtualObjectError::UnknownInvocation(invocation_id))?;

        if let Some(entry) = inv.journal.iter_mut().find(|e| e.sequence == sequence) {
            entry.output = output;
            entry.completed = true;
            Ok(())
        } else {
            Err(VirtualObjectError::UnknownJournalEntry(sequence))
        }
    }

    // ─── State Operations ──────────────────────────────────────────────────

    /// Get a state value for a virtual object key.
    pub fn state_get(&self, object_type: &str, key: &str, state_key: &str) -> Option<&Vec<u8>> {
        let full_key = format!("{}/{}", object_type, key);
        self.key_state.get(&full_key)?.entries.get(state_key)
    }

    /// Set a state value for a virtual object key.
    pub fn state_set(
        &mut self,
        object_type: &str,
        key: &str,
        state_key: &str,
        value: Vec<u8>,
    ) -> Result<(), VirtualObjectError> {
        let full_key = format!("{}/{}", object_type, key);
        let state = self
            .key_state
            .get_mut(&full_key)
            .ok_or(VirtualObjectError::UnknownKey(full_key))?;

        state.entries.insert(state_key.to_string(), value);
        state.version += 1;
        self.stats.total_state_entries = state.entries.len() as u64;
        Ok(())
    }

    /// Clear a state value for a virtual object key.
    pub fn state_clear(
        &mut self,
        object_type: &str,
        key: &str,
        state_key: &str,
    ) -> Result<(), VirtualObjectError> {
        let full_key = format!("{}/{}", object_type, key);
        let state = self
            .key_state
            .get_mut(&full_key)
            .ok_or(VirtualObjectError::UnknownKey(full_key))?;

        state.entries.remove(state_key);
        state.version += 1;
        Ok(())
    }

    /// Get all state keys for a virtual object.
    pub fn state_keys(&self, object_type: &str, key: &str) -> Vec<&String> {
        let full_key = format!("{}/{}", object_type, key);
        match self.key_state.get(&full_key) {
            Some(state) => state.entries.keys().collect(),
            None => Vec::new(),
        }
    }

    // ─── Invocation Completion ─────────────────────────────────────────────

    /// Mark an invocation as completed.
    pub fn complete_invocation(
        &mut self,
        invocation_id: u64,
        output: Vec<u8>,
    ) -> Result<(), VirtualObjectError> {
        let inv = self
            .invocations
            .get_mut(&invocation_id)
            .ok_or(VirtualObjectError::UnknownInvocation(invocation_id))?;

        let full_key = inv.target.full_key();
        inv.state = HandlerState::Completed;
        inv.output = Some(output);
        inv.completed_ms = 0; // Would use system clock

        self.stats.active_invocations = self.stats.active_invocations.saturating_sub(1);
        self.stats.completed_invocations += 1;

        // Release key lock and dispatch next
        self.key_locks.remove(&full_key);
        self.try_dispatch(&full_key);

        Ok(())
    }

    /// Mark an invocation as failed.
    pub fn fail_invocation(
        &mut self,
        invocation_id: u64,
        error: String,
    ) -> Result<(), VirtualObjectError> {
        let inv = self
            .invocations
            .get_mut(&invocation_id)
            .ok_or(VirtualObjectError::UnknownInvocation(invocation_id))?;

        let full_key = inv.target.full_key();
        inv.state = HandlerState::Failed;
        inv.error = Some(error);
        inv.completed_ms = 0;

        self.stats.active_invocations = self.stats.active_invocations.saturating_sub(1);
        self.stats.failed_invocations += 1;

        // Release key lock and dispatch next
        self.key_locks.remove(&full_key);
        self.try_dispatch(&full_key);

        Ok(())
    }

    /// Suspend an invocation (awaiting external input).
    pub fn suspend_invocation(&mut self, invocation_id: u64) -> Result<(), VirtualObjectError> {
        let inv = self
            .invocations
            .get_mut(&invocation_id)
            .ok_or(VirtualObjectError::UnknownInvocation(invocation_id))?;

        inv.state = HandlerState::Suspended;
        self.stats.suspended_invocations += 1;
        Ok(())
    }

    /// Resume a suspended invocation.
    pub fn resume_invocation(&mut self, invocation_id: u64) -> Result<(), VirtualObjectError> {
        let inv = self
            .invocations
            .get_mut(&invocation_id)
            .ok_or(VirtualObjectError::UnknownInvocation(invocation_id))?;

        if inv.state != HandlerState::Suspended {
            return Err(VirtualObjectError::InvalidStateTransition(format!(
                "Cannot resume invocation in state {:?}",
                inv.state
            )));
        }

        inv.state = HandlerState::Running;
        self.stats.suspended_invocations = self.stats.suspended_invocations.saturating_sub(1);
        Ok(())
    }

    // ─── Awakeables ────────────────────────────────────────────────────────

    /// Create an awakeable for an invocation.
    pub fn create_awakeable(&mut self, invocation_id: u64) -> Result<String, VirtualObjectError> {
        let inv = self
            .invocations
            .get(&invocation_id)
            .ok_or(VirtualObjectError::UnknownInvocation(invocation_id))?;

        let awakeable_id = format!("awk_{}_{}", invocation_id, self.awakeables.len());
        let awakeable = Awakeable {
            id: awakeable_id.clone(),
            owner_invocation_id: invocation_id,
            owner_key: inv.target.clone(),
            resolved: false,
            value: None,
            error: None,
        };

        self.awakeables.insert(awakeable_id.clone(), awakeable);
        Ok(awakeable_id)
    }

    /// Resolve an awakeable (external system calls this).
    pub fn resolve_awakeable(
        &mut self,
        awakeable_id: &str,
        value: Vec<u8>,
    ) -> Result<u64, VirtualObjectError> {
        let awk = self
            .awakeables
            .get_mut(awakeable_id)
            .ok_or_else(|| VirtualObjectError::UnknownAwakeable(awakeable_id.to_string()))?;

        if awk.resolved {
            return Err(VirtualObjectError::AlreadyResolved(
                awakeable_id.to_string(),
            ));
        }

        awk.resolved = true;
        awk.value = Some(value);
        let owner_id = awk.owner_invocation_id;

        // Resume the owner invocation if it was suspended
        if let Some(inv) = self.invocations.get_mut(&owner_id) {
            if inv.state == HandlerState::Suspended {
                inv.state = HandlerState::Running;
                self.stats.suspended_invocations =
                    self.stats.suspended_invocations.saturating_sub(1);
            }
        }

        Ok(owner_id)
    }

    /// Reject an awakeable (external system signals failure).
    pub fn reject_awakeable(
        &mut self,
        awakeable_id: &str,
        error: String,
    ) -> Result<u64, VirtualObjectError> {
        let awk = self
            .awakeables
            .get_mut(awakeable_id)
            .ok_or_else(|| VirtualObjectError::UnknownAwakeable(awakeable_id.to_string()))?;

        if awk.resolved {
            return Err(VirtualObjectError::AlreadyResolved(
                awakeable_id.to_string(),
            ));
        }

        awk.resolved = true;
        awk.error = Some(error);
        let owner_id = awk.owner_invocation_id;

        // Resume the owner (it will see the error)
        if let Some(inv) = self.invocations.get_mut(&owner_id) {
            if inv.state == HandlerState::Suspended {
                inv.state = HandlerState::Running;
                self.stats.suspended_invocations =
                    self.stats.suspended_invocations.saturating_sub(1);
            }
        }

        Ok(owner_id)
    }

    // ─── Query Operations ──────────────────────────────────────────────────

    /// Get an invocation by ID.
    pub fn get_invocation(&self, invocation_id: u64) -> Option<&HandlerInvocation> {
        self.invocations.get(&invocation_id)
    }

    /// Get invocation output (for completed invocations).
    pub fn get_output(&self, invocation_id: u64) -> Option<&Vec<u8>> {
        self.invocations.get(&invocation_id)?.output.as_ref()
    }

    /// Check if a key has an active invocation.
    pub fn is_key_busy(&self, object_type: &str, key: &str) -> bool {
        let full_key = format!("{}/{}", object_type, key);
        self.key_locks.contains_key(&full_key)
    }

    /// Get queue depth for a key.
    pub fn key_queue_depth(&self, object_type: &str, key: &str) -> usize {
        let full_key = format!("{}/{}", object_type, key);
        self.key_queues.get(&full_key).map_or(0, |q| q.len())
    }

    /// Get statistics.
    pub fn stats(&self) -> &VirtualObjectStats {
        &self.stats
    }

    /// Get the number of registered object types.
    pub fn object_type_count(&self) -> usize {
        self.object_types.len()
    }

    /// Get the total number of tracked keys.
    pub fn key_count(&self) -> usize {
        self.key_state.len()
    }

    /// Get the number of active invocations.
    pub fn active_count(&self) -> u64 {
        self.stats.active_invocations
    }

    /// Get the number of awakeables.
    pub fn awakeable_count(&self) -> usize {
        self.awakeables.len()
    }
}

/// Errors from virtual object operations.
#[derive(Debug, Clone)]
pub enum VirtualObjectError {
    UnknownObjectType(String),
    UnknownHandler(String),
    UnknownInvocation(u64),
    UnknownKey(String),
    UnknownJournalEntry(u32),
    UnknownAwakeable(String),
    AlreadyResolved(String),
    InvalidStateTransition(String),
}

impl std::fmt::Display for VirtualObjectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownObjectType(t) => write!(f, "unknown object type: {}", t),
            Self::UnknownHandler(h) => write!(f, "unknown handler: {}", h),
            Self::UnknownInvocation(id) => write!(f, "unknown invocation: {}", id),
            Self::UnknownKey(k) => write!(f, "unknown key: {}", k),
            Self::UnknownJournalEntry(s) => write!(f, "unknown journal entry: {}", s),
            Self::UnknownAwakeable(id) => write!(f, "unknown awakeable: {}", id),
            Self::AlreadyResolved(id) => write!(f, "awakeable already resolved: {}", id),
            Self::InvalidStateTransition(msg) => write!(f, "invalid state transition: {}", msg),
        }
    }
}

impl std::error::Error for VirtualObjectError {}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_runtime() -> VirtualObjectRuntime {
        let mut rt = VirtualObjectRuntime::new();
        rt.register_object_type(
            "ChatAgent",
            vec![
                HandlerDefinition {
                    name: "message".to_string(),
                    handler_kind: HandlerKind::Workflow,
                    input_schema: None,
                    output_schema: None,
                },
                HandlerDefinition {
                    name: "get_history".to_string(),
                    handler_kind: HandlerKind::Shared,
                    input_schema: None,
                    output_schema: None,
                },
            ],
        );
        rt.register_object_type(
            "ShoppingCart",
            vec![
                HandlerDefinition {
                    name: "add_item".to_string(),
                    handler_kind: HandlerKind::Service,
                    input_schema: None,
                    output_schema: None,
                },
                HandlerDefinition {
                    name: "checkout".to_string(),
                    handler_kind: HandlerKind::Workflow,
                    input_schema: None,
                    output_schema: None,
                },
            ],
        );
        rt
    }

    #[test]
    fn test_register_object_types() {
        let rt = test_runtime();
        assert_eq!(rt.object_type_count(), 2);
        assert!(rt.get_handlers("ChatAgent").is_some());
        assert!(rt.get_handlers("ShoppingCart").is_some());
        assert!(rt.get_handlers("Unknown").is_none());
    }

    #[test]
    fn test_invoke_handler() {
        let mut rt = test_runtime();
        let id = rt
            .invoke("ChatAgent", "session-1", "message", b"hello".to_vec(), None)
            .unwrap();
        assert_eq!(id, 1);

        let inv = rt.get_invocation(id).unwrap();
        assert_eq!(inv.state, HandlerState::Running); // First on this key, runs immediately
        assert_eq!(inv.handler_name, "message");
    }

    #[test]
    fn test_single_writer_serialization() {
        let mut rt = test_runtime();

        // First invocation runs immediately
        let id1 = rt
            .invoke("ChatAgent", "session-1", "message", b"msg1".to_vec(), None)
            .unwrap();
        assert_eq!(rt.get_invocation(id1).unwrap().state, HandlerState::Running);

        // Second invocation on same key is queued
        let id2 = rt
            .invoke("ChatAgent", "session-1", "message", b"msg2".to_vec(), None)
            .unwrap();
        assert_eq!(rt.get_invocation(id2).unwrap().state, HandlerState::Queued);
        assert_eq!(rt.key_queue_depth("ChatAgent", "session-1"), 1);

        // Invocation on different key runs in parallel
        let id3 = rt
            .invoke("ChatAgent", "session-2", "message", b"msg3".to_vec(), None)
            .unwrap();
        assert_eq!(rt.get_invocation(id3).unwrap().state, HandlerState::Running);
    }

    #[test]
    fn test_complete_dispatches_next() {
        let mut rt = test_runtime();

        let id1 = rt
            .invoke("ChatAgent", "session-1", "message", b"msg1".to_vec(), None)
            .unwrap();
        let id2 = rt
            .invoke("ChatAgent", "session-1", "message", b"msg2".to_vec(), None)
            .unwrap();

        // Complete first — second should start
        rt.complete_invocation(id1, b"reply1".to_vec()).unwrap();
        assert_eq!(
            rt.get_invocation(id1).unwrap().state,
            HandlerState::Completed
        );
        assert_eq!(rt.get_invocation(id2).unwrap().state, HandlerState::Running);
    }

    #[test]
    fn test_state_operations() {
        let mut rt = test_runtime();
        let _id = rt
            .invoke("ChatAgent", "session-1", "message", b"hello".to_vec(), None)
            .unwrap();

        // State starts empty
        assert!(rt.state_get("ChatAgent", "session-1", "history").is_none());

        // Set state
        rt.state_set("ChatAgent", "session-1", "history", b"[msg1,msg2]".to_vec())
            .unwrap();
        assert_eq!(
            rt.state_get("ChatAgent", "session-1", "history").unwrap(),
            b"[msg1,msg2]"
        );

        // Clear state
        rt.state_clear("ChatAgent", "session-1", "history").unwrap();
        assert!(rt.state_get("ChatAgent", "session-1", "history").is_none());
    }

    #[test]
    fn test_state_isolation_between_keys() {
        let mut rt = test_runtime();
        let _id1 = rt
            .invoke("ChatAgent", "session-1", "message", b"a".to_vec(), None)
            .unwrap();
        let _id2 = rt
            .invoke("ChatAgent", "session-2", "message", b"b".to_vec(), None)
            .unwrap();

        rt.state_set("ChatAgent", "session-1", "history", b"history-1".to_vec())
            .unwrap();
        rt.state_set("ChatAgent", "session-2", "history", b"history-2".to_vec())
            .unwrap();

        assert_eq!(
            rt.state_get("ChatAgent", "session-1", "history").unwrap(),
            b"history-1"
        );
        assert_eq!(
            rt.state_get("ChatAgent", "session-2", "history").unwrap(),
            b"history-2"
        );
    }

    #[test]
    fn test_idempotency() {
        let mut rt = test_runtime();

        let id1 = rt
            .invoke(
                "ChatAgent",
                "session-1",
                "message",
                b"hello".to_vec(),
                Some("idem-1".to_string()),
            )
            .unwrap();
        let id2 = rt
            .invoke(
                "ChatAgent",
                "session-1",
                "message",
                b"hello".to_vec(),
                Some("idem-1".to_string()),
            )
            .unwrap();

        // Same idempotency key returns same invocation
        assert_eq!(id1, id2);
        assert_eq!(rt.stats().total_invocations, 1);
    }

    #[test]
    fn test_journal_operations() {
        let mut rt = test_runtime();
        let id = rt
            .invoke("ChatAgent", "session-1", "message", b"hello".to_vec(), None)
            .unwrap();

        // Append journal entries
        let seq0 = rt
            .append_journal(id, JournalEntryType::DurableStep, b"step1".to_vec())
            .unwrap();
        let seq1 = rt
            .append_journal(
                id,
                JournalEntryType::StateGet {
                    state_key: "history".to_string(),
                },
                vec![],
            )
            .unwrap();

        assert_eq!(seq0, 0);
        assert_eq!(seq1, 1);

        // Complete journal entries
        rt.complete_journal(id, seq0, b"result1".to_vec()).unwrap();
        rt.complete_journal(id, seq1, b"[]".to_vec()).unwrap();

        let inv = rt.get_invocation(id).unwrap();
        assert_eq!(inv.journal.len(), 2);
        assert!(inv.journal[0].completed);
        assert!(inv.journal[1].completed);
    }

    #[test]
    fn test_suspend_and_resume() {
        let mut rt = test_runtime();
        let id = rt
            .invoke("ChatAgent", "session-1", "message", b"hello".to_vec(), None)
            .unwrap();

        rt.suspend_invocation(id).unwrap();
        assert_eq!(
            rt.get_invocation(id).unwrap().state,
            HandlerState::Suspended
        );
        assert_eq!(rt.stats().suspended_invocations, 1);

        rt.resume_invocation(id).unwrap();
        assert_eq!(rt.get_invocation(id).unwrap().state, HandlerState::Running);
        assert_eq!(rt.stats().suspended_invocations, 0);
    }

    #[test]
    fn test_awakeable_lifecycle() {
        let mut rt = test_runtime();
        let id = rt
            .invoke("ChatAgent", "session-1", "message", b"hello".to_vec(), None)
            .unwrap();

        // Create awakeable
        let awk_id = rt.create_awakeable(id).unwrap();
        assert_eq!(rt.awakeable_count(), 1);

        // Suspend invocation waiting for awakeable
        rt.suspend_invocation(id).unwrap();

        // Resolve awakeable (external system)
        let owner = rt.resolve_awakeable(&awk_id, b"approved".to_vec()).unwrap();
        assert_eq!(owner, id);
        assert_eq!(rt.get_invocation(id).unwrap().state, HandlerState::Running);
        // Resumed!
    }

    #[test]
    fn test_awakeable_rejection() {
        let mut rt = test_runtime();
        let id = rt
            .invoke("ChatAgent", "session-1", "message", b"hello".to_vec(), None)
            .unwrap();

        let awk_id = rt.create_awakeable(id).unwrap();
        rt.suspend_invocation(id).unwrap();

        let owner = rt.reject_awakeable(&awk_id, "timeout".to_string()).unwrap();
        assert_eq!(owner, id);
        assert_eq!(rt.get_invocation(id).unwrap().state, HandlerState::Running);
    }

    #[test]
    fn test_unknown_object_type() {
        let mut rt = test_runtime();
        let result = rt.invoke("Unknown", "key", "handler", vec![], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_unknown_handler() {
        let mut rt = test_runtime();
        let result = rt.invoke("ChatAgent", "session-1", "unknown_handler", vec![], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_fail_dispatches_next() {
        let mut rt = test_runtime();

        let id1 = rt
            .invoke("ChatAgent", "session-1", "message", b"msg1".to_vec(), None)
            .unwrap();
        let id2 = rt
            .invoke("ChatAgent", "session-1", "message", b"msg2".to_vec(), None)
            .unwrap();

        // Fail first — second should start
        rt.fail_invocation(id1, "error".to_string()).unwrap();
        assert_eq!(rt.get_invocation(id1).unwrap().state, HandlerState::Failed);
        assert_eq!(rt.get_invocation(id2).unwrap().state, HandlerState::Running);
    }

    #[test]
    fn test_parallel_across_different_object_types() {
        let mut rt = test_runtime();

        // Different object types run in parallel
        let id1 = rt
            .invoke("ChatAgent", "session-1", "message", b"a".to_vec(), None)
            .unwrap();
        let id2 = rt
            .invoke("ShoppingCart", "cart-1", "add_item", b"item".to_vec(), None)
            .unwrap();

        assert_eq!(rt.get_invocation(id1).unwrap().state, HandlerState::Running);
        assert_eq!(rt.get_invocation(id2).unwrap().state, HandlerState::Running);
    }

    #[test]
    fn test_stats() {
        let mut rt = test_runtime();

        let id1 = rt
            .invoke("ChatAgent", "s1", "message", b"a".to_vec(), None)
            .unwrap();
        let _id2 = rt
            .invoke("ChatAgent", "s1", "message", b"b".to_vec(), None)
            .unwrap();
        let _id3 = rt
            .invoke("ChatAgent", "s2", "message", b"c".to_vec(), None)
            .unwrap();

        rt.complete_invocation(id1, b"done".to_vec()).unwrap();

        let stats = rt.stats();
        assert_eq!(stats.total_invocations, 3);
        assert_eq!(stats.completed_invocations, 1);
        assert_eq!(stats.active_invocations, 2);
    }
}
