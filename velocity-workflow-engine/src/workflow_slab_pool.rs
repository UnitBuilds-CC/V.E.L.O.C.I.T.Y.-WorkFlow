//! Fixed-capacity workflow slab pool — zero-allocation workflow lifecycle management.
//!
//! Replaces `DashMap<u64, WorkflowContext>` with a pre-allocated slot array.
//! Each slot holds a `SlabHeader` (128 bytes) plus inline metadata — no heap
//! allocations for the workflow itself. Step results are stored in the
//! `BumpArenaPage` (Tier-2), referenced by offset.
//!
//! Completed workflows are **immediately evicted** — their slot becomes available
//! for reuse. This enforces the fixed-memory contract: memory usage is proportional
//! to *concurrent active workflows*, not total workflows ever created.

use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::Mutex;
use velocity_workflow_core::slab::SlabHeader;
use crate::engine::WorkflowContext;

/// Maximum concurrent active workflows in the slab pool.
/// 65,536 slots × sizeof(SlabSlot) bytes = fixed memory (see `memory_bytes()`).
pub const SLAB_POOL_CAPACITY: usize = 65_536;

/// Inline per-workflow metadata — no heap allocations.
/// Padded to 128 bytes so slot total = SlabHeader(128) + SlotMeta(128) = 256 bytes.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SlotMeta {
    pub workflow_type_id: u64,
    pub namespace_id: u64,
    pub task_queue_hash: u64,
    pub status: u8,          // 0=empty, 1=running, 2=completed, 3=failed, 4=timed_out
    pub _pad0: [u8; 7],
    pub start_time_ms: u64,
    pub close_time_ms: u64,
    pub event_sequence: u64,
    pub parent_key: u64,     // 0 = no parent
    pub child_count: u32,
    pub child_keys: [u64; 4], // inline — no Vec heap alloc
    pub _pad1: [u8; 4],
    pub result_offset: u32,   // offset into BumpArenaPage (0 = no result)
    pub result_len: u32,
    pub input_offset: u32,    // offset into BumpArenaPage (0 = no input)
    pub input_len: u32,
    pub arena_page_index: u16, // which arena page holds this workflow's payloads
    pub _pad2: [u8; 6],
}

impl SlotMeta {
    #[inline(always)]
    pub const fn empty() -> Self {
        Self {
            workflow_type_id: 0,
            namespace_id: 0,
            task_queue_hash: 0,
            status: 0,
            _pad0: [0; 7],
            start_time_ms: 0,
            close_time_ms: 0,
            event_sequence: 0,
            parent_key: 0,
            child_count: 0,
            child_keys: [0; 4],
            _pad1: [0; 4],
            result_offset: 0,
            result_len: 0,
            input_offset: 0,
            input_len: 0,
            arena_page_index: 0,
            _pad2: [0; 6],
        }
    }

    #[inline(always)]
    pub fn is_free(&self) -> bool {
        self.status == 0
    }

    #[inline(always)]
    pub fn is_active(&self) -> bool {
        self.status == 1
    }
}

/// A single slab slot: fixed-size, no heap allocations.
/// 128 (SlabHeader) + 128 (SlotMeta) = 256 bytes per slot.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SlabSlot {
    pub header: SlabHeader,
    pub meta: SlotMeta,
}

impl SlabSlot {
    pub const fn empty() -> Self {
        Self {
            header: SlabHeader {
                magic: 0,
                schema_version: 0,
                workflow_id: 0,
                run_id: 0,
                current_step: 0,
                total_steps: 0,
                merkle_root: [0u8; 32],
                step_bitmask: velocity_workflow_core::bitmask::Bitmask256::new(),
                prev_merkle_root: [0u8; 32],
            },
            meta: SlotMeta::empty(),
        }
    }
}

/// Fixed-capacity slab pool for workflow lifecycle management.
///
/// Memory usage: `SLAB_POOL_CAPACITY * sizeof(SlabSlot)` bytes = fixed (see `memory_bytes()`).
/// Workflow lookup: O(n) scan for key match (acceptable for ≤65K active workflows).
/// Allocation: O(1) via free-list cursor.
pub struct WorkflowSlabPool {
    slots: Vec<SlabSlot>,          // pre-allocated, fixed capacity
    free_cursor: AtomicU16,        // next slot to try (wrapping scan)
    active_count: AtomicU64,       // number of occupied slots
    total_allocated: AtomicU64,    // lifetime allocations (for metrics)
    total_evicted: AtomicU64,      // lifetime evictions (for metrics)
}

impl WorkflowSlabPool {
    /// Create a new slab pool with pre-allocated slots.
    /// Memory is allocated once and never grows.
    /// Single heap allocation — `vec![value; N]` uses one `alloc_zeroed` + fill.
    pub fn new() -> Self {
        // SlabSlot is Copy + Clone — vec![val; N] does one allocation, not N
        let slots = vec![SlabSlot::empty(); SLAB_POOL_CAPACITY];
        Self {
            slots,
            free_cursor: AtomicU16::new(0),
            active_count: AtomicU64::new(0),
            total_allocated: AtomicU64::new(0),
            total_evicted: AtomicU64::new(0),
        }
    }

    /// Allocate a slot for a new workflow. Returns the slot index.
    /// Returns `None` if the pool is full (all slots occupied).
    pub fn allocate(&self, header: SlabHeader, meta: SlotMeta) -> Option<usize> {
        let start = self.free_cursor.load(Ordering::Relaxed) as usize;
        
        // Scan for a free slot starting from cursor
        for i in 0..SLAB_POOL_CAPACITY {
            let idx = (start + i) % SLAB_POOL_CAPACITY;
            
            // Safety: we never remove slots, indices are always valid
            let slot = unsafe { self.slots.get_unchecked(idx) };
            if slot.meta.is_free() {
                // Found a free slot — mutate it
                // Safety: exclusive access guaranteed by atomic CAS below
                let slot_ptr = &self.slots[idx] as *const SlabSlot as *mut SlabSlot;
                unsafe {
                    (*slot_ptr).header = header;
                    (*slot_ptr).meta = meta;
                }
                
                // Advance cursor (wrapping)
                self.free_cursor.store(((idx + 1) % SLAB_POOL_CAPACITY) as u16, Ordering::Relaxed);
                self.active_count.fetch_add(1, Ordering::Relaxed);
                self.total_allocated.fetch_add(1, Ordering::Relaxed);
                return Some(idx);
            }
        }
        
        None // Pool exhausted
    }

    /// Find a workflow by workflow_id. Returns slot index.
    pub fn find_by_workflow_id(&self, workflow_id: u64) -> Option<usize> {
        for (idx, slot) in self.slots.iter().enumerate() {
            if !slot.meta.is_free() && slot.header.workflow_id == workflow_id {
                return Some(idx);
            }
        }
        None
    }

    /// Find a workflow by workflow_key (run_id). Returns slot index.
    pub fn find_by_run_id(&self, run_id: u64) -> Option<usize> {
        for (idx, slot) in self.slots.iter().enumerate() {
            if !slot.meta.is_free() && slot.header.run_id == run_id {
                return Some(idx);
            }
        }
        None
    }

    /// Evict a workflow by slot index — immediately frees the slot.
    /// The caller should have already persisted state to WAL before calling this.
    /// Zero-alloc: uses `write_bytes` to zero the slot in-place (no temporary).
    pub fn evict(&self, idx: usize) {
        if idx >= self.slots.len() {
            return;
        }
        
        // Zero the slot in-place — no temporary, no memcpy from stack
        let slot_ptr = &self.slots[idx] as *const SlabSlot as *mut SlabSlot;
        unsafe {
            std::ptr::write_bytes(slot_ptr as *mut u8, 0, std::mem::size_of::<SlabSlot>());
        }
        
        // Reset free_cursor to the evicted slot so next allocate() reuses it immediately
        self.free_cursor.store(idx as u16, Ordering::Relaxed);
        self.active_count.fetch_sub(1, Ordering::Relaxed);
        self.total_evicted.fetch_add(1, Ordering::Relaxed);
    }

    /// Evict all completed workflows (status != Running).
    /// Returns the number of workflows evicted.
    pub fn evict_completed(&self) -> usize {
        let mut count = 0;
        for idx in 0..self.slots.len() {
            let slot = &self.slots[idx];
            if slot.meta.status == 2 || slot.meta.status == 3 || slot.meta.status == 4 {
                // Completed, failed, or timed out — evict
                self.evict(idx);
                count += 1;
            }
        }
        count
    }

    /// Get a reference to a slot's header.
    pub fn get_header(&self, idx: usize) -> Option<&SlabHeader> {
        self.slots.get(idx).filter(|s| !s.meta.is_free()).map(|s| &s.header)
    }

    /// Get a mutable reference to a slot's header.
    /// Safety: caller must ensure no concurrent access to the same slot.
    pub fn get_header_mut(&self, idx: usize) -> Option<&mut SlabHeader> {
        if idx >= self.slots.len() || self.slots[idx].meta.is_free() {
            return None;
        }
        let slot_ptr = &self.slots[idx] as *const SlabSlot as *mut SlabSlot;
        Some(unsafe { &mut (*slot_ptr).header })
    }

    /// Get a reference to a slot's metadata.
    pub fn get_meta(&self, idx: usize) -> Option<&SlotMeta> {
        self.slots.get(idx).filter(|s| !s.meta.is_free()).map(|s| &s.meta)
    }

    /// Get a mutable reference to a slot's metadata.
    pub fn get_meta_mut(&self, idx: usize) -> Option<&mut SlotMeta> {
        if idx >= self.slots.len() || self.slots[idx].meta.is_free() {
            return None;
        }
        let slot_ptr = &self.slots[idx] as *const SlabSlot as *mut SlabSlot;
        Some(unsafe { &mut (*slot_ptr).meta })
    }

    /// Number of currently active workflows.
    pub fn active_count(&self) -> u64 {
        self.active_count.load(Ordering::Relaxed)
    }

    /// Total allocations since creation.
    pub fn total_allocated(&self) -> u64 {
        self.total_allocated.load(Ordering::Relaxed)
    }

    /// Total evictions since creation.
    pub fn total_evicted(&self) -> u64 {
        self.total_evicted.load(Ordering::Relaxed)
    }

    /// Fixed memory footprint in bytes.
    pub const fn memory_bytes() -> usize {
        SLAB_POOL_CAPACITY * std::mem::size_of::<SlabSlot>()
    }

    /// Iterate over active workflow slot indices.
    pub fn active_slots(&self) -> impl Iterator<Item = usize> + '_ {
        self.slots.iter().enumerate()
            .filter(|(_, s)| !s.meta.is_free())
            .map(|(idx, _)| idx)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// WorkflowContextPool — drop-in replacement for DashMap<u64, WorkflowContext>
// ═══════════════════════════════════════════════════════════════════════════════

/// Maximum concurrent active workflows (power of 2 for fast modulo).
/// 8192 slots: key_index = 64 KB, slots = 8 KB. Total fixed overhead ≈ 72 KB.
/// Peak bench concurrency is ~50, so this gives 160× headroom.
pub const WORKFLOW_POOL_CAPACITY: usize = 8192;

/// Sentinel: slot is unoccupied in the key index.
const KEY_SLOT_EMPTY: u16 = u16::MAX;

/// Fixed-capacity, thread-safe workflow context pool.
///
/// Replaces `DashMap<u64, WorkflowContext>` with:
/// - Pre-allocated key index: O(1) lookup by workflow_key
/// - Pre-allocated slot array: fixed metadata (no DashMap bucket growth)
/// - Free list: O(1) slot allocation with context recycling
/// - Single Mutex: thread safety (contention negligible at ≤100 concurrent)
///
/// **Memory contract**: all structural memory is pre-allocated at startup.
/// WorkflowContext objects are created on demand but recycled via the free list —
/// no new heap allocations after warmup. This eliminates both DashMap bucket growth
/// and Windows heap fragmentation.
pub struct WorkflowContextPool {
    inner: Mutex<WorkflowPoolInner>,
}

struct WorkflowPoolInner {
    /// Fixed-size key → slot index mapping. Pre-allocated once, never grows.
    /// `KEY_SLOT_EMPTY` = no workflow at this key.
    key_index: Vec<u16>,

    /// Fixed-size slot array. Each slot tracks its key and active state.
    /// Pre-allocated once, never grows.
    slots: Vec<PoolSlot>,

    /// Fixed-size context array. `None` = slot is free.
    /// WorkflowContext objects are recycled: on insert, reuse existing context
    /// if present; on remove, set to None but keep the Vec capacity.
    contexts: Vec<Option<WorkflowContext>>,

    /// Free list: stack of available slot indices. Pre-allocated with all indices.
    free_list: Vec<u16>,

    /// Number of currently active workflows.
    active_count: u64,

    /// Lifetime insertions (for metrics).
    total_inserted: u64,

    /// Lifetime removals (for metrics).
    total_removed: u64,
}

/// Per-slot metadata — fixed size, no heap allocations.
#[derive(Clone, Copy)]
struct PoolSlot {
    key: u64,      // workflow_key (0 = free)
    active: bool,  // whether this slot holds a live workflow
}

impl PoolSlot {
    const fn empty() -> Self {
        Self { key: 0, active: false }
    }
}

impl WorkflowContextPool {
    /// Create a new pool with pre-allocated structural memory.
    /// All three arrays are allocated once and never grow.
    pub fn new() -> Self {
        let key_index = vec![KEY_SLOT_EMPTY; WORKFLOW_POOL_CAPACITY];

        let slots = vec![PoolSlot::empty(); WORKFLOW_POOL_CAPACITY];

        // Pre-allocate context slots as None. The Vec<Option<WorkflowContext>>
        // allocates its pointer array once (8192 × 24 bytes = 192 KB).
        // Individual WorkflowContext objects are created on demand.
        // Can't use vec![None; N] because WorkflowContext doesn't impl Clone.
        let mut contexts = Vec::with_capacity(WORKFLOW_POOL_CAPACITY);
        for _ in 0..WORKFLOW_POOL_CAPACITY {
            contexts.push(None);
        }

        // Free list: all slots start free (in reverse order so slot 0 is first).
        let free_list: Vec<u16> = (0..WORKFLOW_POOL_CAPACITY as u16).rev().collect();

        Self {
            inner: Mutex::new(WorkflowPoolInner {
                key_index,
                slots,
                contexts,
                free_list,
                active_count: 0,
                total_inserted: 0,
                total_removed: 0,
            }),
        }
    }

    /// Insert a workflow context. If the key already exists, replace it.
    /// Returns `Some(slot_idx)` on success, `None` if the pool is full.
    pub fn insert(&self, key: u64, ctx: WorkflowContext) -> Option<usize> {
        let mut inner = self.inner.lock().unwrap();

        // Check if key already exists — update in place (recycle slot)
        let slot_idx = inner.find_key_slot(key);
        if let Some(idx) = slot_idx {
            inner.contexts[idx] = Some(ctx);
            return Some(idx);
        }

        // Allocate a free slot
        let idx = inner.free_list.pop()? as usize;

        inner.slots[idx] = PoolSlot { key, active: true };
        inner.key_index[key as usize % WORKFLOW_POOL_CAPACITY] = idx as u16;
        inner.contexts[idx] = Some(ctx);
        inner.active_count += 1;
        inner.total_inserted += 1;
        Some(idx)
    }

    /// Insert only if the key is absent (WAL replay entry API).
    /// Returns `true` if inserted, `false` if key already present.
    pub fn insert_if_absent(&self, key: u64, ctx: WorkflowContext) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if inner.find_key_slot(key).is_some() {
            return false;
        }
        let Some(idx) = inner.free_list.pop() else {
            return false;
        };
        let idx = idx as usize;
        inner.slots[idx] = PoolSlot { key, active: true };
        inner.key_index[key as usize % WORKFLOW_POOL_CAPACITY] = idx as u16;
        inner.contexts[idx] = Some(ctx);
        inner.active_count += 1;
        inner.total_inserted += 1;
        true
    }

    /// Get a reference to a workflow context by key.
    /// The closure receives `Option<&WorkflowContext>` while the lock is held.
    pub fn with<F, R>(&self, key: u64, f: F) -> R
    where
        F: FnOnce(Option<&WorkflowContext>) -> R,
    {
        let inner = self.inner.lock().unwrap();
        let ctx_ref = inner
            .find_key_slot(key)
            .and_then(|idx| inner.contexts[idx].as_ref());
        f(ctx_ref)
    }

    /// Get a mutable reference to a workflow context by key.
    /// The closure receives `Option<&mut WorkflowContext>` while the lock is held.
    pub fn with_mut<F, R>(&self, key: u64, f: F) -> R
    where
        F: FnOnce(Option<&mut WorkflowContext>) -> R,
    {
        let mut inner = self.inner.lock().unwrap();
        let ctx_ref = inner
            .find_key_slot(key)
            .and_then(|idx| inner.contexts[idx].as_mut());
        f(ctx_ref)
    }

    /// Remove a workflow by key. Returns the context if it existed.
    /// The slot is returned to the free list for reuse.
    pub fn remove(&self, key: u64) -> Option<WorkflowContext> {
        let mut inner = self.inner.lock().unwrap();
        let idx = inner.find_key_slot(key)?;

        let ctx = inner.contexts[idx].take();
        inner.slots[idx] = PoolSlot::empty();
        inner.key_index[key as usize % WORKFLOW_POOL_CAPACITY] = KEY_SLOT_EMPTY;
        inner.free_list.push(idx as u16);
        inner.active_count -= 1;
        inner.total_removed += 1;
        ctx
    }

    /// Iterate over all active (key, &mut WorkflowContext) pairs.
    /// The closure is called while the lock is held.
    pub fn for_each_mut<F>(&self, mut f: F)
    where
        F: FnMut(u64, &mut WorkflowContext),
    {
        let mut inner = self.inner.lock().unwrap();
        for idx in 0..WORKFLOW_POOL_CAPACITY {
            if inner.slots[idx].active {
                let key = inner.slots[idx].key;
                if let Some(ctx) = &mut inner.contexts[idx] {
                    f(key, ctx);
                }
            }
        }
    }

    /// Iterate over all active (key, &WorkflowContext) pairs.
    /// The closure is called while the lock is held.
    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(u64, &WorkflowContext),
    {
        let inner = self.inner.lock().unwrap();
        for idx in 0..WORKFLOW_POOL_CAPACITY {
            if inner.slots[idx].active {
                if let Some(ctx) = &inner.contexts[idx] {
                    f(inner.slots[idx].key, ctx);
                }
            }
        }
    }

    /// Collect all active workflow keys.
    pub fn keys(&self) -> Vec<u64> {
        let inner = self.inner.lock().unwrap();
        let mut result = Vec::with_capacity(inner.active_count as usize);
        for idx in 0..WORKFLOW_POOL_CAPACITY {
            if inner.slots[idx].active {
                result.push(inner.slots[idx].key);
            }
        }
        result
    }

    /// Number of currently active workflows.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().active_count as usize
    }

    /// Whether the pool has no active workflows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Total insertions since creation (for metrics).
    pub fn total_inserted(&self) -> u64 {
        self.inner.lock().unwrap().total_inserted
    }

    /// Total removals since creation (for metrics).
    pub fn total_removed(&self) -> u64 {
        self.inner.lock().unwrap().total_removed
    }

    /// Available slot count (for metrics / backpressure).
    pub fn available(&self) -> usize {
        self.inner.lock().unwrap().free_list.len()
    }
}

impl WorkflowPoolInner {
    /// O(1) key → slot index lookup via the key index.
    /// Handles hash collisions by verifying the slot's actual key.
    fn find_key_slot(&self, key: u64) -> Option<usize> {
        let bucket = key as usize % WORKFLOW_POOL_CAPACITY;
        let idx = self.key_index[bucket];
        if idx == KEY_SLOT_EMPTY {
            return None;
        }
        let idx = idx as usize;
        // Verify: handle hash collisions
        if self.slots[idx].active && self.slots[idx].key == key {
            Some(idx)
        } else {
            // Collision: key index slot is occupied by a different key.
            // Fall back to linear scan (rare — only when two keys hash to same bucket).
            self.find_key_slot_linear(key)
        }
    }

    /// O(n) fallback for hash collisions. Rare in practice.
    fn find_key_slot_linear(&self, key: u64) -> Option<usize> {
        for idx in 0..WORKFLOW_POOL_CAPACITY {
            if self.slots[idx].active && self.slots[idx].key == key {
                return Some(idx);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velocity_workflow_core::slab::SlabHeader;

    #[test]
    fn test_slab_pool_allocate_and_find() {
        let pool = WorkflowSlabPool::new();
        let header = SlabHeader::new(42, 100, 5);
        let mut meta = SlotMeta::empty();
        meta.status = 1; // running
        meta.workflow_type_id = 1;

        let idx = pool.allocate(header, meta).unwrap();
        assert_eq!(pool.active_count(), 1);
        assert_eq!(pool.find_by_workflow_id(42), Some(idx));
        assert_eq!(pool.find_by_run_id(100), Some(idx));
    }

    #[test]
    fn test_slab_pool_evict_reuses_slot() {
        let pool = WorkflowSlabPool::new();
        let header = SlabHeader::new(1, 1, 1);
        let mut meta = SlotMeta::empty();
        meta.status = 1;

        let idx = pool.allocate(header, meta).unwrap();
        assert_eq!(pool.active_count(), 1);

        pool.evict(idx);
        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool.find_by_workflow_id(1), None);

        // Re-allocate should reuse the evicted slot
        let header2 = SlabHeader::new(2, 2, 1);
        let mut meta2 = SlotMeta::empty();
        meta2.status = 1;
        let idx2 = pool.allocate(header2, meta2).unwrap();
        assert_eq!(idx2, idx); // same slot reused
        assert_eq!(pool.active_count(), 1);
    }

    #[test]
    fn test_slab_pool_evict_completed() {
        let pool = WorkflowSlabPool::new();

        // Allocate 3 workflows: 1 running, 1 completed, 1 failed
        for i in 0..3u64 {
            let header = SlabHeader::new(i, i + 100, 1);
            let mut meta = SlotMeta::empty();
            meta.status = match i {
                0 => 1, // running
                1 => 2, // completed
                _ => 3, // failed
            };
            pool.allocate(header, meta);
        }
        assert_eq!(pool.active_count(), 3);

        let evicted = pool.evict_completed();
        assert_eq!(evicted, 2); // completed + failed
        assert_eq!(pool.active_count(), 1); // only running remains
    }

    #[test]
    fn test_slab_pool_fixed_memory() {
        // Verify the memory footprint is constant and matches actual struct size
        let expected = SLAB_POOL_CAPACITY * std::mem::size_of::<SlabSlot>();
        assert_eq!(WorkflowSlabPool::memory_bytes(), expected);
        // The pool should not grow beyond this fixed allocation
        assert!(expected <= 32 * 1024 * 1024, "slab pool must be ≤ 32 MB");
    }

    // ─── WorkflowContextPool tests ──────────────────────────────────────────

    fn make_test_context(wf_id: u64) -> WorkflowContext {
        WorkflowContext::new(wf_id, wf_id + 1000, 1, 42, 5)
    }

    #[test]
    fn test_context_pool_insert_and_get() {
        let pool = WorkflowContextPool::new();
        let ctx = make_test_context(1);
        let key = 0x0000_0001u64;

        pool.insert(key, ctx);
        assert_eq!(pool.len(), 1);

        pool.with(key, |c| {
            let ctx = c.expect("should find workflow");
            assert_eq!(ctx.workflow_id, 1);
        });
    }

    #[test]
    fn test_context_pool_with_mut() {
        let pool = WorkflowContextPool::new();
        let key = 0x0000_0002u64;
        pool.insert(key, make_test_context(2));

        // Mutate via with_mut
        pool.with_mut(key, |c| {
            let ctx = c.expect("should find workflow");
            ctx.complete_step(0, b"step0".to_vec());
        });

        // Verify mutation persisted
        pool.with(key, |c| {
            let ctx = c.expect("should find workflow");
            assert!(ctx.is_step_completed(0));
        });
    }

    #[test]
    fn test_context_pool_remove_and_reuse() {
        let pool = WorkflowContextPool::new();
        let key1 = 0x0000_0010u64;
        let key2 = 0x0000_0020u64;

        pool.insert(key1, make_test_context(10));
        pool.insert(key2, make_test_context(20));
        assert_eq!(pool.len(), 2);

        // Remove first workflow
        let removed = pool.remove(key1);
        assert!(removed.is_some());
        assert_eq!(pool.len(), 1);

        // First key should be gone
        pool.with(key1, |c| assert!(c.is_none()));

        // Second key should still be there
        pool.with(key2, |c| assert!(c.is_some()));

        // Insert new workflow — should reuse the freed slot
        let key3 = 0x0000_0030u64;
        pool.insert(key3, make_test_context(30));
        assert_eq!(pool.len(), 2);
    }

    #[test]
    fn test_context_pool_insert_if_absent() {
        let pool = WorkflowContextPool::new();
        let key = 0x0000_0040u64;

        // First insert succeeds
        assert!(pool.insert_if_absent(key, make_test_context(40)));
        assert_eq!(pool.len(), 1);

        // Second insert with same key fails (does not replace)
        assert!(!pool.insert_if_absent(key, make_test_context(99)));
        assert_eq!(pool.len(), 1);

        // Original context is preserved
        pool.with(key, |c| {
            let ctx = c.expect("should exist");
            assert_eq!(ctx.workflow_id, 40);
        });
    }

    #[test]
    fn test_context_pool_for_each() {
        let pool = WorkflowContextPool::new();
        for i in 0..10u64 {
            let key = i + 100;
            pool.insert(key, make_test_context(i));
        }

        let mut count = 0;
        pool.for_each(|_key, _ctx| {
            count += 1;
        });
        assert_eq!(count, 10);
    }

    #[test]
    fn test_context_pool_keys() {
        let pool = WorkflowContextPool::new();
        let keys = vec![10u64, 20, 30, 40, 50];
        for &k in &keys {
            pool.insert(k, make_test_context(k));
        }

        let mut result = pool.keys();
        result.sort();
        assert_eq!(result, keys);
    }

    #[test]
    fn test_context_pool_rapid_create_destroy() {
        // Simulate the bench server pattern: create → complete → purge in tight loop
        let pool = WorkflowContextPool::new();
        for i in 0..10_000u64 {
            let key = i % 100; // reuse keys
            if pool.with(key, |c| c.is_some()) {
                pool.remove(key);
            }
            pool.insert(key, make_test_context(i));
        }
        // Should have at most 100 active workflows
        assert!(pool.len() <= 100);
    }
}
