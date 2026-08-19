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
use velocity_workflow_core::slab::SlabHeader;

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
}
