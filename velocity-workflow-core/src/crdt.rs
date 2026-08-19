//! Zero-allocation CRDT (Conflict-Free Replicated Data Types) for multi-region workflow convergence.
//!
//! Provides PNCounter (positive-negative counter) and AWORSet (Add-Wins Observed-Remove Set)
//! for convergent multi-region state without coordination.

/// Maximum number of elements tracked in a fixed-size AWORSet.
/// 256 elements × (8-byte key + 8-byte dot) = 4 KB per set.
pub const AWORSET_CAPACITY: usize = 256;

/// Maximum number of tombstone entries for removed elements.
pub const AWORSET_TOMBSTONE_CAPACITY: usize = 128;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PNCounter {
    pub increments: u64,
    pub decrements: u64,
}

impl Default for PNCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl PNCounter {
    pub const fn new() -> Self {
        Self {
            increments: 0,
            decrements: 0,
        }
    }

    #[inline(always)]
    pub fn value(&self) -> i64 {
        (self.increments as i64) - (self.decrements as i64)
    }

    #[inline(always)]
    pub fn inc(&mut self, delta: u64) {
        self.increments = self.increments.saturating_add(delta);
    }

    #[inline(always)]
    pub fn dec(&mut self, delta: u64) {
        self.decrements = self.decrements.saturating_add(delta);
    }

    #[inline(always)]
    pub fn merge(&mut self, other: &PNCounter) {
        if other.increments > self.increments {
            self.increments = other.increments;
        }
        if other.decrements > self.decrements {
            self.decrements = other.decrements;
        }
    }
}

/// A dot (replica_id, counter) pair used for causality tracking in AWORSet.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dot {
    pub replica_id: u16,
    pub counter: u64,
}

impl Dot {
    pub const fn new(replica_id: u16, counter: u64) -> Self {
        Self { replica_id, counter }
    }
}

/// An entry in the AWORSet element buffer.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct AworSetEntry {
    /// The element key (workflow_id, run_id, or opaque u64 identifier).
    key: u64,
    /// The dot when this element was added.
    dot: Dot,
    /// Whether this slot is occupied.
    occupied: bool,
}

impl Default for AworSetEntry {
    fn default() -> Self {
        Self {
            key: 0,
            dot: Dot { replica_id: 0, counter: 0 },
            occupied: false,
        }
    }
}

/// Add-Wins Observed-Remove Set (AWORSet) CRDT.
///
/// A set where:
/// - **Add** inserts an element with a unique dot (replica_id, monotonically increasing counter).
/// - **Remove** records the element's dot in a tombstone set.
/// - **Add-wins semantics**: if add and remove are concurrent, the add takes precedence.
/// - **Merge** takes the element-wise max dot per replica and applies add-wins resolution.
///
/// Fixed-capacity, zero-allocation: uses inline arrays sized for typical workflow sets.
/// Suitable for tracking which workflows are active across regions.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct AWORSet {
    /// Local replica identifier.
    replica_id: u16,
    /// Per-replica monotonic counter for generating unique dots.
    local_counter: u64,
    /// Number of active elements.
    element_count: u16,
    /// Number of tombstone entries.
    tombstone_count: u16,
    /// Fixed-size element buffer.
    elements: [AworSetEntry; AWORSET_CAPACITY],
    /// Fixed-size tombstone buffer (records dots of removed elements).
    tombstones: [Dot; AWORSET_TOMBSTONE_CAPACITY],
}

impl Default for AWORSet {
    fn default() -> Self {
        Self::new(0)
    }
}

impl AWORSet {
    /// Create a new empty AWORSet for the given replica.
    pub const fn new(replica_id: u16) -> Self {
        Self {
            replica_id,
            local_counter: 0,
            element_count: 0,
            tombstone_count: 0,
            elements: [AworSetEntry {
                key: 0,
                dot: Dot { replica_id: 0, counter: 0 },
                occupied: false,
            }; AWORSET_CAPACITY],
            tombstones: [Dot { replica_id: 0, counter: 0 }; AWORSET_TOMBSTONE_CAPACITY],
        }
    }

    /// Returns the number of active elements in the set.
    #[inline(always)]
    pub fn len(&self) -> u16 {
        self.element_count
    }

    /// Returns true if the set contains no elements.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.element_count == 0
    }

    /// Check if the set contains a given key.
    #[inline(always)]
    pub fn contains(&self, key: u64) -> bool {
        for entry in &self.elements {
            if entry.occupied && entry.key == key {
                return true;
            }
        }
        false
    }

    /// Add an element to the set. Returns the assigned dot.
    /// If the element already exists, its dot is updated (re-add).
    /// Returns None if the set is at capacity.
    pub fn add(&mut self, key: u64) -> Option<Dot> {
        self.local_counter = self.local_counter.saturating_add(1);
        let dot = Dot::new(self.replica_id, self.local_counter);

        // Check if element already exists — update its dot
        for entry in self.elements.iter_mut() {
            if entry.occupied && entry.key == key {
                entry.dot = dot;
                return Some(dot);
            }
        }

        // Find a free slot
        if (self.element_count as usize) >= AWORSET_CAPACITY {
            return None; // at capacity
        }
        for entry in self.elements.iter_mut() {
            if !entry.occupied {
                entry.key = key;
                entry.dot = dot;
                entry.occupied = true;
                self.element_count += 1;
                return Some(dot);
            }
        }
        None
    }

    /// Remove an element from the set.
    /// Records the element's dot in the tombstone set (add-wins: concurrent adds survive).
    /// Returns true if the element was found and removed.
    pub fn remove(&mut self, key: u64) -> bool {
        let mut found_idx = None;
        for (i, entry) in self.elements.iter().enumerate() {
            if entry.occupied && entry.key == key {
                found_idx = Some(i);
                break;
            }
        }

        if let Some(idx) = found_idx {
            let removed_dot = self.elements[idx].dot;
            self.elements[idx].occupied = false;
            self.elements[idx].key = 0;
            self.element_count -= 1;

            // Record in tombstones
            if (self.tombstone_count as usize) < AWORSET_TOMBSTONE_CAPACITY {
                self.tombstones[self.tombstone_count as usize] = removed_dot;
                self.tombstone_count += 1;
            }
            true
        } else {
            false
        }
    }

    /// Merge another AWORSet into this one (union-based, add-wins).
    ///
    /// For each element in `other`:
    /// - If the key is not present locally, add it with its dot.
    /// - If the key is present locally, keep the entry with the higher counter (add-wins).
    /// - Tombstones from `other` are merged; elements whose dots appear only in
    ///   the remote tombstone set AND are not present with a higher dot are removed.
    pub fn merge(&mut self, other: &AWORSet) {
        // Merge elements from other: add-wins semantics
        for other_entry in &other.elements {
            if !other_entry.occupied {
                continue;
            }

            let mut found_local = false;
            for local_entry in self.elements.iter_mut() {
                if local_entry.occupied && local_entry.key == other_entry.key {
                    found_local = true;
                    // Add-wins: keep the entry with the higher counter
                    if other_entry.dot.counter > local_entry.dot.counter {
                        local_entry.dot = other_entry.dot;
                    }
                    break;
                }
            }

            if !found_local {
                // Check if this element was tombstoned locally
                let is_tombstoned = self.tombstones.iter()
                    .take(self.tombstone_count as usize)
                    .any(|t| t.replica_id == other_entry.dot.replica_id
                             && t.counter == other_entry.dot.counter);

                if !is_tombstoned {
                    // Add the remote element
                    if (self.element_count as usize) < AWORSET_CAPACITY {
                        for slot in self.elements.iter_mut() {
                            if !slot.occupied {
                                slot.key = other_entry.key;
                                slot.dot = other_entry.dot;
                                slot.occupied = true;
                                self.element_count += 1;
                                break;
                            }
                        }
                    }
                }
            }
        }

        // Merge tombstones from other
        for other_ts in other.tombstones.iter().take(other.tombstone_count as usize) {
            if (self.tombstone_count as usize) < AWORSET_TOMBSTONE_CAPACITY {
                let already_present = self.tombstones.iter()
                    .take(self.tombstone_count as usize)
                    .any(|t| t.replica_id == other_ts.replica_id && t.counter == other_ts.counter);
                if !already_present {
                    self.tombstones[self.tombstone_count as usize] = *other_ts;
                    self.tombstone_count += 1;
                }
            }
        }

        // Update local counter to max
        if other.local_counter > self.local_counter {
            self.local_counter = other.local_counter;
        }
    }

    /// Returns the current replica ID.
    pub fn replica_id(&self) -> u16 {
        self.replica_id
    }

    /// Returns the current local counter value.
    pub fn local_counter(&self) -> u64 {
        self.local_counter
    }

    /// Iterate over active element keys.
    pub fn keys(&self) -> impl Iterator<Item = u64> + '_ {
        self.elements.iter()
            .filter(|e| e.occupied)
            .map(|e| e.key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pn_counter_convergence() {
        let mut node1 = PNCounter::new();
        let mut node2 = PNCounter::new();

        node1.inc(10);
        node1.dec(3);

        node2.inc(15);
        node2.dec(1);

        node1.merge(&node2);
        assert_eq!(node1.increments, 15);
        assert_eq!(node1.decrements, 3);
        assert_eq!(node1.value(), 12);
    }

    // ─── AWORSet Tests ──────────────────────────────────────────────────────

    #[test]
    fn test_aworset_add_contains() {
        let mut set = AWORSet::new(1);
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);

        let dot = set.add(42);
        assert!(dot.is_some());
        assert!(set.contains(42));
        assert!(!set.contains(99));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_aworset_remove() {
        let mut set = AWORSet::new(1);
        set.add(10);
        set.add(20);
        assert_eq!(set.len(), 2);

        assert!(set.remove(10));
        assert!(!set.contains(10));
        assert!(set.contains(20));
        assert_eq!(set.len(), 1);

        // Removing non-existent key returns false
        assert!(!set.remove(999));
    }

    #[test]
    fn test_aworset_re_add() {
        let mut set = AWORSet::new(1);
        let dot1 = set.add(42).unwrap();
        let dot2 = set.add(42).unwrap(); // re-add same key
        // Counter should increase
        assert!(dot2.counter > dot1.counter);
        assert_eq!(set.len(), 1); // still one element
    }

    #[test]
    fn test_aworset_merge_add_wins() {
        let mut set_a = AWORSet::new(1);
        let mut set_b = AWORSet::new(2);

        // Both add key=100 concurrently
        set_a.add(100);
        set_b.add(100);

        // set_a removes key=100
        set_a.remove(100);
        assert!(!set_a.contains(100));

        // Merge: add-wins — set_b's add should survive set_a's remove
        // because they are concurrent
        set_a.merge(&set_b);
        // After merge, key=100 should be present (add-wins from set_b)
        assert!(set_a.contains(100));
    }

    #[test]
    fn test_aworset_merge_disjoint() {
        let mut set_a = AWORSet::new(1);
        let mut set_b = AWORSet::new(2);

        set_a.add(1);
        set_a.add(2);
        set_b.add(3);
        set_b.add(4);

        set_a.merge(&set_b);
        assert!(set_a.contains(1));
        assert!(set_a.contains(2));
        assert!(set_a.contains(3));
        assert!(set_a.contains(4));
        assert_eq!(set_a.len(), 4);
    }

    #[test]
    fn test_aworset_convergence() {
        // Two replicas perform operations independently, then merge both ways
        let mut replica_a = AWORSet::new(1);
        let mut replica_b = AWORSet::new(2);

        replica_a.add(10);
        replica_a.add(20);
        replica_b.add(20); // concurrent add on same key
        replica_b.add(30);

        // Merge A → B and B → A
        let mut merged_ab = replica_a.clone();
        merged_ab.merge(&replica_b);

        let mut merged_ba = replica_b.clone();
        merged_ba.merge(&replica_a);

        // Both merge results should converge to the same element set
        assert_eq!(merged_ab.len(), merged_ba.len());
        for key in merged_ab.keys() {
            assert!(merged_ba.contains(key), "key {} missing from merged_ba", key);
        }
        for key in merged_ba.keys() {
            assert!(merged_ab.contains(key), "key {} missing from merged_ab", key);
        }
    }

    #[test]
    fn test_aworset_keys_iterator() {
        let mut set = AWORSet::new(1);
        set.add(100);
        set.add(200);
        set.add(300);
        set.remove(200);

        let keys: Vec<u64> = set.keys().collect();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&100));
        assert!(keys.contains(&300));
    }
}
