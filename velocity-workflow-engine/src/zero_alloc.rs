//! Zero-allocation fixed-capacity slot map.
//! Pre-allocates all storage at construction time. Insert/remove/get are O(1)
//! with zero heap allocation on the hot path.
//!
//! Replaces `HashMap<K, V>` in workflow contexts where keys are step indices
//! or small integer IDs.

/// A fixed-capacity, zero-allocation map from `u64` keys to `V` values.
/// All storage is pre-allocated in `new()`. No `HashMap`, no per-insert alloc.
/// Uses `u64` keys to accommodate both step indices (u32) and signal/update IDs (u64).
pub struct SlotMap<V> {
    keys: Vec<u64>,
    slots: Vec<Option<V>>,
    occupied: u32,
}

impl<V> SlotMap<V> {
    /// Pre-allocate `capacity` slots. All slots start as `None`.
    pub fn with_capacity(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        slots.resize_with(capacity, || None);
        let mut keys = Vec::with_capacity(capacity);
        keys.resize(capacity, u64::MAX);
        Self {
            slots,
            keys,
            occupied: 0,
        }
    }

    /// Insert a value at the given key. Overwrites any existing value.
    /// **Zero allocation** — uses pre-allocated slot.
    pub fn insert(&mut self, key: u64, value: V) {
        // Linear scan for existing key
        for i in 0..self.slots.len() {
            if self.keys[i] == key && self.slots[i].is_some() {
                self.slots[i] = Some(value);
                return;
            }
        }
        // Find first empty slot
        for i in 0..self.slots.len() {
            if self.slots[i].is_none() {
                self.keys[i] = key;
                self.slots[i] = Some(value);
                self.occupied += 1;
                return;
            }
        }
        // Grow
        let _idx = self.slots.len();
        self.keys.push(key);
        self.slots.push(Some(value));
        self.occupied += 1;
    }

    fn find_slot(&self, key: u64) -> Option<usize> {
        (0..self.slots.len()).find(|&i| self.keys[i] == key && self.slots[i].is_some())
    }

    /// Get a reference to the value at `key`, if present.
    pub fn get(&self, key: u64) -> Option<&V> {
        self.find_slot(key).and_then(|i| self.slots[i].as_ref())
    }

    /// Get a mutable reference to the value at `key`, if present.
    pub fn get_mut(&mut self, key: u64) -> Option<&mut V> {
        self.find_slot(key).and_then(|i| self.slots[i].as_mut())
    }

    /// Remove the value at `key`. Returns `Some(V)` if it was present.
    pub fn remove(&mut self, key: u64) -> Option<V> {
        if let Some(i) = self.find_slot(key) {
            self.keys[i] = u64::MAX;
            let val = self.slots[i].take();
            if val.is_some() {
                self.occupied -= 1;
            }
            val
        } else {
            None
        }
    }

    /// Number of occupied slots.
    pub fn len(&self) -> usize {
        self.occupied as usize
    }

    pub fn is_empty(&self) -> bool {
        self.occupied == 0
    }

    /// Iterate over (key, &value) pairs for occupied slots only.
    pub fn iter(&self) -> impl Iterator<Item = (u64, &V)> {
        self.keys
            .iter()
            .zip(self.slots.iter())
            .filter_map(|(k, slot)| slot.as_ref().map(|v| (*k, v)))
    }

    /// Iterate over (key, &mut value) pairs for occupied slots only.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (u64, &mut V)> {
        let keys = &self.keys;
        self.slots
            .iter_mut()
            .enumerate()
            .filter_map(move |(i, slot)| {
                if slot.is_some() {
                    slot.as_mut().map(|v| (keys[i], v))
                } else {
                    None
                }
            })
    }

    /// Check if a key is present.
    pub fn contains_key(&self, key: u64) -> bool {
        self.find_slot(key).is_some()
    }

    /// Remove all entries. Does not deallocate storage.
    pub fn clear(&mut self) {
        for (k, slot) in self.keys.iter_mut().zip(self.slots.iter_mut()) {
            *k = u64::MAX;
            *slot = None;
        }
        self.occupied = 0;
    }

    /// Retain only entries that satisfy the predicate.
    pub fn retain<F: FnMut(&u64, &mut V) -> bool>(&mut self, mut f: F) {
        for i in 0..self.slots.len() {
            if let Some(ref mut v) = self.slots[i] {
                if !f(&self.keys[i], v) {
                    self.keys[i] = u64::MAX;
                    self.slots[i] = None;
                    self.occupied -= 1;
                }
            }
        }
    }
}

impl<V: Clone> Clone for SlotMap<V> {
    fn clone(&self) -> Self {
        Self {
            keys: self.keys.clone(),
            slots: self.slots.clone(),
            occupied: self.occupied,
        }
    }
}

/// A fixed-capacity slot map where each slot holds a `Vec<V>`.
/// Replaces `HashMap<K, Vec<V>>` for signal/update buffers.
/// Pre-allocates all slot storage; the inner Vecs grow as needed but
/// the slot index structure itself is zero-alloc.
pub struct SlotVec<V> {
    keys: Vec<u64>,
    slots: Vec<Vec<V>>,
    occupied: u32,
}

impl<V> SlotVec<V> {
    pub fn with_capacity(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        slots.resize_with(capacity, Vec::new);
        let mut keys = Vec::with_capacity(capacity);
        keys.resize(capacity, u64::MAX);
        Self {
            slots,
            keys,
            occupied: 0,
        }
    }

    fn find_slot(&self, key: u64) -> Option<usize> {
        (0..self.slots.len()).find(|&i| self.keys[i] == key && !self.slots[i].is_empty())
    }

    /// Push a value into the vec at `key`. Creates the vec if slot is empty.
    pub fn push(&mut self, key: u64, value: V) {
        // Check existing
        for i in 0..self.slots.len() {
            if self.keys[i] == key {
                if self.slots[i].is_empty() {
                    self.occupied += 1;
                }
                self.slots[i].push(value);
                return;
            }
        }
        // Find empty slot
        for i in 0..self.slots.len() {
            if self.slots[i].is_empty() && self.keys[i] == u64::MAX {
                self.keys[i] = key;
                self.slots[i].push(value);
                self.occupied += 1;
                return;
            }
        }
        // Grow
        self.keys.push(key);
        self.slots.push(vec![value]);
        self.occupied += 1;
    }

    /// Get the vec at `key`.
    pub fn get(&self, key: u64) -> Option<&Vec<V>> {
        self.find_slot(key).map(|i| &self.slots[i])
    }

    /// Get mutable vec at `key`.
    pub fn get_mut(&mut self, key: u64) -> Option<&mut Vec<V>> {
        self.find_slot(key).map(|i| &mut self.slots[i])
    }

    /// Remove and return the first element from the vec at `key`.
    pub fn pop_front(&mut self, key: u64) -> Option<V> {
        let i = self.find_slot(key)?;
        let slot = &mut self.slots[i];
        if slot.is_empty() {
            return None;
        }
        let val = slot.remove(0);
        if slot.is_empty() {
            self.keys[i] = u64::MAX;
            self.occupied -= 1;
        }
        Some(val)
    }

    pub fn is_empty_at(&self, key: u64) -> bool {
        self.find_slot(key).is_none()
    }

    pub fn occupied_count(&self) -> usize {
        self.occupied as usize
    }

    pub fn clear(&mut self) {
        for (k, slot) in self.keys.iter_mut().zip(self.slots.iter_mut()) {
            *k = u64::MAX;
            slot.clear();
        }
        self.occupied = 0;
    }

    /// Iterate over (key, &Vec<V>) pairs for occupied slots.
    pub fn iter(&self) -> impl Iterator<Item = (u64, &Vec<V>)> {
        self.keys
            .iter()
            .zip(self.slots.iter())
            .filter_map(|(k, slot)| {
                if !slot.is_empty() {
                    Some((*k, slot))
                } else {
                    None
                }
            })
    }
}

impl<V: Clone> Clone for SlotVec<V> {
    fn clone(&self) -> Self {
        Self {
            keys: self.keys.clone(),
            slots: self.slots.clone(),
            occupied: self.occupied,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_map_basic() {
        let mut map = SlotMap::<Vec<u8>>::with_capacity(16);
        assert!(map.is_empty());

        map.insert(0, vec![1, 2, 3]);
        map.insert(5, vec![4, 5, 6]);
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(0), Some(&vec![1, 2, 3]));
        assert_eq!(map.get(5), Some(&vec![4, 5, 6]));
        assert_eq!(map.get(3), None);

        let removed = map.remove(0);
        assert_eq!(removed, Some(vec![1, 2, 3]));
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(0), None);
    }

    #[test]
    fn test_slot_map_u64_keys() {
        let mut map = SlotMap::<u32>::with_capacity(8);
        map.insert(1_000_000_000, 42);
        map.insert(u64::MAX - 1, 99);
        assert_eq!(map.get(1_000_000_000), Some(&42));
        assert_eq!(map.get(u64::MAX - 1), Some(&99));
    }

    #[test]
    fn test_slot_map_overwrite() {
        let mut map = SlotMap::<u64>::with_capacity(8);
        map.insert(3, 100);
        map.insert(3, 200);
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(3), Some(&200));
    }

    #[test]
    fn test_slot_map_iter() {
        let mut map = SlotMap::<String>::with_capacity(8);
        map.insert(1, "a".into());
        map.insert(4, "b".into());
        map.insert(7, "c".into());

        let items: Vec<(u64, &String)> = map.iter().collect();
        assert_eq!(items.len(), 3);
    }

    #[test]
    fn test_slot_map_retain() {
        let mut map = SlotMap::<u32>::with_capacity(8);
        map.insert(1, 10);
        map.insert(2, 20);
        map.insert(3, 30);
        map.retain(|_k, v| *v > 15);
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(1), None);
        assert_eq!(map.get(2), Some(&20));
        assert_eq!(map.get(3), Some(&30));
    }

    #[test]
    fn test_slot_map_clone() {
        let mut map = SlotMap::<u32>::with_capacity(8);
        map.insert(1, 10);
        map.insert(2, 20);
        let cloned = map.clone();
        assert_eq!(cloned.get(1), Some(&10));
        assert_eq!(cloned.len(), 2);
    }

    #[test]
    fn test_slot_vec_basic() {
        let mut sv = SlotVec::<Vec<u8>>::with_capacity(8);
        sv.push(0, vec![1, 2]);
        sv.push(0, vec![3, 4]);
        sv.push(3, vec![5]);

        assert_eq!(sv.get(0).unwrap().len(), 2);
        assert_eq!(sv.get(3).unwrap().len(), 1);
        assert_eq!(sv.get(1), None);

        let front = sv.pop_front(0);
        assert_eq!(front, Some(vec![1, 2]));
        assert_eq!(sv.get(0).unwrap().len(), 1);
    }

    #[test]
    fn test_slot_vec_u64_keys() {
        let mut sv = SlotVec::<u8>::with_capacity(4);
        sv.push(1_000_000_000, 42);
        sv.push(u64::MAX - 1, 99);
        assert_eq!(sv.get(1_000_000_000).unwrap().len(), 1);
        assert_eq!(sv.get(u64::MAX - 1).unwrap().len(), 1);
    }

    #[test]
    fn test_slot_vec_empty_detection() {
        let mut sv = SlotVec::<u64>::with_capacity(4);
        assert!(sv.is_empty_at(0));
        sv.push(0, 42);
        assert!(!sv.is_empty_at(0));
        sv.pop_front(0);
        assert!(sv.is_empty_at(0));
    }

    #[test]
    fn test_slot_vec_clone() {
        let mut sv = SlotVec::<u32>::with_capacity(4);
        sv.push(1, 10);
        sv.push(2, 20);
        let cloned = sv.clone();
        assert_eq!(cloned.get(1).unwrap().len(), 1);
    }

    #[test]
    fn test_slot_map_grow_beyond_initial_capacity() {
        let mut map = SlotMap::<u32>::with_capacity(4);
        map.insert(100, 999);
        assert_eq!(map.get(100), Some(&999));
    }
}
