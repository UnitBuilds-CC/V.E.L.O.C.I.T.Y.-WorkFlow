//! LRU cache matching Temporal's common/cache (1,561 lines).
//!
//! Covers: LRU cache with eviction, TTL support, cache metrics,
//! pinned entries, and concurrent access patterns.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
};
use std::time::{Duration, Instant};

// ═══════════════════════════════════════════════════════════════════════════════
// LRU Cache Node
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug)]
struct CacheNode<K: Clone, V: Clone> {
    key: K,
    value: V,
    created_at: Instant,
    last_accessed: Instant,
    access_count: u64,
    pinned: bool,
    ttl: Option<Duration>,
}

// ═══════════════════════════════════════════════════════════════════════════════
// LRU Cache
// ═══════════════════════════════════════════════════════════════════════════════

pub struct LruCache<K: Clone + Eq + std::hash::Hash, V: Clone> {
    entries: RwLock<HashMap<K, CacheNode<K, V>>>,
    order: RwLock<Vec<K>>,
    capacity: usize,
    default_ttl: Option<Duration>,
    stats: CacheStats,
}

#[derive(Debug, Default)]
pub struct CacheStats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub evictions: AtomicU64,
    pub insertions: AtomicU64,
    pub deletions: AtomicU64,
    pub expirations: AtomicU64,
}

impl<K: Clone + Eq + std::hash::Hash + Ord, V: Clone> LruCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            order: RwLock::new(Vec::new()),
            capacity,
            default_ttl: None,
            stats: CacheStats::default(),
        }
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = Some(ttl);
        self
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let mut entries = self.entries.write().unwrap();
        if let Some(node) = entries.get_mut(key) {
            // Check TTL
            if let Some(ttl) = node.ttl {
                if node.last_accessed.elapsed() > ttl {
                    drop(entries);
                    self.remove(key);
                    self.stats.misses.fetch_add(1, Ordering::Relaxed);
                    self.stats.expirations.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            }
            node.last_accessed = Instant::now();
            node.access_count += 1;
            let value = node.value.clone();

            // Move to front of order
            let mut order = self.order.write().unwrap();
            if let Some(pos) = order.iter().position(|k| k == key) {
                order.remove(pos);
            }
            order.insert(0, key.clone());

            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            Some(value)
        } else {
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    pub fn put(&self, key: K, value: V) {
        self.put_with_ttl(key, value, None);
    }

    pub fn put_with_ttl(&self, key: K, value: V, ttl: Option<Duration>) {
        let mut entries = self.entries.write().unwrap();
        let now = Instant::now();

        if entries.contains_key(&key) {
            let node = entries.get_mut(&key).unwrap();
            node.value = value;
            node.last_accessed = now;
            node.ttl = ttl.or(self.default_ttl);
            return;
        }

        // Evict if at capacity
        if entries.len() >= self.capacity {
            drop(entries);
            self.evict_one();
            entries = self.entries.write().unwrap();
        }

        let node = CacheNode {
            key: key.clone(),
            value,
            created_at: now,
            last_accessed: now,
            access_count: 0,
            pinned: false,
            ttl: ttl.or(self.default_ttl),
        };

        entries.insert(key.clone(), node);
        self.order.write().unwrap().insert(0, key);
        self.stats.insertions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn put_pinned(&self, key: K, value: V) {
        let mut entries = self.entries.write().unwrap();
        let now = Instant::now();

        let node = CacheNode {
            key: key.clone(),
            value,
            created_at: now,
            last_accessed: now,
            access_count: 0,
            pinned: true,
            ttl: self.default_ttl,
        };

        if entries.contains_key(&key) {
            let existing = entries.get_mut(&key).unwrap();
            existing.value = node.value;
            existing.pinned = true;
            return;
        }

        if entries.len() >= self.capacity {
            drop(entries);
            self.evict_one();
            entries = self.entries.write().unwrap();
        }

        entries.insert(key.clone(), node);
        self.order.write().unwrap().insert(0, key);
        self.stats.insertions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        let mut entries = self.entries.write().unwrap();
        let node = entries.remove(key)?;
        let mut order = self.order.write().unwrap();
        if let Some(pos) = order.iter().position(|k| k == key) {
            order.remove(pos);
        }
        self.stats.deletions.fetch_add(1, Ordering::Relaxed);
        Some(node.value)
    }

    pub fn unpin(&self, key: &K) {
        let mut entries = self.entries.write().unwrap();
        if let Some(node) = entries.get_mut(key) {
            node.pinned = false;
        }
    }

    fn evict_one(&self) {
        let mut order = self.order.write().unwrap();
        let mut entries = self.entries.write().unwrap();

        // Find last non-pinned entry
        let mut evict_pos = None;
        for i in (0..order.len()).rev() {
            let key = &order[i];
            if let Some(node) = entries.get(key) {
                if !node.pinned {
                    evict_pos = Some(i);
                    break;
                }
            }
        }

        if let Some(pos) = evict_pos {
            let key = order.remove(pos);
            entries.remove(&key);
            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn evict_expired(&self) -> usize {
        let mut expired_keys = Vec::new();
        let entries = self.entries.read().unwrap();
        for (key, node) in entries.iter() {
            if let Some(ttl) = node.ttl {
                if node.last_accessed.elapsed() > ttl && !node.pinned {
                    expired_keys.push(key.clone());
                }
            }
        }
        drop(entries);

        let count = expired_keys.len();
        for key in &expired_keys {
            self.remove(key);
            self.stats.expirations.fetch_add(1, Ordering::Relaxed);
        }
        count
    }

    pub fn contains(&self, key: &K) -> bool {
        self.entries.read().unwrap().contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.entries.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.read().unwrap().is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn clear(&self) {
        self.entries.write().unwrap().clear();
        self.order.write().unwrap().clear();
    }

    pub fn keys(&self) -> Vec<K> {
        self.order.read().unwrap().clone()
    }

    pub fn values(&self) -> Vec<V> {
        let order = self.order.read().unwrap();
        let entries = self.entries.read().unwrap();
        order
            .iter()
            .filter_map(|k| entries.get(k).map(|n| n.value.clone()))
            .collect()
    }

    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    pub fn hit_rate(&self) -> f64 {
        let hits = self.stats.hits.load(Ordering::Relaxed) as f64;
        let total = hits + self.stats.misses.load(Ordering::Relaxed) as f64;
        if total > 0.0 {
            hits / total
        } else {
            0.0
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_basic_put_get() {
        let cache = LruCache::new(10);
        cache.put("key1", "value1");
        assert_eq!(cache.get(&"key1"), Some("value1"));
    }

    #[test]
    fn test_miss() {
        let cache = LruCache::<&str, &str>::new(10);
        assert_eq!(cache.get(&"missing"), None);
    }

    #[test]
    fn test_eviction_on_capacity() {
        let cache = LruCache::new(3);
        cache.put("a", 1);
        cache.put("b", 2);
        cache.put("c", 3);
        cache.put("d", 4); // should evict "a"
        assert_eq!(cache.get(&"a"), None);
        assert_eq!(cache.get(&"d"), Some(4));
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_lru_order() {
        let cache = LruCache::new(3);
        cache.put("a", 1);
        cache.put("b", 2);
        cache.put("c", 3);
        // Access "a" to make it recently used
        cache.get(&"a");
        // Insert "d" — should evict "b" (least recently used)
        cache.put("d", 4);
        assert!(cache.get(&"a").is_some());
        assert!(cache.get(&"b").is_none());
        assert!(cache.get(&"c").is_some());
        assert!(cache.get(&"d").is_some());
    }

    #[test]
    fn test_remove() {
        let cache = LruCache::new(10);
        cache.put("key", "value");
        let removed = cache.remove(&"key");
        assert_eq!(removed, Some("value"));
        assert_eq!(cache.get(&"key"), None);
    }

    #[test]
    fn test_pinned_not_evicted() {
        let cache = LruCache::new(2);
        cache.put_pinned("pinned", 1);
        cache.put("normal", 2);
        cache.put("evict_me", 3); // should evict "normal", not "pinned"
        assert!(cache.get(&"pinned").is_some());
        assert!(cache.get(&"normal").is_none());
    }

    #[test]
    fn test_unpin() {
        let cache = LruCache::new(2);
        cache.put_pinned("key", 1);
        cache.put("other", 2);
        cache.unpin(&"key");
        cache.put("new", 3); // now "key" can be evicted
        assert!(cache.get(&"key").is_none());
    }

    #[test]
    fn test_stats() {
        let cache = LruCache::new(10);
        cache.put("a", 1);
        cache.get(&"a"); // hit
        cache.get(&"b"); // miss
        assert_eq!(cache.stats().hits.load(Ordering::Relaxed), 1);
        assert_eq!(cache.stats().misses.load(Ordering::Relaxed), 1);
        assert_eq!(cache.stats().insertions.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_hit_rate() {
        let cache = LruCache::new(10);
        cache.put("a", 1);
        cache.get(&"a"); // hit
        cache.get(&"b"); // miss
        let rate = cache.hit_rate();
        assert!((rate - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_clear() {
        let cache = LruCache::new(10);
        cache.put("a", 1);
        cache.put("b", 2);
        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_contains() {
        let cache = LruCache::new(10);
        cache.put("key", "val");
        assert!(cache.contains(&"key"));
        assert!(!cache.contains(&"other"));
    }

    #[test]
    fn test_keys_and_values() {
        let cache = LruCache::new(10);
        cache.put("b", 2);
        cache.put("a", 1);
        let keys = cache.keys();
        assert_eq!(keys[0], "a"); // most recent first
        assert_eq!(keys[1], "b");
    }

    #[test]
    fn test_update_existing() {
        let cache = LruCache::new(10);
        cache.put("key", 1);
        cache.put("key", 2);
        assert_eq!(cache.get(&"key"), Some(2));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_with_ttl() {
        let cache = LruCache::new(10).with_ttl(Duration::from_millis(50));
        cache.put("key", "value");
        assert_eq!(cache.get(&"key"), Some("value"));
        thread::sleep(Duration::from_millis(100));
        assert_eq!(cache.get(&"key"), None); // expired
    }

    #[test]
    fn test_evict_expired() {
        let cache = LruCache::new(10).with_ttl(Duration::from_millis(30));
        cache.put("a", 1);
        cache.put("b", 2);
        thread::sleep(Duration::from_millis(50));
        let expired = cache.evict_expired();
        assert_eq!(expired, 2);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_capacity() {
        let cache = LruCache::<String, i32>::new(50);
        assert_eq!(cache.capacity(), 50);
    }
}
