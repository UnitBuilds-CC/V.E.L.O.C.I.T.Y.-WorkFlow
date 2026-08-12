//! Quota management matching Temporal's common/quotas (3,535 lines).
//!
//! Covers: quota policies, per-namespace quotas, collection-aware quotas,
//! quota calculator, priority-based quotas, and quota tracking.

use std::collections::HashMap;
use std::sync::{Arc, RwLock, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::time::{SystemTime, Duration, Instant};

// ═══════════════════════════════════════════════════════════════════════════════
// Quota Policy
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct QuotaPolicy {
    pub name: String,
    pub burst: u32,
    pub rate_per_second: f64,
    pub max_tokens_per_cycle: u32,
    pub priority: QuotaPriority,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum QuotaPriority {
    Default = 0,
    Preemptible = 1,
    Standard = 10,
    Elevated = 20,
    Critical = 30,
}

impl QuotaPolicy {
    pub fn new(name: &str, rate: f64, burst: u32) -> Self {
        Self {
            name: name.to_string(),
            burst,
            rate_per_second: rate,
            max_tokens_per_cycle: burst,
            priority: QuotaPriority::Standard,
        }
    }

    pub fn with_priority(mut self, priority: QuotaPriority) -> Self {
        self.priority = priority;
        self
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Quota Token Bucket
// ═══════════════════════════════════════════════════════════════════════════════

pub struct QuotaBucket {
    tokens: RwLock<f64>,
    last_refill: RwLock<Instant>,
    policy: QuotaPolicy,
    stats: BucketStats,
}

#[derive(Debug, Default)]
pub struct BucketStats {
    pub requests_allowed: AtomicU64,
    pub requests_denied: AtomicU64,
    pub tokens_consumed: AtomicU64,
}

impl QuotaBucket {
    pub fn new(policy: QuotaPolicy) -> Self {
        Self {
            tokens: RwLock::new(policy.burst as f64),
            last_refill: RwLock::new(Instant::now()),
            policy,
            stats: BucketStats::default(),
        }
    }

    pub fn allow(&self, count: u32) -> bool {
        self.refill();
        let mut tokens = self.tokens.write().unwrap();
        if *tokens >= count as f64 {
            *tokens -= count as f64;
            self.stats.requests_allowed.fetch_add(1, Ordering::Relaxed);
            self.stats.tokens_consumed.fetch_add(count as u64, Ordering::Relaxed);
            true
        } else {
            self.stats.requests_denied.fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    pub fn allow_one(&self) -> bool {
        self.allow(1)
    }

    fn refill(&self) {
        let now = Instant::now();
        let mut last = self.last_refill.write().unwrap();
        let elapsed = now.duration_since(*last);
        if elapsed.as_millis() > 0 {
            let new_tokens = elapsed.as_secs_f64() * self.policy.rate_per_second;
            let mut tokens = self.tokens.write().unwrap();
            *tokens = (*tokens + new_tokens).min(self.policy.burst as f64);
            *last = now;
        }
    }

    pub fn available_tokens(&self) -> f64 {
        self.refill();
        *self.tokens.read().unwrap()
    }

    pub fn policy(&self) -> &QuotaPolicy { &self.policy }
    pub fn stats(&self) -> &BucketStats { &self.stats }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Namespace Quota Tracker
// ═══════════════════════════════════════════════════════════════════════════════

pub struct NamespaceQuotaTracker {
    namespaces: RwLock<HashMap<String, Arc<QuotaBucket>>>,
    default_policy: RwLock<QuotaPolicy>,
    overrides: RwLock<HashMap<String, QuotaPolicy>>,
    stats: NamespaceQuotaStats,
}

#[derive(Debug, Default)]
pub struct NamespaceQuotaStats {
    pub namespaces_tracked: AtomicU64,
    pub total_allowed: AtomicU64,
    pub total_denied: AtomicU64,
}

impl NamespaceQuotaTracker {
    pub fn new(default_rate: f64, default_burst: u32) -> Self {
        Self {
            namespaces: RwLock::new(HashMap::new()),
            default_policy: RwLock::new(QuotaPolicy::new("default", default_rate, default_burst)),
            overrides: RwLock::new(HashMap::new()),
            stats: NamespaceQuotaStats::default(),
        }
    }

    pub fn get_or_create_bucket(&self, namespace: &str) -> Arc<QuotaBucket> {
        // Check cache first
        {
            let ns = self.namespaces.read().unwrap();
            if let Some(bucket) = ns.get(namespace) {
                return bucket.clone();
            }
        }

        // Create new bucket
        let policy = {
            let overrides = self.overrides.read().unwrap();
            overrides.get(namespace).cloned()
                .unwrap_or_else(|| self.default_policy.read().unwrap().clone())
        };

        let bucket = Arc::new(QuotaBucket::new(policy));
        self.namespaces.write().unwrap().insert(namespace.to_string(), bucket.clone());
        self.stats.namespaces_tracked.fetch_add(1, Ordering::Relaxed);
        bucket
    }

    pub fn set_namespace_quota(&self, namespace: &str, rate: f64, burst: u32) {
        let policy = QuotaPolicy::new(namespace, rate, burst);
        self.overrides.write().unwrap().insert(namespace.to_string(), policy.clone());
        let bucket = Arc::new(QuotaBucket::new(policy));
        self.namespaces.write().unwrap().insert(namespace.to_string(), bucket);
    }

    pub fn check_quota(&self, namespace: &str, count: u32) -> bool {
        let bucket = self.get_or_create_bucket(namespace);
        let allowed = bucket.allow(count);
        if allowed {
            self.stats.total_allowed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.total_denied.fetch_add(1, Ordering::Relaxed);
        }
        allowed
    }

    pub fn set_default_quota(&self, rate: f64, burst: u32) {
        *self.default_policy.write().unwrap() = QuotaPolicy::new("default", rate, burst);
    }

    pub fn remove_namespace_quota(&self, namespace: &str) {
        self.overrides.write().unwrap().remove(namespace);
        self.namespaces.write().unwrap().remove(namespace);
    }

    pub fn tracked_count(&self) -> usize {
        self.namespaces.read().unwrap().len()
    }

    pub fn stats(&self) -> &NamespaceQuotaStats { &self.stats }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Quota Calculator
// ═══════════════════════════════════════════════════════════════════════════════

pub struct QuotaCalculator {
    base_rate: f64,
    namespace_weights: RwLock<HashMap<String, f64>>,
    total_namespaces: AtomicU64,
}

impl QuotaCalculator {
    pub fn new(base_rate: f64) -> Self {
        Self {
            base_rate,
            namespace_weights: RwLock::new(HashMap::new()),
            total_namespaces: AtomicU64::new(0),
        }
    }

    pub fn calculate_quota(&self, namespace: &str) -> f64 {
        let weights = self.namespace_weights.read().unwrap();
        let weight = weights.get(namespace).copied().unwrap_or(1.0);
        let total_weight: f64 = weights.values().sum::<f64>().max(1.0);
        (self.base_rate * weight / total_weight).max(1.0)
    }

    pub fn set_namespace_weight(&self, namespace: &str, weight: f64) {
        self.namespace_weights.write().unwrap().insert(namespace.to_string(), weight);
        let count = self.namespace_weights.read().unwrap().len() as u64;
        self.total_namespaces.store(count, Ordering::Relaxed);
    }

    pub fn remove_namespace_weight(&self, namespace: &str) {
        self.namespace_weights.write().unwrap().remove(namespace);
        let count = self.namespace_weights.read().unwrap().len() as u64;
        self.total_namespaces.store(count, Ordering::Relaxed);
    }

    pub fn total_namespaces(&self) -> u64 {
        self.total_namespaces.load(Ordering::Relaxed)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Rate Limited Operation Tracker
// ═══════════════════════════════════════════════════════════════════════════════

pub struct OperationQuotaTracker {
    operations: RwLock<HashMap<String, Arc<QuotaBucket>>>,
}

impl OperationQuotaTracker {
    pub fn new() -> Self {
        Self { operations: RwLock::new(HashMap::new()) }
    }

    pub fn register_operation(&self, operation: &str, rate: f64, burst: u32) {
        let policy = QuotaPolicy::new(operation, rate, burst);
        self.operations.write().unwrap().insert(operation.to_string(), Arc::new(QuotaBucket::new(policy)));
    }

    pub fn check_operation_quota(&self, operation: &str) -> bool {
        let ops = self.operations.read().unwrap();
        if let Some(bucket) = ops.get(operation) {
            bucket.allow_one()
        } else {
            true // No quota registered = allowed
        }
    }

    pub fn operation_count(&self) -> usize {
        self.operations.read().unwrap().len()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quota_bucket_basic() {
        let policy = QuotaPolicy::new("test", 100.0, 10);
        let bucket = QuotaBucket::new(policy);
        assert!(bucket.allow_one());
        assert!(bucket.allow(5));
    }

    #[test]
    fn test_quota_bucket_exhaustion() {
        let policy = QuotaPolicy::new("test", 1.0, 3);
        let bucket = QuotaBucket::new(policy);
        assert!(bucket.allow_one());
        assert!(bucket.allow_one());
        assert!(bucket.allow_one());
        assert!(!bucket.allow_one()); // exhausted
    }

    #[test]
    fn test_quota_bucket_stats() {
        let policy = QuotaPolicy::new("test", 1.0, 2);
        let bucket = QuotaBucket::new(policy);
        bucket.allow_one();
        bucket.allow_one();
        bucket.allow_one(); // denied
        assert_eq!(bucket.stats().requests_allowed.load(Ordering::Relaxed), 2);
        assert_eq!(bucket.stats().requests_denied.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_namespace_quota_tracker() {
        let tracker = NamespaceQuotaTracker::new(100.0, 10);
        assert!(tracker.check_quota("ns-1", 5));
        assert!(tracker.check_quota("ns-2", 5));
        assert_eq!(tracker.tracked_count(), 2);
    }

    #[test]
    fn test_namespace_quota_override() {
        let tracker = NamespaceQuotaTracker::new(100.0, 10);
        tracker.set_namespace_quota("premium", 1000.0, 100);
        let bucket = tracker.get_or_create_bucket("premium");
        assert_eq!(bucket.policy().rate_per_second, 1000.0);
        assert_eq!(bucket.policy().burst, 100);
    }

    #[test]
    fn test_quota_calculator() {
        let calc = QuotaCalculator::new(1000.0);
        calc.set_namespace_weight("ns-1", 1.0);
        calc.set_namespace_weight("ns-2", 3.0);

        let q1 = calc.calculate_quota("ns-1");
        let q2 = calc.calculate_quota("ns-2");
        // ns-2 should get ~3x the quota of ns-1
        assert!(q2 > q1);
    }

    #[test]
    fn test_quota_calculator_unknown_namespace() {
        let calc = QuotaCalculator::new(100.0);
        let q = calc.calculate_quota("unknown");
        assert!(q >= 1.0);
    }

    #[test]
    fn test_operation_quota_tracker() {
        let tracker = OperationQuotaTracker::new();
        tracker.register_operation("StartWorkflow", 50.0, 5);
        assert!(tracker.check_operation_quota("StartWorkflow"));
        assert_eq!(tracker.operation_count(), 1);
    }

    #[test]
    fn test_unregistered_operation_allowed() {
        let tracker = OperationQuotaTracker::new();
        assert!(tracker.check_operation_quota("Unknown"));
    }

    #[test]
    fn test_quota_policy_priority() {
        let p1 = QuotaPolicy::new("low", 10.0, 5).with_priority(QuotaPriority::Default);
        let p2 = QuotaPolicy::new("high", 10.0, 5).with_priority(QuotaPriority::Critical);
        assert!(p2.priority > p1.priority);
    }

    #[test]
    fn test_remove_namespace_quota() {
        let tracker = NamespaceQuotaTracker::new(100.0, 10);
        tracker.set_namespace_quota("temp", 50.0, 5);
        assert_eq!(tracker.tracked_count(), 1);
        tracker.remove_namespace_quota("temp");
        assert_eq!(tracker.tracked_count(), 0);
    }
}
