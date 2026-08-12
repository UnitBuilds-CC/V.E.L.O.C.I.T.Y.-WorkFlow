//! Deep common utilities matching Temporal's common subsystems.
//!
//! Covers: quota management (3.5K), search attribute management (4.8K),
//! metrics framework depth (6.9K), task framework (4.2K),
//! worker versioning depth (3.7K), RPC utilities.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex, RwLock,
};
use std::time::{Duration, Instant, SystemTime};

// ═══════════════════════════════════════════════════════════════════════════════
// Quota Management (3,535 lines in Temporal)
// ═══════════════════════════════════════════════════════════════════════════════

pub struct QuotaManager {
    quotas: RwLock<HashMap<String, QuotaPolicy>>,
    usage: RwLock<HashMap<String, QuotaUsageTracker>>,
    stats: QuotaStats,
}

#[derive(Debug, Default)]
pub struct QuotaStats {
    pub checks: AtomicU64,
    pub allowed: AtomicU64,
    pub denied: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct QuotaPolicy {
    pub name: String,
    pub namespace: String,
    pub calls_per_second: f64,
    pub burst: u32,
    pub max_concurrent: u32,
}

struct QuotaUsageTracker {
    tokens: f64,
    max_tokens: f64,
    rate: f64,
    concurrent: u32,
    max_concurrent: u32,
    last_refill: Instant,
}

impl QuotaManager {
    pub fn new() -> Self {
        Self {
            quotas: RwLock::new(HashMap::new()),
            usage: RwLock::new(HashMap::new()),
            stats: QuotaStats::default(),
        }
    }

    pub fn set_policy(&self, policy: QuotaPolicy) {
        let key = format!("{}:{}", policy.namespace, policy.name);
        self.quotas.write().unwrap().insert(key, policy);
    }

    pub fn try_acquire(&self, namespace: &str, quota_name: &str) -> bool {
        self.stats.checks.fetch_add(1, Ordering::Relaxed);
        let key = format!("{}:{}", namespace, quota_name);

        let quotas = self.quotas.read().unwrap();
        let policy = match quotas.get(&key) {
            Some(p) => p,
            None => {
                self.stats.allowed.fetch_add(1, Ordering::Relaxed);
                return true;
            }
        };

        let mut usage = self.usage.write().unwrap();
        let tracker = usage.entry(key).or_insert_with(|| QuotaUsageTracker {
            tokens: policy.calls_per_second,
            max_tokens: policy.calls_per_second,
            rate: policy.calls_per_second,
            concurrent: 0,
            max_concurrent: policy.max_concurrent,
            last_refill: Instant::now(),
        });

        // Refill tokens
        let now = Instant::now();
        let elapsed = now.duration_since(tracker.last_refill).as_secs_f64();
        tracker.tokens = (tracker.tokens + elapsed * tracker.rate).min(tracker.max_tokens);
        tracker.last_refill = now;

        if tracker.tokens >= 1.0 {
            tracker.tokens -= 1.0;
            self.stats.allowed.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            self.stats.denied.fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    pub fn try_acquire_concurrent(&self, namespace: &str, quota_name: &str) -> bool {
        let key = format!("{}:{}", namespace, quota_name);
        let quotas = self.quotas.read().unwrap();
        let policy = match quotas.get(&key) {
            Some(p) => p,
            None => return true,
        };

        let mut usage = self.usage.write().unwrap();
        let tracker = usage.entry(key).or_insert_with(|| QuotaUsageTracker {
            tokens: policy.calls_per_second,
            max_tokens: policy.calls_per_second,
            rate: policy.calls_per_second,
            concurrent: 0,
            max_concurrent: policy.max_concurrent,
            last_refill: Instant::now(),
        });

        if tracker.concurrent < tracker.max_concurrent {
            tracker.concurrent += 1;
            true
        } else {
            false
        }
    }

    pub fn release_concurrent(&self, namespace: &str, quota_name: &str) {
        let key = format!("{}:{}", namespace, quota_name);
        let mut usage = self.usage.write().unwrap();
        if let Some(tracker) = usage.get_mut(&key) {
            tracker.concurrent = tracker.concurrent.saturating_sub(1);
        }
    }

    pub fn stats(&self) -> &QuotaStats {
        &self.stats
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Search Attribute Management (4,763 lines in Temporal)
// ═══════════════════════════════════════════════════════════════════════════════

pub struct SearchAttributeManager {
    system_attributes: RwLock<HashMap<String, SearchAttributeDefinition>>,
    custom_attributes: RwLock<HashMap<String, SearchAttributeDefinition>>,
    namespace_attributes: RwLock<HashMap<String, HashMap<String, SearchAttributeDefinition>>>,
    stats: SearchAttributeStats,
}

#[derive(Debug, Default)]
pub struct SearchAttributeStats {
    pub lookups: AtomicU64,
    pub validations: AtomicU64,
    pub validation_failures: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct SearchAttributeDefinition {
    pub name: String,
    pub field_type: SearchAttributeFieldType,
    pub is_system: bool,
    pub description: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchAttributeFieldType {
    Text = 0,
    Keyword = 1,
    Int = 2,
    Double = 3,
    Bool = 4,
    Datetime = 5,
    KeywordList = 6,
}

impl SearchAttributeManager {
    pub fn new() -> Self {
        let mgr = Self {
            system_attributes: RwLock::new(HashMap::new()),
            custom_attributes: RwLock::new(HashMap::new()),
            namespace_attributes: RwLock::new(HashMap::new()),
            stats: SearchAttributeStats::default(),
        };

        // Register system attributes
        let system_attrs = vec![
            ("WorkflowId", SearchAttributeFieldType::Keyword),
            ("RunId", SearchAttributeFieldType::Keyword),
            ("WorkflowType", SearchAttributeFieldType::Keyword),
            ("StartTime", SearchAttributeFieldType::Datetime),
            ("CloseTime", SearchAttributeFieldType::Datetime),
            ("ExecutionStatus", SearchAttributeFieldType::Keyword),
            ("ExecutionDuration", SearchAttributeFieldType::Int),
            ("StateTransitionCount", SearchAttributeFieldType::Int),
            ("HistoryLength", SearchAttributeFieldType::Int),
            ("HistorySizeBytes", SearchAttributeFieldType::Int),
            ("TaskQueue", SearchAttributeFieldType::Keyword),
            ("Namespace", SearchAttributeFieldType::Keyword),
            ("ParentWorkflowId", SearchAttributeFieldType::Keyword),
            ("ParentRunId", SearchAttributeFieldType::Keyword),
            ("BinaryChecksums", SearchAttributeFieldType::KeywordList),
            (
                "TemporalChangeVersion",
                SearchAttributeFieldType::KeywordList,
            ),
            ("BatchOperationId", SearchAttributeFieldType::Keyword),
            ("BuildIds", SearchAttributeFieldType::KeywordList),
        ];

        for (name, field_type) in system_attrs {
            mgr.system_attributes.write().unwrap().insert(
                name.to_string(),
                SearchAttributeDefinition {
                    name: name.to_string(),
                    field_type,
                    is_system: true,
                    description: format!("System search attribute: {}", name),
                },
            );
        }

        mgr
    }

    pub fn register_custom_attribute(
        &self,
        namespace: &str,
        name: &str,
        field_type: SearchAttributeFieldType,
    ) -> Result<(), SearchAttributeError> {
        if self.get_attribute(name).is_some() {
            return Err(SearchAttributeError::AlreadyExists(name.to_string()));
        }

        let def = SearchAttributeDefinition {
            name: name.to_string(),
            field_type,
            is_system: false,
            description: String::new(),
        };

        self.custom_attributes
            .write()
            .unwrap()
            .insert(name.to_string(), def.clone());
        self.namespace_attributes
            .write()
            .unwrap()
            .entry(namespace.to_string())
            .or_insert_with(HashMap::new)
            .insert(name.to_string(), def);

        Ok(())
    }

    pub fn get_attribute(&self, name: &str) -> Option<SearchAttributeDefinition> {
        self.stats.lookups.fetch_add(1, Ordering::Relaxed);
        if let Some(def) = self.system_attributes.read().unwrap().get(name) {
            return Some(def.clone());
        }
        self.custom_attributes.read().unwrap().get(name).cloned()
    }

    pub fn validate_value(
        &self,
        name: &str,
        value: &SearchAttributeValue,
    ) -> Result<(), SearchAttributeError> {
        self.stats.validations.fetch_add(1, Ordering::Relaxed);
        let def = self
            .get_attribute(name)
            .ok_or_else(|| SearchAttributeError::NotFound(name.to_string()))?;

        let valid = match def.field_type {
            SearchAttributeFieldType::Text => matches!(value, SearchAttributeValue::Text(_)),
            SearchAttributeFieldType::Keyword => matches!(value, SearchAttributeValue::Keyword(_)),
            SearchAttributeFieldType::Int => matches!(value, SearchAttributeValue::Int(_)),
            SearchAttributeFieldType::Double => matches!(value, SearchAttributeValue::Double(_)),
            SearchAttributeFieldType::Bool => matches!(value, SearchAttributeValue::Bool(_)),
            SearchAttributeFieldType::Datetime => {
                matches!(value, SearchAttributeValue::Datetime(_))
            }
            SearchAttributeFieldType::KeywordList => {
                matches!(value, SearchAttributeValue::KeywordList(_))
            }
        };

        if !valid {
            self.stats
                .validation_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(SearchAttributeError::TypeMismatch {
                name: name.to_string(),
                expected: format!("{:?}", def.field_type),
                got: format!("{:?}", value),
            });
        }
        Ok(())
    }

    pub fn list_system_attributes(&self) -> Vec<SearchAttributeDefinition> {
        self.system_attributes
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    pub fn list_custom_attributes(&self, namespace: &str) -> Vec<SearchAttributeDefinition> {
        self.namespace_attributes
            .read()
            .unwrap()
            .get(namespace)
            .map(|attrs| attrs.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn stats(&self) -> &SearchAttributeStats {
        &self.stats
    }
}

#[derive(Debug, Clone)]
pub enum SearchAttributeValue {
    Text(String),
    Keyword(String),
    Int(i64),
    Double(f64),
    Bool(bool),
    Datetime(i64),
    KeywordList(Vec<String>),
}

#[derive(Debug, Clone)]
pub enum SearchAttributeError {
    AlreadyExists(String),
    NotFound(String),
    TypeMismatch {
        name: String,
        expected: String,
        got: String,
    },
}

// ═══════════════════════════════════════════════════════════════════════════════
// Metrics Framework Depth (6,935 lines in Temporal)
// ═══════════════════════════════════════════════════════════════════════════════

pub struct MetricsFramework {
    scopes: RwLock<HashMap<String, MetricsScope>>,
    definitions: RwLock<HashMap<String, MetricDefinition>>,
    stats: MetricsFrameworkStats,
}

#[derive(Debug, Default)]
pub struct MetricsFrameworkStats {
    pub counters_recorded: AtomicU64,
    pub gauges_recorded: AtomicU64,
    pub histograms_recorded: AtomicU64,
    pub timers_recorded: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct MetricsScope {
    pub name: String,
    pub tags: HashMap<String, String>,
    pub counters: HashMap<String, u64>,
    pub gauges: HashMap<String, f64>,
    pub histograms: HashMap<String, Vec<f64>>,
    pub timers: HashMap<String, Vec<Duration>>,
}

#[derive(Debug, Clone)]
pub struct MetricDefinition {
    pub name: String,
    pub metric_type: MetricType,
    pub description: String,
    pub unit: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    Counter = 0,
    Gauge = 1,
    Histogram = 2,
    Timer = 3,
}

impl MetricsFramework {
    pub fn new() -> Self {
        let fw = Self {
            scopes: RwLock::new(HashMap::new()),
            definitions: RwLock::new(HashMap::new()),
            stats: MetricsFrameworkStats::default(),
        };

        // Register standard metric definitions
        let defs = vec![
            (
                "workflow_started",
                MetricType::Counter,
                "Number of workflows started",
                "1",
            ),
            (
                "workflow_completed",
                MetricType::Counter,
                "Number of workflows completed",
                "1",
            ),
            (
                "workflow_failed",
                MetricType::Counter,
                "Number of workflows failed",
                "1",
            ),
            (
                "workflow_cancelled",
                MetricType::Counter,
                "Number of workflows cancelled",
                "1",
            ),
            (
                "workflow_continued_as_new",
                MetricType::Counter,
                "Number of workflows continued as new",
                "1",
            ),
            (
                "workflow_task_latency",
                MetricType::Timer,
                "Workflow task processing latency",
                "ms",
            ),
            (
                "activity_task_latency",
                MetricType::Timer,
                "Activity task processing latency",
                "ms",
            ),
            (
                "matching_latency",
                MetricType::Timer,
                "Matching engine latency",
                "ms",
            ),
            (
                "history_size",
                MetricType::Histogram,
                "Workflow history size",
                "bytes",
            ),
            (
                "mutable_state_size",
                MetricType::Histogram,
                "Mutable state size",
                "bytes",
            ),
            (
                "persistence_latency",
                MetricType::Timer,
                "Persistence operation latency",
                "ms",
            ),
            (
                "replication_latency",
                MetricType::Timer,
                "Replication latency",
                "ms",
            ),
            (
                "task_queue_depth",
                MetricType::Gauge,
                "Current task queue depth",
                "1",
            ),
            (
                "active_pollers",
                MetricType::Gauge,
                "Number of active pollers",
                "1",
            ),
            ("shard_count", MetricType::Gauge, "Number of shards", "1"),
        ];

        for (name, metric_type, desc, unit) in defs {
            fw.definitions.write().unwrap().insert(
                name.to_string(),
                MetricDefinition {
                    name: name.to_string(),
                    metric_type,
                    description: desc.to_string(),
                    unit: unit.to_string(),
                },
            );
        }

        fw
    }

    pub fn create_scope(&self, name: &str, tags: HashMap<String, String>) -> String {
        let scope = MetricsScope {
            name: name.to_string(),
            tags,
            counters: HashMap::new(),
            gauges: HashMap::new(),
            histograms: HashMap::new(),
            timers: HashMap::new(),
        };
        let key = format!("{}:{:?}", name, scope.tags);
        self.scopes.write().unwrap().insert(key.clone(), scope);
        key
    }

    pub fn record_counter(&self, scope_name: &str, metric: &str, value: u64) {
        let mut scopes = self.scopes.write().unwrap();
        if let Some(scope) = scopes.get_mut(scope_name) {
            let counter = scope.counters.entry(metric.to_string()).or_insert(0);
            *counter += value;
        }
        self.stats.counters_recorded.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_gauge(&self, scope_name: &str, metric: &str, value: f64) {
        let mut scopes = self.scopes.write().unwrap();
        if let Some(scope) = scopes.get_mut(scope_name) {
            scope.gauges.insert(metric.to_string(), value);
        }
        self.stats.gauges_recorded.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_histogram(&self, scope_name: &str, metric: &str, value: f64) {
        let mut scopes = self.scopes.write().unwrap();
        if let Some(scope) = scopes.get_mut(scope_name) {
            scope
                .histograms
                .entry(metric.to_string())
                .or_insert_with(Vec::new)
                .push(value);
        }
        self.stats
            .histograms_recorded
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_timer(&self, scope_name: &str, metric: &str, duration: Duration) {
        let mut scopes = self.scopes.write().unwrap();
        if let Some(scope) = scopes.get_mut(scope_name) {
            scope
                .timers
                .entry(metric.to_string())
                .or_insert_with(Vec::new)
                .push(duration);
        }
        self.stats.timers_recorded.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_counter(&self, scope_name: &str, metric: &str) -> u64 {
        self.scopes
            .read()
            .unwrap()
            .get(scope_name)
            .and_then(|s| s.counters.get(metric))
            .copied()
            .unwrap_or(0)
    }

    pub fn get_gauge(&self, scope_name: &str, metric: &str) -> Option<f64> {
        self.scopes
            .read()
            .unwrap()
            .get(scope_name)
            .and_then(|s| s.gauges.get(metric))
            .copied()
    }

    pub fn list_definitions(&self) -> Vec<MetricDefinition> {
        self.definitions.read().unwrap().values().cloned().collect()
    }

    pub fn stats(&self) -> &MetricsFrameworkStats {
        &self.stats
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task Framework (4,181 lines in Temporal)
// ═══════════════════════════════════════════════════════════════════════════════

pub struct TaskFramework {
    task_executors: RwLock<HashMap<String, Arc<dyn TaskExecutor>>>,
    stats: TaskFrameworkStats,
}

#[derive(Debug, Default)]
pub struct TaskFrameworkStats {
    pub tasks_executed: AtomicU64,
    pub tasks_failed: AtomicU64,
    pub tasks_retried: AtomicU64,
}

pub trait TaskExecutor: Send + Sync {
    fn task_type(&self) -> &str;
    fn execute(&self, task: &FrameworkTask) -> Result<TaskResult, TaskError>;
}

#[derive(Debug, Clone)]
pub struct FrameworkTask {
    pub task_id: i64,
    pub task_type: String,
    pub namespace_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub shard_id: i32,
    pub visibility_time_ms: i64,
    pub version: i64,
    pub payload: Vec<u8>,
    pub attempt: i32,
}

#[derive(Debug, Clone)]
pub struct TaskResult {
    pub success: bool,
    pub ack: bool,
    pub nack_reason: Option<String>,
    pub retry: bool,
}

#[derive(Debug, Clone)]
pub enum TaskError {
    ExecutionFailed(String),
    SerializationError(String),
    Timeout,
}

impl TaskFramework {
    pub fn new() -> Self {
        Self {
            task_executors: RwLock::new(HashMap::new()),
            stats: TaskFrameworkStats::default(),
        }
    }

    pub fn register_executor(&self, executor: Arc<dyn TaskExecutor>) {
        self.task_executors
            .write()
            .unwrap()
            .insert(executor.task_type().to_string(), executor);
    }

    pub fn execute_task(&self, task: &FrameworkTask) -> Result<TaskResult, TaskError> {
        let executors = self.task_executors.read().unwrap();
        let executor = executors.get(&task.task_type).ok_or_else(|| {
            TaskError::ExecutionFailed(format!("no executor for task type: {}", task.task_type))
        })?;

        self.stats.tasks_executed.fetch_add(1, Ordering::Relaxed);
        match executor.execute(task) {
            Ok(result) => {
                if !result.success {
                    self.stats.tasks_failed.fetch_add(1, Ordering::Relaxed);
                    if result.retry {
                        self.stats.tasks_retried.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Ok(result)
            }
            Err(e) => {
                self.stats.tasks_failed.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    pub fn stats(&self) -> &TaskFrameworkStats {
        &self.stats
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Worker Versioning Depth (3,727 lines in Temporal)
// ═══════════════════════════════════════════════════════════════════════════════

pub struct VersioningManager {
    version_sets: RwLock<HashMap<String, VersionSet>>,
    redirect_rules: RwLock<Vec<VersionRedirectRule>>,
    stats: VersioningStats,
}

#[derive(Debug, Default)]
pub struct VersioningStats {
    pub assignments: AtomicU64,
    pub redirects: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct VersionSet {
    pub task_queue: String,
    pub build_ids: Vec<String>,
    pub default_build_id: String,
    pub redirect_rules: Vec<VersionRedirectRule>,
}

#[derive(Debug, Clone)]
pub struct VersionRedirectRule {
    pub source: String,
    pub target: String,
    pub created_at_ms: i64,
}

impl VersioningManager {
    pub fn new() -> Self {
        Self {
            version_sets: RwLock::new(HashMap::new()),
            redirect_rules: RwLock::new(Vec::new()),
            stats: VersioningStats::default(),
        }
    }

    pub fn create_version_set(&self, task_queue: &str, default_build_id: &str) -> String {
        let set = VersionSet {
            task_queue: task_queue.to_string(),
            build_ids: vec![default_build_id.to_string()],
            default_build_id: default_build_id.to_string(),
            redirect_rules: vec![],
        };
        self.version_sets
            .write()
            .unwrap()
            .insert(task_queue.to_string(), set);
        task_queue.to_string()
    }

    pub fn add_build_id(&self, task_queue: &str, build_id: &str) {
        if let Some(set) = self.version_sets.write().unwrap().get_mut(task_queue) {
            if !set.build_ids.contains(&build_id.to_string()) {
                set.build_ids.push(build_id.to_string());
            }
        }
    }

    pub fn set_default(&self, task_queue: &str, build_id: &str) {
        if let Some(set) = self.version_sets.write().unwrap().get_mut(task_queue) {
            set.default_build_id = build_id.to_string();
        }
    }

    pub fn resolve_build_id(&self, task_queue: &str, requested: &str) -> String {
        self.stats.assignments.fetch_add(1, Ordering::Relaxed);

        // Follow redirect chain
        let mut current = requested.to_string();
        let mut visited = HashSet::new();
        loop {
            if visited.contains(&current) {
                break;
            }
            visited.insert(current.clone());

            let rules = self.redirect_rules.read().unwrap();
            if let Some(rule) = rules.iter().find(|r| r.source == current) {
                current = rule.target.clone();
                self.stats.redirects.fetch_add(1, Ordering::Relaxed);
            } else {
                break;
            }
        }

        // Verify build ID exists in version set
        if let Some(set) = self.version_sets.read().unwrap().get(task_queue) {
            if set.build_ids.contains(&current) {
                return current;
            }
            return set.default_build_id.clone();
        }

        current
    }

    pub fn add_redirect_rule(&self, source: &str, target: &str) {
        self.redirect_rules
            .write()
            .unwrap()
            .push(VersionRedirectRule {
                source: source.to_string(),
                target: target.to_string(),
                created_at_ms: now_ms(),
            });
    }

    pub fn stats(&self) -> &VersioningStats {
        &self.stats
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quota_manager() {
        let mgr = QuotaManager::new();
        mgr.set_policy(QuotaPolicy {
            name: "api_calls".to_string(),
            namespace: "ns1".to_string(),
            calls_per_second: 2.0,
            burst: 2,
            max_concurrent: 5,
        });

        assert!(mgr.try_acquire("ns1", "api_calls"));
        assert!(mgr.try_acquire("ns1", "api_calls"));
        assert!(!mgr.try_acquire("ns1", "api_calls")); // Rate limited

        // Unknown quota -> allowed
        assert!(mgr.try_acquire("ns1", "unknown"));
    }

    #[test]
    fn test_quota_concurrent() {
        let mgr = QuotaManager::new();
        mgr.set_policy(QuotaPolicy {
            name: "concurrent_ops".to_string(),
            namespace: "ns1".to_string(),
            calls_per_second: 100.0,
            burst: 100,
            max_concurrent: 2,
        });

        assert!(mgr.try_acquire_concurrent("ns1", "concurrent_ops"));
        assert!(mgr.try_acquire_concurrent("ns1", "concurrent_ops"));
        assert!(!mgr.try_acquire_concurrent("ns1", "concurrent_ops")); // Max concurrent

        mgr.release_concurrent("ns1", "concurrent_ops");
        assert!(mgr.try_acquire_concurrent("ns1", "concurrent_ops"));
    }

    #[test]
    fn test_search_attribute_manager() {
        let mgr = SearchAttributeManager::new();

        // System attributes should exist
        assert!(mgr.get_attribute("WorkflowId").is_some());
        assert!(mgr.get_attribute("RunId").is_some());
        assert!(mgr.get_attribute("ExecutionStatus").is_some());

        // Validate values
        mgr.validate_value(
            "WorkflowId",
            &SearchAttributeValue::Keyword("wf-1".to_string()),
        )
        .unwrap();
        assert!(mgr
            .validate_value("WorkflowId", &SearchAttributeValue::Int(42))
            .is_err());

        // Register custom attribute
        mgr.register_custom_attribute("ns1", "CustomField", SearchAttributeFieldType::Text)
            .unwrap();
        assert!(mgr.get_attribute("CustomField").is_some());

        // Duplicate should fail
        assert!(mgr
            .register_custom_attribute("ns1", "CustomField", SearchAttributeFieldType::Text)
            .is_err());
    }

    #[test]
    fn test_search_attribute_list() {
        let mgr = SearchAttributeManager::new();
        let system = mgr.list_system_attributes();
        assert!(system.len() >= 15);

        mgr.register_custom_attribute("ns1", "MyAttr", SearchAttributeFieldType::Int)
            .unwrap();
        let custom = mgr.list_custom_attributes("ns1");
        assert_eq!(custom.len(), 1);
    }

    #[test]
    fn test_metrics_framework() {
        let fw = MetricsFramework::new();
        let scope = fw.create_scope(
            "test_scope",
            HashMap::from([("ns".to_string(), "ns1".to_string())]),
        );

        fw.record_counter(&scope, "workflow_started", 1);
        fw.record_counter(&scope, "workflow_started", 1);
        assert_eq!(fw.get_counter(&scope, "workflow_started"), 2);

        fw.record_gauge(&scope, "task_queue_depth", 42.0);
        assert_eq!(fw.get_gauge(&scope, "task_queue_depth"), Some(42.0));

        fw.record_timer(&scope, "workflow_task_latency", Duration::from_millis(100));
        assert_eq!(fw.stats().timers_recorded.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_metric_definitions() {
        let fw = MetricsFramework::new();
        let defs = fw.list_definitions();
        assert!(defs.len() >= 10);
        assert!(defs.iter().any(|d| d.name == "workflow_started"));
    }

    #[test]
    fn test_task_framework() {
        struct TestExecutor;
        impl TaskExecutor for TestExecutor {
            fn task_type(&self) -> &str {
                "transfer"
            }
            fn execute(&self, task: &FrameworkTask) -> Result<TaskResult, TaskError> {
                Ok(TaskResult {
                    success: true,
                    ack: true,
                    nack_reason: None,
                    retry: false,
                })
            }
        }

        let fw = TaskFramework::new();
        fw.register_executor(Arc::new(TestExecutor));

        let task = FrameworkTask {
            task_id: 1,
            task_type: "transfer".to_string(),
            namespace_id: "ns1".to_string(),
            workflow_id: "wf1".to_string(),
            run_id: "run1".to_string(),
            shard_id: 1,
            visibility_time_ms: 0,
            version: 1,
            payload: vec![],
            attempt: 1,
        };

        let result = fw.execute_task(&task).unwrap();
        assert!(result.success);
        assert_eq!(fw.stats().tasks_executed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_task_framework_unknown_type() {
        let fw = TaskFramework::new();
        let task = FrameworkTask {
            task_id: 1,
            task_type: "unknown".to_string(),
            namespace_id: "ns1".to_string(),
            workflow_id: "wf1".to_string(),
            run_id: "run1".to_string(),
            shard_id: 1,
            visibility_time_ms: 0,
            version: 1,
            payload: vec![],
            attempt: 1,
        };
        assert!(fw.execute_task(&task).is_err());
    }

    #[test]
    fn test_versioning_manager() {
        let mgr = VersioningManager::new();
        mgr.create_version_set("my-queue", "build-1");
        mgr.add_build_id("my-queue", "build-2");

        assert_eq!(mgr.resolve_build_id("my-queue", "build-1"), "build-1");
        assert_eq!(mgr.resolve_build_id("my-queue", "build-2"), "build-2");
        // Unknown build ID -> default
        assert_eq!(mgr.resolve_build_id("my-queue", "build-unknown"), "build-1");

        // Add redirect
        mgr.add_redirect_rule("build-1", "build-2");
        assert_eq!(mgr.resolve_build_id("my-queue", "build-1"), "build-2");
    }
}
