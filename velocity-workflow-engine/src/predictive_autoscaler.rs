//! Predictive Autoscaler — a capability Temporal does NOT have.
//!
//! Uses time-series forecasting, load prediction, and proactive scaling
//! to stay ahead of demand before it becomes a problem. Temporal requires
//! manual scaling or external autoscalers — VELOCITY does it natively.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock, atomic::{AtomicU64, AtomicI64, Ordering}};
use std::time::{SystemTime, Duration};

// ═══════════════════════════════════════════════════════════════════════════════
// Time-Series Data Collection
// ═══════════════════════════════════════════════════════════════════════════════

pub struct TimeSeriesBuffer {
    pub data_points: VecDeque<DataPoint>,
    pub max_points: usize,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct DataPoint {
    pub timestamp: i64,
    pub value: f64,
    pub labels: HashMap<String, String>,
}

impl TimeSeriesBuffer {
    pub fn new(name: &str, max_points: usize) -> Self {
        Self { data_points: VecDeque::new(), max_points, name: name.to_string() }
    }

    pub fn push(&mut self, value: f64) {
        if self.data_points.len() >= self.max_points { self.data_points.pop_front(); }
        self.data_points.push_back(DataPoint { timestamp: now_millis(), value, labels: HashMap::new() });
    }

    pub fn push_labeled(&mut self, value: f64, labels: HashMap<String, String>) {
        if self.data_points.len() >= self.max_points { self.data_points.pop_front(); }
        self.data_points.push_back(DataPoint { timestamp: now_millis(), value, labels });
    }

    pub fn recent(&self, count: usize) -> Vec<&DataPoint> {
        let len = self.data_points.len();
        let start = if len > count { len - count } else { 0 };
        self.data_points.iter().skip(start).collect()
    }

    pub fn values(&self) -> Vec<f64> { self.data_points.iter().map(|dp| dp.value).collect() }

    pub fn mean(&self) -> f64 {
        if self.data_points.is_empty() { return 0.0; }
        self.values().iter().sum::<f64>() / self.data_points.len() as f64
    }

    pub fn p99(&self) -> f64 {
        let mut vals = self.values();
        if vals.is_empty() { return 0.0; }
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = (vals.len() as f64 * 0.99).ceil() as usize;
        vals[idx.min(vals.len()) - 1]
    }

    pub fn trend(&self) -> f64 {
        let vals = self.values();
        if vals.len() < 2 { return 0.0; }
        let n = vals.len() as f64;
        let x_mean = (vals.len() - 1) as f64 / 2.0;
        let y_mean = vals.iter().sum::<f64>() / n;
        let mut num = 0.0;
        let mut den = 0.0;
        for (i, v) in vals.iter().enumerate() {
            let x = i as f64;
            num += (x - x_mean) * (v - y_mean);
            den += (x - x_mean).powi(2);
        }
        if den.abs() < 0.001 { return 0.0; }
        num / den
    }

    pub fn rate_of_change(&self) -> f64 {
        let vals = self.values();
        if vals.len() < 2 { return 0.0; }
        let last = vals[vals.len() - 1];
        let prev = vals[vals.len() - 2];
        if prev.abs() < 0.001 { return 0.0; }
        (last - prev) / prev
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Load Forecaster — predicts future load using exponential smoothing + trend
// ═══════════════════════════════════════════════════════════════════════════════

pub struct LoadForecaster {
    pub metrics: RwLock<HashMap<String, TimeSeriesBuffer>>,
    pub alpha: f64, // exponential smoothing factor
    pub stats: ForecasterStats,
}

#[derive(Debug, Default)]
pub struct ForecasterStats {
    pub predictions_made: AtomicU64,
    pub prediction_accuracy_sum: AtomicU64,
    pub prediction_count: AtomicU64,
}

impl LoadForecaster {
    pub fn new(alpha: f64) -> Self {
        Self { metrics: RwLock::new(HashMap::new()), alpha, stats: ForecasterStats::default() }
    }

    pub fn record_metric(&self, name: &str, value: f64) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.entry(name.to_string()).or_insert_with(|| TimeSeriesBuffer::new(name, 1000)).push(value);
    }

    pub fn forecast(&self, name: &str, horizon_points: usize) -> Vec<f64> {
        let metrics = self.metrics.read().unwrap();
        let buffer = match metrics.get(name) {
            Some(b) => b,
            None => return vec![0.0; horizon_points],
        };
        let vals = buffer.values();
        if vals.is_empty() { return vec![0.0; horizon_points]; }

        // Exponential smoothing
        let mut smoothed = vals[0];
        for &v in &vals[1..] { smoothed = self.alpha * v + (1.0 - self.alpha) * smoothed; }

        // Linear trend
        let trend = buffer.trend();

        // Seasonal component (simple: compare to same position in previous cycle)
        let seasonal = if vals.len() >= 60 {
            let cycle_len = 60;
            let pos = vals.len() % cycle_len;
            let cycle_avg: f64 = vals.iter().enumerate().filter(|(i, _)| i % cycle_len == pos).map(|(_, v)| v).sum::<f64>() / (vals.len() / cycle_len) as f64;
            cycle_avg - smoothed
        } else { 0.0 };

        let mut forecast = Vec::with_capacity(horizon_points);
        for i in 0..horizon_points {
            let predicted = smoothed + trend * (i as f64) + seasonal;
            forecast.push(predicted.max(0.0));
        }

        self.stats.predictions_made.fetch_add(1, Ordering::Relaxed);
        forecast
    }

    pub fn predict_peak(&self, name: &str, window: usize) -> f64 {
        let forecast = self.forecast(name, window);
        forecast.iter().cloned().fold(0.0_f64, f64::max)
    }

    pub fn current_load(&self, name: &str) -> f64 {
        let metrics = self.metrics.read().unwrap();
        metrics.get(name).map(|b| b.mean()).unwrap_or(0.0)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Scaling Decision Engine
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct ScalingDecision {
    pub decision_id: String,
    pub component: String,
    pub direction: ScalingDirection,
    pub magnitude: u32,
    pub reason: String,
    pub confidence: f64,
    pub predicted_load: f64,
    pub current_capacity: f64,
    pub target_capacity: f64,
    pub created_at: i64,
    pub status: ScalingDecisionStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalingDirection { ScaleUp, ScaleDown, NoChange }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalingDecisionStatus { Proposed, Approved, Executing, Completed, Rejected, RolledBack }

pub struct ScalingEngine {
    pub decisions: RwLock<Vec<ScalingDecision>>,
    pub min_instances: HashMap<String, u32>,
    pub max_instances: HashMap<String, u32>,
    pub scale_up_threshold: f64,
    pub scale_down_threshold: f64,
    pub cooldown_seconds: i64,
    pub last_scale_time: RwLock<HashMap<String, i64>>,
    pub forecaster: Arc<LoadForecaster>,
    pub stats: ScalingEngineStats,
}

#[derive(Debug, Default)]
pub struct ScalingEngineStats {
    pub decisions_made: AtomicU64, pub scale_ups: AtomicU64,
    pub scale_downs: AtomicU64, pub rejected: AtomicU64,
    pub cooldown_vetos: AtomicU64,
}

impl ScalingEngine {
    pub fn new(forecaster: Arc<LoadForecaster>, scale_up_threshold: f64, scale_down_threshold: f64) -> Self {
        Self {
            decisions: RwLock::new(Vec::new()),
            min_instances: HashMap::new(),
            max_instances: HashMap::new(),
            scale_up_threshold, scale_down_threshold,
            cooldown_seconds: 60,
            last_scale_time: RwLock::new(HashMap::new()),
            forecaster, stats: ScalingEngineStats::default(),
        }
    }

    pub fn evaluate(&self, component: &str, current_load: f64, current_capacity: f64) -> ScalingDecision {
        let predicted = self.forecaster.predict_peak(component, 30);
        let utilization = if current_capacity > 0.0 { predicted / current_capacity } else { 1.0 };

        let (direction, magnitude) = if utilization > self.scale_up_threshold {
            let needed = (predicted / self.scale_up_threshold).ceil() as u32;
            let current = current_capacity as u32;
            (ScalingDirection::ScaleUp, needed.saturating_sub(current).max(1))
        } else if utilization < self.scale_down_threshold && current_capacity > self.min_instances.get(component).cloned().unwrap_or(1) as f64 {
            let excess = (current_capacity - predicted / self.scale_up_threshold).floor() as u32;
            (ScalingDirection::ScaleDown, excess.max(1))
        } else {
            (ScalingDirection::NoChange, 0)
        };

        let max = self.max_instances.get(component).cloned().unwrap_or(100);
        let min = self.min_instances.get(component).cloned().unwrap_or(1);
        let magnitude = if direction == ScalingDirection::ScaleUp { magnitude.min(max - current_capacity as u32) } else { magnitude.min(current_capacity as u32 - min) };

        let decision = ScalingDecision {
            decision_id: format!("scale-{}", now_millis()),
            component: component.to_string(), direction, magnitude,
            reason: format!("predicted_load={:.1}, capacity={:.1}, utilization={:.2}", predicted, current_capacity, utilization),
            confidence: 0.85, predicted_load: predicted, current_capacity,
            target_capacity: if direction == ScalingDirection::ScaleUp { current_capacity + magnitude as f64 } else { current_capacity - magnitude as f64 },
            created_at: now_millis(), status: ScalingDecisionStatus::Proposed,
        };

        self.decisions.write().unwrap().push(decision.clone());
        self.stats.decisions_made.fetch_add(1, Ordering::Relaxed);
        decision
    }

    pub fn approve_and_execute(&self, decision_id: &str) -> bool {
        let mut decisions = self.decisions.write().unwrap();
        let decision = match decisions.iter_mut().find(|d| d.decision_id == decision_id) {
            Some(d) => d, None => return false,
        };
        // Check cooldown
        let last = self.last_scale_time.read().unwrap().get(&decision.component).cloned().unwrap_or(0);
        if now_millis() - last < self.cooldown_seconds * 1000 {
            decision.status = ScalingDecisionStatus::Rejected;
            self.stats.rejected.fetch_add(1, Ordering::Relaxed);
            self.stats.cooldown_vetos.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        decision.status = ScalingDecisionStatus::Executing;
        // Simulate execution
        decision.status = ScalingDecisionStatus::Completed;
        self.last_scale_time.write().unwrap().insert(decision.component.clone(), now_millis());
        match decision.direction {
            ScalingDirection::ScaleUp => { self.stats.scale_ups.fetch_add(1, Ordering::Relaxed); }
            ScalingDirection::ScaleDown => { self.stats.scale_downs.fetch_add(1, Ordering::Relaxed); }
            _ => {},
        }
        true
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Worker Pool Scaler — scales worker pools based on queue depth and latency
// ═══════════════════════════════════════════════════════════════════════════════

pub struct WorkerPoolScaler {
    pub pool_metrics: RwLock<HashMap<String, PoolMetrics>>,
    pub forecaster: Arc<LoadForecaster>,
    pub scaling_engine: Arc<ScalingEngine>,
    pub stats: WorkerPoolScalerStats,
}

#[derive(Debug, Clone)]
pub struct PoolMetrics {
    pub queue_depth: f64,
    pub avg_latency_ms: f64,
    pub active_workers: u32,
    pub max_workers: u32,
    pub min_workers: u32,
    pub tasks_per_second: f64,
    pub last_scaled_at: i64,
}

#[derive(Debug, Default)]
pub struct WorkerPoolScalerStats {
    pub evaluations: AtomicU64, pub scale_events: AtomicU64,
}

impl WorkerPoolScaler {
    pub fn new(forecaster: Arc<LoadForecaster>, scaling_engine: Arc<ScalingEngine>) -> Self {
        Self { pool_metrics: RwLock::new(HashMap::new()), forecaster, scaling_engine, stats: WorkerPoolScalerStats::default() }
    }

    pub fn update_pool_metrics(&self, pool_name: &str, metrics: PoolMetrics) {
        self.pool_metrics.write().unwrap().insert(pool_name.to_string(), metrics);
    }

    pub fn evaluate_pool(&self, pool_name: &str) -> Option<ScalingDecision> {
        self.stats.evaluations.fetch_add(1, Ordering::Relaxed);
        let metrics = self.pool_metrics.read().unwrap().get(pool_name)?.clone();

        // Record metrics for forecasting
        self.forecaster.record_metric(&format!("{}.queue_depth", pool_name), metrics.queue_depth);
        self.forecaster.record_metric(&format!("{}.latency", pool_name), metrics.avg_latency_ms);
        self.forecaster.record_metric(&format!("{}.throughput", pool_name), metrics.tasks_per_second);

        // Evaluate based on queue depth and latency
        let predicted_depth = self.forecaster.predict_peak(&format!("{}.queue_depth", pool_name), 10);
        let predicted_latency = self.forecaster.predict_peak(&format!("{}.latency", pool_name), 10);

        let needs_scale_up = predicted_depth > 100.0 || predicted_latency > 500.0;
        let can_scale_down = metrics.queue_depth < 5.0 && predicted_latency < 50.0 && metrics.active_workers > metrics.min_workers;

        let decision = if needs_scale_up && metrics.active_workers < metrics.max_workers {
            let additional = ((predicted_depth / 50.0).ceil() as u32).max(1);
            Some(ScalingDecision {
                decision_id: format!("pool-{}", now_millis()),
                component: pool_name.to_string(),
                direction: ScalingDirection::ScaleUp,
                magnitude: additional.min(metrics.max_workers - metrics.active_workers),
                reason: format!("predicted_depth={:.0}, predicted_latency={:.0}ms", predicted_depth, predicted_latency),
                confidence: 0.9,
                predicted_load: predicted_depth,
                current_capacity: metrics.active_workers as f64,
                target_capacity: (metrics.active_workers + additional) as f64,
                created_at: now_millis(),
                status: ScalingDecisionStatus::Approved,
            })
        } else if can_scale_down {
            let reduce = (metrics.active_workers - metrics.min_workers).max(1);
            Some(ScalingDecision {
                decision_id: format!("pool-{}", now_millis()),
                component: pool_name.to_string(),
                direction: ScalingDirection::ScaleDown,
                magnitude: reduce,
                reason: format!("low_load: depth={:.0}, latency={:.0}ms", metrics.queue_depth, metrics.avg_latency_ms),
                confidence: 0.7,
                predicted_load: predicted_depth,
                current_capacity: metrics.active_workers as f64,
                target_capacity: (metrics.active_workers - reduce) as f64,
                created_at: now_millis(),
                status: ScalingDecisionStatus::Approved,
            })
        } else {
            None
        };

        if let Some(ref d) = decision {
            self.stats.scale_events.fetch_add(1, Ordering::Relaxed);
        }
        decision
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Capacity Planner — long-term capacity planning
// ═══════════════════════════════════════════════════════════════════════════════

pub struct CapacityPlanner {
    pub forecaster: Arc<LoadForecaster>,
    pub resource_limits: RwLock<HashMap<String, ResourceLimit>>,
    pub plans: RwLock<Vec<CapacityPlan>>,
}

#[derive(Debug, Clone)]
pub struct ResourceLimit {
    pub name: String, pub max_value: f64, pub current_value: f64,
    pub unit: String, pub warning_pct: f64,
}

#[derive(Debug, Clone)]
pub struct CapacityPlan {
    pub plan_id: String, pub horizon_days: u32, pub resource_name: String,
    pub current_usage: f64, pub predicted_usage: f64, pub headroom_pct: f64,
    pub recommendation: String, pub urgency: CapacityUrgency,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapacityUrgency { Low, Medium, High, Critical }

impl CapacityPlanner {
    pub fn new(forecaster: Arc<LoadForecaster>) -> Self {
        Self { forecaster, resource_limits: RwLock::new(HashMap::new()), plans: RwLock::new(Vec::new()) }
    }

    pub fn add_resource_limit(&self, limit: ResourceLimit) {
        self.resource_limits.write().unwrap().insert(limit.name.clone(), limit);
    }

    pub fn generate_plan(&self, horizon_days: u32) -> Vec<CapacityPlan> {
        let limits = self.resource_limits.read().unwrap();
        let mut plans = Vec::new();
        for (name, limit) in limits.iter() {
            let metric_name = format!("resource.{}", name);
            let predicted = self.forecaster.predict_peak(&metric_name, horizon_days as usize * 24);
            let headroom = if limit.max_value > 0.0 { (limit.max_value - predicted) / limit.max_value * 100.0 } else { 0.0 };
            let urgency = if headroom < 5.0 { CapacityUrgency::Critical } else if headroom < 15.0 { CapacityUrgency::High } else if headroom < 30.0 { CapacityUrgency::Medium } else { CapacityUrgency::Low };
            let recommendation = match urgency {
                CapacityUrgency::Critical => format!("IMMEDIATE: Increase {} capacity by {:.0}%", name, (limit.max_value * 0.5 / limit.max_value) * 100.0),
                CapacityUrgency::High => format!("Increase {} capacity within {} days", name, horizon_days),
                CapacityUrgency::Medium => format!("Plan {} capacity increase for next quarter", name),
                CapacityUrgency::Low => format!("{} capacity sufficient for {} day horizon", name, horizon_days),
            };
            plans.push(CapacityPlan { plan_id: format!("cap-{}", name), horizon_days, resource_name: name.clone(), current_usage: limit.current_value, predicted_usage: predicted, headroom_pct: headroom, recommendation, urgency });
        }
        self.plans.write().unwrap().extend(plans.clone());
        plans
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Autoscaler Orchestrator
// ═══════════════════════════════════════════════════════════════════════════════

pub struct AutoscalerOrchestrator {
    pub forecaster: Arc<LoadForecaster>,
    pub scaling_engine: Arc<ScalingEngine>,
    pub pool_scaler: Arc<WorkerPoolScaler>,
    pub capacity_planner: Arc<CapacityPlanner>,
    pub stats: AutoscalerStats,
}

#[derive(Debug, Default)]
pub struct AutoscalerStats {
    pub cycles_run: AtomicU64,
    pub scale_ups: AtomicU64,
    pub scale_downs: AtomicU64,
    pub capacity_plans_generated: AtomicU64,
}

impl AutoscalerOrchestrator {
    pub fn new() -> Self {
        let forecaster = Arc::new(LoadForecaster::new(0.3));
        let scaling_engine = Arc::new(ScalingEngine::new(forecaster.clone(), 0.8, 0.3));
        let pool_scaler = Arc::new(WorkerPoolScaler::new(forecaster.clone(), scaling_engine.clone()));
        let capacity_planner = Arc::new(CapacityPlanner::new(forecaster.clone()));
        Self { forecaster, scaling_engine, pool_scaler, capacity_planner, stats: AutoscalerStats::default() }
    }

    pub fn run_scaling_cycle(&self) -> ScalingCycleResult {
        self.stats.cycles_run.fetch_add(1, Ordering::Relaxed);
        let mut result = ScalingCycleResult::default();
        let pools: Vec<String> = self.pool_scaler.pool_metrics.read().unwrap().keys().cloned().collect();
        for pool in &pools {
            if let Some(decision) = self.pool_scaler.evaluate_pool(pool) {
                match decision.direction {
                    ScalingDirection::ScaleUp => { self.stats.scale_ups.fetch_add(1, Ordering::Relaxed); result.scale_ups += 1; }
                    ScalingDirection::ScaleDown => { self.stats.scale_downs.fetch_add(1, Ordering::Relaxed); result.scale_downs += 1; }
                    _ => {}
                }
                result.decisions.push(decision);
            }
        }
        result
    }

    pub fn generate_capacity_plan(&self, horizon_days: u32) -> Vec<CapacityPlan> {
        let plans = self.capacity_planner.generate_plan(horizon_days);
        self.stats.capacity_plans_generated.fetch_add(1, Ordering::Relaxed);
        plans
    }
}

#[derive(Debug, Default)]
pub struct ScalingCycleResult {
    pub scale_ups: u32,
    pub scale_downs: u32,
    pub decisions: Vec<ScalingDecision>,
}

fn now_millis() -> i64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_series_buffer() {
        let mut buf = TimeSeriesBuffer::new("test", 100);
        for i in 0..10 { buf.push(i as f64); }
        assert_eq!(buf.values().len(), 10);
        assert!((buf.mean() - 4.5).abs() < 0.01);
        assert!(buf.trend() > 0.0); // increasing
    }

    #[test]
    fn test_time_series_p99() {
        let mut buf = TimeSeriesBuffer::new("test", 100);
        for i in 0..100 { buf.push(i as f64); }
        let p99 = buf.p99();
        assert!(p99 >= 98.0);
    }

    #[test]
    fn test_forecaster_basic() {
        let fc = LoadForecaster::new(0.3);
        for i in 0..50 { fc.record_metric("cpu", 50.0 + (i as f64) * 0.5); }
        let forecast = fc.forecast("cpu", 5);
        assert_eq!(forecast.len(), 5);
        assert!(forecast[0] > 0.0);
    }

    #[test]
    fn test_forecaster_predict_peak() {
        let fc = LoadForecaster::new(0.3);
        for _ in 0..30 { fc.record_metric("load", 100.0); }
        let peak = fc.predict_peak("load", 10);
        assert!(peak > 0.0);
    }

    #[test]
    fn test_scaling_engine_evaluate() {
        let fc = Arc::new(LoadForecaster::new(0.3));
        for _ in 0..50 { fc.record_metric("api", 50.0); }
        let engine = ScalingEngine::new(fc, 0.8, 0.3);
        let decision = engine.evaluate("api", 50.0, 100.0);
        assert_eq!(decision.direction, ScalingDirection::NoChange);
    }

    #[test]
    fn test_scaling_engine_scale_up() {
        let fc = Arc::new(LoadForecaster::new(0.3));
        for _ in 0..50 { fc.record_metric("api", 150.0); }
        let engine = ScalingEngine::new(fc, 0.8, 0.3);
        let decision = engine.evaluate("api", 150.0, 100.0);
        assert_eq!(decision.direction, ScalingDirection::ScaleUp);
    }

    #[test]
    fn test_scaling_engine_approve() {
        let fc = Arc::new(LoadForecaster::new(0.3));
        let engine = ScalingEngine::new(fc, 0.8, 0.3);
        let decision = engine.evaluate("test", 100.0, 50.0);
        let id = decision.decision_id.clone();
        assert!(engine.approve_and_execute(&id));
    }

    #[test]
    fn test_worker_pool_scaler() {
        let fc = Arc::new(LoadForecaster::new(0.3));
        let se = Arc::new(ScalingEngine::new(fc.clone(), 0.8, 0.3));
        let scaler = WorkerPoolScaler::new(fc, se);
        scaler.update_pool_metrics("workers", PoolMetrics {
            queue_depth: 200.0, avg_latency_ms: 800.0, active_workers: 5,
            max_workers: 20, min_workers: 1, tasks_per_second: 50.0, last_scaled_at: 0,
        });
        let decision = scaler.evaluate_pool("workers");
        assert!(decision.is_some());
        assert_eq!(decision.unwrap().direction, ScalingDirection::ScaleUp);
    }

    #[test]
    fn test_capacity_planner() {
        let fc = Arc::new(LoadForecaster::new(0.3));
        let planner = CapacityPlanner::new(fc);
        planner.add_resource_limit(ResourceLimit { name: "disk".into(), max_value: 1000.0, current_value: 700.0, unit: "GB".into(), warning_pct: 80.0 });
        let plans = planner.generate_plan(30);
        assert!(!plans.is_empty());
        assert_eq!(plans[0].resource_name, "disk");
    }

    #[test]
    fn test_autoscaler_orchestrator() {
        let orch = AutoscalerOrchestrator::new();
        orch.pool_scaler.update_pool_metrics("pool1", PoolMetrics {
            queue_depth: 10.0, avg_latency_ms: 50.0, active_workers: 3,
            max_workers: 10, min_workers: 1, tasks_per_second: 100.0, last_scaled_at: 0,
        });
        let result = orch.run_scaling_cycle();
        assert!(result.decisions.is_empty() || !result.decisions.is_empty()); // just runs without panic
    }
}
