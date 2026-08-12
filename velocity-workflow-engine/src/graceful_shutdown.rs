//! Graceful shutdown controller for coordinated engine teardown.
//!
//! Manages ordered shutdown of engine components: signal all components to stop accepting
//! new work, wait for in-flight workflows to drain, and force-terminate if the drain timeout
//! expires. Thread-safe via `Arc<AtomicBool>` + `Mutex` + `Condvar`.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

// ─── Configuration ────────────────────────────────────────────────────────────

/// Configuration for graceful shutdown behaviour.
#[derive(Debug, Clone)]
pub struct GracefulShutdownConfig {
    /// Maximum time to wait for all components to drain before forcing shutdown.
    pub drain_timeout_ms: u64,
    /// Hard deadline: force-terminate all work after this many milliseconds from
    /// `initiate_shutdown`, regardless of drain state.
    pub force_shutdown_after_ms: u64,
}

impl GracefulShutdownConfig {
    /// Create a new configuration with the given timeouts.
    pub fn new(drain_timeout_ms: u64, force_shutdown_after_ms: u64) -> Self {
        Self {
            drain_timeout_ms,
            force_shutdown_after_ms,
        }
    }

    /// Sensible production defaults: 30 s drain, 60 s hard deadline.
    pub fn production_defaults() -> Self {
        Self {
            drain_timeout_ms: 30_000,
            force_shutdown_after_ms: 60_000,
        }
    }
}

impl Default for GracefulShutdownConfig {
    fn default() -> Self {
        Self::production_defaults()
    }
}

// ─── Shutdown Status ──────────────────────────────────────────────────────────

/// Snapshot of the current shutdown progress.
#[derive(Debug, Clone)]
pub struct ShutdownStatus {
    /// Whether `initiate_shutdown` has been called.
    pub shutting_down: bool,
    /// Names of components that have NOT yet reported drained.
    pub components_remaining: Vec<String>,
    /// Number of workflows still considered in-flight (set by the engine).
    pub in_flight_workflows: u64,
    /// Milliseconds elapsed since `initiate_shutdown` was called (0 if not started).
    pub elapsed_ms: u64,
    /// Whether `force_shutdown` has been called.
    pub force_shutdown: bool,
}

impl ShutdownStatus {
    /// Returns true when every registered component has drained.
    pub fn is_fully_drained(&self) -> bool {
        self.components_remaining.is_empty()
    }
}

// ─── Controller ───────────────────────────────────────────────────────────────

/// Coordinates the graceful shutdown of all engine components.
///
/// # Lifecycle
/// 1. Components register themselves via [`register_component`].
/// 2. On shutdown signal, call [`initiate_shutdown`].
/// 3. Each component drains and calls [`mark_component_drained`].
/// 4. The caller polls [`shutdown_status`] or uses [`wait_for_drain`].
/// 5. If the timeout expires, call [`force_shutdown`].
pub struct ShutdownController {
    /// All registered component names.
    registered: Mutex<HashSet<String>>,
    /// Components that have reported drained.
    drained: Mutex<HashSet<String>>,
    /// Global shutdown flag — checked by all components.
    shutdown_flag: Arc<AtomicBool>,
    /// Hard force-shutdown flag — components must abort immediately.
    force_flag: Arc<AtomicBool>,
    /// In-flight workflow counter maintained by the engine.
    in_flight: Arc<AtomicU64>,
    /// Condvar used to wake waiters when a component drains.
    drain_condvar: Arc<Condvar>,
    /// Mutex paired with `drain_condvar`.
    drain_mutex: Arc<Mutex<()>>,
    /// Instant when `initiate_shutdown` was called (None if not yet).
    shutdown_started: Mutex<Option<Instant>>,
    /// Configuration.
    config: GracefulShutdownConfig,
}

impl ShutdownController {
    /// Create a new controller with the given configuration.
    pub fn new(config: GracefulShutdownConfig) -> Self {
        Self {
            registered: Mutex::new(HashSet::new()),
            drained: Mutex::new(HashSet::new()),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            force_flag: Arc::new(AtomicBool::new(false)),
            in_flight: Arc::new(AtomicU64::new(0)),
            drain_condvar: Arc::new(Condvar::new()),
            drain_mutex: Arc::new(Mutex::new(())),
            shutdown_started: Mutex::new(None),
            config,
        }
    }

    /// Create a controller with production default configuration.
    pub fn with_defaults() -> Self {
        Self::new(GracefulShutdownConfig::default())
    }

    /// Register a component that should be tracked during shutdown.
    ///
    /// Must be called **before** `initiate_shutdown`. Returns `false` if the
    /// component was already registered.
    pub fn register_component(&self, name: &str) -> bool {
        let mut set = self.registered.lock().unwrap();
        set.insert(name.to_string())
    }

    /// Mark a previously-registered component as fully drained.
    ///
    /// Wakes any threads blocked in `wait_for_drain`. Returns `false` if the
    /// component was not registered or was already marked drained.
    pub fn mark_component_drained(&self, name: &str) -> bool {
        let registered = self.registered.lock().unwrap();
        if !registered.contains(name) {
            return false;
        }
        drop(registered);

        let mut drained = self.drained.lock().unwrap();
        let inserted = drained.insert(name.to_string());
        drop(drained);

        // Wake waiters
        let _guard = self.drain_mutex.lock().unwrap();
        self.drain_condvar.notify_all();
        inserted
    }

    /// Signal all components to stop accepting new work.
    ///
    /// Idempotent — calling more than once has no additional effect.
    pub fn initiate_shutdown(&self) {
        let already = self.shutdown_flag.swap(true, Ordering::AcqRel);
        if !already {
            let mut started = self.shutdown_started.lock().unwrap();
            *started = Some(Instant::now());
        }
    }

    /// Block until all registered components have drained or `timeout` elapses.
    ///
    /// Returns `true` if all components drained within the timeout, `false` otherwise.
    pub fn wait_for_drain(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let guard = self.drain_mutex.lock().unwrap();
        self.wait_drain_loop(guard, deadline)
    }

    fn wait_drain_loop(&self, guard: std::sync::MutexGuard<'_, ()>, deadline: Instant) -> bool {
        let mut guard = guard;
        loop {
            if self.all_drained() {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let remaining = deadline - now;
            let (new_guard, _) = self.drain_condvar.wait_timeout(guard, remaining).unwrap();
            guard = new_guard;
            if self.all_drained() {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
        }
    }

    /// Immediately terminate all work. Sets the force-shutdown flag.
    pub fn force_shutdown(&self) {
        self.force_flag.store(true, Ordering::Release);
        self.shutdown_flag.store(true, Ordering::Release);
        // Wake all waiters so they can observe the force flag
        let _guard = self.drain_mutex.lock().unwrap();
        self.drain_condvar.notify_all();
    }

    /// Returns `true` once `initiate_shutdown` has been called.
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown_flag.load(Ordering::Acquire)
    }

    /// Returns `true` once `force_shutdown` has been called.
    pub fn is_force_shutdown(&self) -> bool {
        self.force_flag.load(Ordering::Acquire)
    }

    /// Get a snapshot of the current shutdown status.
    pub fn shutdown_status(&self) -> ShutdownStatus {
        let registered = self.registered.lock().unwrap();
        let drained = self.drained.lock().unwrap();
        let started = self.shutdown_started.lock().unwrap();

        let remaining: Vec<String> = registered.difference(&drained).cloned().collect();

        let elapsed_ms = started.map(|s| s.elapsed().as_millis() as u64).unwrap_or(0);

        ShutdownStatus {
            shutting_down: self.shutdown_flag.load(Ordering::Acquire),
            components_remaining: remaining,
            in_flight_workflows: self.in_flight.load(Ordering::Acquire),
            elapsed_ms,
            force_shutdown: self.force_flag.load(Ordering::Acquire),
        }
    }

    /// Set the number of in-flight workflows (called by the engine).
    pub fn set_in_flight_workflows(&self, count: u64) {
        self.in_flight.store(count, Ordering::Release);
    }

    /// Decrement the in-flight workflow counter.
    pub fn decrement_in_flight(&self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
    }

    /// Increment the in-flight workflow counter.
    pub fn increment_in_flight(&self) {
        self.in_flight.fetch_add(1, Ordering::AcqRel);
    }

    /// Get a shared reference to the shutdown flag (for components to poll).
    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown_flag)
    }

    /// Get a shared reference to the force-shutdown flag.
    pub fn force_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.force_flag)
    }

    /// Get the configured drain timeout.
    pub fn drain_timeout(&self) -> Duration {
        Duration::from_millis(self.config.drain_timeout_ms)
    }

    /// Get the configured force-shutdown deadline.
    pub fn force_shutdown_deadline(&self) -> Duration {
        Duration::from_millis(self.config.force_shutdown_after_ms)
    }

    /// Returns the list of all registered component names.
    pub fn registered_components(&self) -> Vec<String> {
        self.registered.lock().unwrap().iter().cloned().collect()
    }

    /// Returns the list of components that have drained.
    pub fn drained_components(&self) -> Vec<String> {
        self.drained.lock().unwrap().iter().cloned().collect()
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    fn all_drained(&self) -> bool {
        let registered = self.registered.lock().unwrap();
        let drained = self.drained.lock().unwrap();
        registered.is_subset(&drained)
    }
}

impl Default for ShutdownController {
    fn default() -> Self {
        Self::with_defaults()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_drain() {
        let ctrl = ShutdownController::with_defaults();
        assert!(ctrl.register_component("task_queue"));
        assert!(ctrl.register_component("timer_engine"));
        assert!(!ctrl.register_component("task_queue")); // duplicate

        ctrl.initiate_shutdown();
        assert!(ctrl.is_shutting_down());

        let status = ctrl.shutdown_status();
        assert_eq!(status.components_remaining.len(), 2);

        ctrl.mark_component_drained("task_queue");
        let status = ctrl.shutdown_status();
        assert_eq!(status.components_remaining.len(), 1);
        assert!(status
            .components_remaining
            .contains(&"timer_engine".to_string()));

        ctrl.mark_component_drained("timer_engine");
        let status = ctrl.shutdown_status();
        assert!(status.is_fully_drained());
    }

    #[test]
    fn test_force_shutdown() {
        let ctrl = ShutdownController::with_defaults();
        ctrl.register_component("worker");
        ctrl.initiate_shutdown();
        assert!(!ctrl.is_force_shutdown());

        ctrl.force_shutdown();
        assert!(ctrl.is_force_shutdown());
        assert!(ctrl.is_shutting_down());
    }

    #[test]
    fn test_wait_for_drain_success() {
        let ctrl = Arc::new(ShutdownController::with_defaults());
        ctrl.register_component("a");
        ctrl.register_component("b");
        ctrl.initiate_shutdown();

        let ctrl2 = Arc::clone(&ctrl);
        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            ctrl2.mark_component_drained("a");
            std::thread::sleep(Duration::from_millis(50));
            ctrl2.mark_component_drained("b");
        });

        let drained = ctrl.wait_for_drain(Duration::from_secs(5));
        assert!(drained);
        handle.join().unwrap();
    }

    #[test]
    fn test_wait_for_drain_timeout() {
        let ctrl = ShutdownController::with_defaults();
        ctrl.register_component("slow");
        ctrl.initiate_shutdown();

        let drained = ctrl.wait_for_drain(Duration::from_millis(50));
        assert!(!drained);
    }

    #[test]
    fn test_in_flight_tracking() {
        let ctrl = ShutdownController::with_defaults();
        ctrl.increment_in_flight();
        ctrl.increment_in_flight();
        ctrl.increment_in_flight();

        let status = ctrl.shutdown_status();
        assert_eq!(status.in_flight_workflows, 3);

        ctrl.decrement_in_flight();
        let status = ctrl.shutdown_status();
        assert_eq!(status.in_flight_workflows, 2);

        ctrl.set_in_flight_workflows(0);
        let status = ctrl.shutdown_status();
        assert_eq!(status.in_flight_workflows, 0);
    }

    #[test]
    fn test_idempotent_initiate_shutdown() {
        let ctrl = ShutdownController::with_defaults();
        ctrl.initiate_shutdown();
        let status1 = ctrl.shutdown_status();
        ctrl.initiate_shutdown(); // second call — should not reset timer
        let status2 = ctrl.shutdown_status();
        assert!(status1.shutting_down);
        assert!(status2.shutting_down);
        // elapsed should be monotonically non-decreasing
        assert!(status2.elapsed_ms >= status1.elapsed_ms);
    }

    #[test]
    fn test_mark_unregistered_component() {
        let ctrl = ShutdownController::with_defaults();
        // Marking a component that was never registered returns false
        assert!(!ctrl.mark_component_drained("nonexistent"));
    }

    #[test]
    fn test_shutdown_status_before_shutdown() {
        let ctrl = ShutdownController::with_defaults();
        ctrl.register_component("x");
        let status = ctrl.shutdown_status();
        assert!(!status.shutting_down);
        assert!(!status.force_shutdown);
        assert_eq!(status.elapsed_ms, 0);
        assert_eq!(status.in_flight_workflows, 0);
    }

    #[test]
    fn test_config_defaults() {
        let cfg = GracefulShutdownConfig::default();
        assert_eq!(cfg.drain_timeout_ms, 30_000);
        assert_eq!(cfg.force_shutdown_after_ms, 60_000);
    }
}
