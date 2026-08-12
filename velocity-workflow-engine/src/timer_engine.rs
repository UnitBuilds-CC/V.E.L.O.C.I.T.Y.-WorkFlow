//! Durable timer engine. Manages a `BinaryHeap` of pending timers sorted by fire time.
//! A background thread checks the heap and fires timers when their deadline expires.
//! Zero managed heap — all timer state lives in Rust-owned memory.

use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex, Condvar, atomic::{AtomicBool, Ordering}};
use std::time::{Instant, Duration};
use std::thread;

/// A pending timer entry.
#[derive(Debug, Clone, Eq, PartialEq)]
struct TimerEntry {
    fire_at: Instant,
    workflow_key: u64,
    timer_id: u64,
}

impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse order for min-heap (earliest fire_at first)
        other.fire_at.cmp(&self.fire_at)
    }
}

impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Callback invoked when a timer fires. The engine passes the workflow key and timer ID.
pub type TimerCallback = Box<dyn Fn(u64, u64) + Send + 'static>;

/// Durable timer engine with a background checker thread.
pub struct TimerEngine {
    heap: Arc<Mutex<BinaryHeap<TimerEntry>>>,
    condvar: Arc<Condvar>,
    shutdown: Arc<AtomicBool>,
    next_timer_id: Mutex<u64>,
    /// Optional callback invoked on timer fire (set by the engine for integration).
    on_fire: Arc<Mutex<Option<TimerCallback>>>,
}

impl TimerEngine {
    pub fn new() -> Self {
        Self {
            heap: Arc::new(Mutex::new(BinaryHeap::new())),
            condvar: Arc::new(Condvar::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
            next_timer_id: Mutex::new(1),
            on_fire: Arc::new(Mutex::new(None)),
        }
    }

    /// Set the callback invoked when a timer fires.
    pub fn set_fire_callback(&self, cb: TimerCallback) {
        let mut guard = self.on_fire.lock().unwrap();
        *guard = Some(cb);
    }

    /// Schedule a timer that fires after `delay`. Returns the timer ID.
    pub fn schedule(&self, workflow_key: u64, delay: Duration) -> u64 {
        let mut id_lock = self.next_timer_id.lock().unwrap();
        let timer_id = *id_lock;
        *id_lock += 1;
        drop(id_lock);

        let entry = TimerEntry {
            fire_at: Instant::now() + delay,
            workflow_key,
            timer_id,
        };

        let mut heap = self.heap.lock().unwrap();
        heap.push(entry);
        drop(heap);

        self.condvar.notify_one();
        timer_id
    }

    /// Cancel a pending timer. Returns true if the timer was found and cancelled.
    pub fn cancel(&self, timer_id: u64) -> bool {
        let mut heap = self.heap.lock().unwrap();
        let before = heap.len();
        let entries: Vec<TimerEntry> = heap.drain().collect();
        for entry in entries {
            if entry.timer_id != timer_id {
                heap.push(entry);
            }
        }
        heap.len() < before
    }

    /// Returns the number of pending timers.
    pub fn pending_count(&self) -> usize {
        self.heap.lock().unwrap().len()
    }

    /// Start the background timer checker thread.
    pub fn start(&self) -> thread::JoinHandle<()> {
        let heap = Arc::clone(&self.heap);
        let condvar = Arc::clone(&self.condvar);
        let shutdown = Arc::clone(&self.shutdown);
        let on_fire = Arc::clone(&self.on_fire);

        thread::spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                let fired = {
                    let mut heap = heap.lock().unwrap();
                    let now = Instant::now();
                    let mut fired = Vec::new();

                    // Pop all timers that have expired
                    while let Some(entry) = heap.peek() {
                        if entry.fire_at <= now {
                            fired.push(heap.pop().unwrap());
                        } else {
                            break;
                        }
                    }

                    if fired.is_empty() {
                        // Calculate how long to wait for the next timer
                        let wait_duration = heap.peek()
                            .map(|e| e.fire_at.duration_since(now))
                            .unwrap_or(Duration::from_millis(500));

                        // Wait with timeout — will be woken if a new timer is added
                        let _ = condvar.wait_timeout(heap, wait_duration.min(Duration::from_millis(100))).unwrap();
                    }

                    fired
                };

                // Fire callbacks outside the lock
                if !fired.is_empty() {
                    let cb = on_fire.lock().unwrap();
                    if let Some(ref callback) = *cb {
                        for entry in &fired {
                            callback(entry.workflow_key, entry.timer_id);
                        }
                    }
                }
            }
        })
    }

    /// Signal the engine to shut down.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.condvar.notify_all();
    }
}

impl Default for TimerEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schedule_and_cancel() {
        let engine = TimerEngine::new();
        let id = engine.schedule(42, Duration::from_secs(60));
        assert_eq!(engine.pending_count(), 1);
        assert!(engine.cancel(id));
        assert_eq!(engine.pending_count(), 0);
    }

    #[test]
    fn test_timer_fires() {
        let engine = TimerEngine::new();
        let fired = Arc::new(AtomicBool::new(false));
        let fired_clone = Arc::clone(&fired);

        engine.set_fire_callback(Box::new(move |_wk, _tid| {
            fired_clone.store(true, Ordering::Relaxed);
        }));

        engine.schedule(1, Duration::from_millis(50));
        let _handle = engine.start();

        thread::sleep(Duration::from_millis(200));
        assert!(fired.load(Ordering::Relaxed));
        engine.shutdown();
    }
}
