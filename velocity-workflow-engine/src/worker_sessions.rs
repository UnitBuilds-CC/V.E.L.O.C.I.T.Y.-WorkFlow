//! Worker Sessions — session support for sticky workflow execution.
//!
//! Sessions allow workflows to be pinned to specific workers for performance,
//! similar to Temporal's session-based sticky execution.

use std::collections::HashMap;
use std::sync::Mutex;

/// Status of a worker session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionStatus {
    Open,
    Closed,
    Failed,
    TimedOut,
}

/// Configuration for session management.
#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub heartbeat_timeout_ms: u64,
    pub max_executions_per_session: u64,
    pub session_idle_timeout_ms: u64,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            heartbeat_timeout_ms: 30_000,
            max_executions_per_session: 1000,
            session_idle_timeout_ms: 60_000,
        }
    }
}

/// A worker session.
#[derive(Debug, Clone)]
pub struct WorkerSession {
    pub session_id: String,
    pub worker_id: String,
    pub task_queue: String,
    pub started_at: u64,
    pub last_heartbeat: u64,
    pub status: SessionStatus,
    pub metadata: HashMap<String, String>,
    pub execution_count: u64,
}

/// Manages worker sessions.
pub struct SessionManager {
    sessions: Mutex<HashMap<String, WorkerSession>>,
    config: SessionConfig,
    next_id: Mutex<u64>,
}

impl SessionManager {
    pub fn new(config: SessionConfig) -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            config,
            next_id: Mutex::new(1),
        }
    }

    /// Create a new session.
    pub fn create_session(&self, worker_id: &str, task_queue: &str) -> String {
        let mut id_gen = self.next_id.lock().unwrap();
        let session_id = format!("session-{}-{}", worker_id, *id_gen);
        *id_gen += 1;
        drop(id_gen);

        let session = WorkerSession {
            session_id: session_id.clone(),
            worker_id: worker_id.to_string(),
            task_queue: task_queue.to_string(),
            started_at: 0,
            last_heartbeat: 0,
            status: SessionStatus::Open,
            metadata: HashMap::new(),
            execution_count: 0,
        };

        self.sessions.lock().unwrap().insert(session_id.clone(), session);
        session_id
    }

    /// Create a session with a specific start time.
    pub fn create_session_at(&self, worker_id: &str, task_queue: &str, timestamp: u64) -> String {
        let session_id = self.create_session(worker_id, task_queue);
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(s) = sessions.get_mut(&session_id) {
            s.started_at = timestamp;
            s.last_heartbeat = timestamp;
        }
        session_id
    }

    /// Close a session.
    pub fn close_session(&self, session_id: &str) -> Result<(), String> {
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.get_mut(session_id).ok_or("Session not found")?;
        if session.status != SessionStatus::Open {
            return Err(format!("Session is {:?}, not Open", session.status));
        }
        session.status = SessionStatus::Closed;
        Ok(())
    }

    /// Record a heartbeat for a session.
    pub fn heartbeat(&self, session_id: &str, timestamp: u64) -> Result<(), String> {
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.get_mut(session_id).ok_or("Session not found")?;
        if session.status != SessionStatus::Open {
            return Err(format!("Session is {:?}, not Open", session.status));
        }
        session.last_heartbeat = timestamp;
        Ok(())
    }

    /// Record an execution in a session.
    pub fn record_execution(&self, session_id: &str) -> Result<(), String> {
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.get_mut(session_id).ok_or("Session not found")?;
        if session.status != SessionStatus::Open {
            return Err(format!("Session is {:?}, not Open", session.status));
        }
        session.execution_count += 1;
        if session.execution_count >= self.config.max_executions_per_session {
            session.status = SessionStatus::Closed;
        }
        Ok(())
    }

    /// Get a session by ID.
    pub fn get_session(&self, session_id: &str) -> Option<WorkerSession> {
        self.sessions.lock().unwrap().get(session_id).cloned()
    }

    /// List sessions, optionally filtered by task queue.
    pub fn list_sessions(&self, task_queue: Option<&str>) -> Vec<WorkerSession> {
        let sessions = self.sessions.lock().unwrap();
        match task_queue {
            Some(tq) => sessions.values().filter(|s| s.task_queue == tq).cloned().collect(),
            None => sessions.values().cloned().collect(),
        }
    }

    /// List sessions by worker ID.
    pub fn list_sessions_by_worker(&self, worker_id: &str) -> Vec<WorkerSession> {
        self.sessions.lock().unwrap()
            .values()
            .filter(|s| s.worker_id == worker_id)
            .cloned()
            .collect()
    }

    /// Cleanup stale sessions (no heartbeat within timeout).
    pub fn cleanup_stale_sessions(&self, current_time: u64) -> usize {
        let mut sessions = self.sessions.lock().unwrap();
        let timeout = self.config.heartbeat_timeout_ms;
        let stale_ids: Vec<String> = sessions.values()
            .filter(|s| s.status == SessionStatus::Open && s.last_heartbeat > 0 && current_time.saturating_sub(s.last_heartbeat) > timeout)
            .map(|s| s.session_id.clone())
            .collect();

        for id in &stale_ids {
            if let Some(s) = sessions.get_mut(id) {
                s.status = SessionStatus::TimedOut;
            }
        }
        stale_ids.len()
    }

    /// Count total sessions.
    pub fn session_count(&self) -> usize {
        self.sessions.lock().unwrap().len()
    }

    /// Count active (Open) sessions.
    pub fn active_session_count(&self) -> usize {
        self.sessions.lock().unwrap().values().filter(|s| s.status == SessionStatus::Open).count()
    }

    /// Set metadata on a session.
    pub fn set_metadata(&self, session_id: &str, key: &str, value: &str) -> Result<(), String> {
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.get_mut(session_id).ok_or("Session not found")?;
        session.metadata.insert(key.to_string(), value.to_string());
        Ok(())
    }

    /// Fail a session.
    pub fn fail_session(&self, session_id: &str) -> Result<(), String> {
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.get_mut(session_id).ok_or("Session not found")?;
        session.status = SessionStatus::Failed;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> SessionConfig {
        SessionConfig {
            heartbeat_timeout_ms: 1000,
            max_executions_per_session: 5,
            session_idle_timeout_ms: 5000,
        }
    }

    #[test]
    fn test_create_session() {
        let mgr = SessionManager::new(test_config());
        let id = mgr.create_session("worker-1", "orders");
        assert!(id.starts_with("session-worker-1-"));
        assert_eq!(mgr.session_count(), 1);
    }

    #[test]
    fn test_close_session() {
        let mgr = SessionManager::new(test_config());
        let id = mgr.create_session("worker-1", "orders");
        mgr.close_session(&id).unwrap();
        let s = mgr.get_session(&id).unwrap();
        assert_eq!(s.status, SessionStatus::Closed);
    }

    #[test]
    fn test_heartbeat() {
        let mgr = SessionManager::new(test_config());
        let id = mgr.create_session_at("worker-1", "orders", 1000);
        mgr.heartbeat(&id, 2000).unwrap();
        let s = mgr.get_session(&id).unwrap();
        assert_eq!(s.last_heartbeat, 2000);
    }

    #[test]
    fn test_record_execution() {
        let mgr = SessionManager::new(test_config());
        let id = mgr.create_session("worker-1", "orders");
        for _ in 0..4 {
            mgr.record_execution(&id).unwrap();
        }
        let s = mgr.get_session(&id).unwrap();
        assert_eq!(s.execution_count, 4);
        assert_eq!(s.status, SessionStatus::Open);

        // 5th execution hits max, auto-closes
        mgr.record_execution(&id).unwrap();
        let s = mgr.get_session(&id).unwrap();
        assert_eq!(s.status, SessionStatus::Closed);
    }

    #[test]
    fn test_list_sessions_by_queue() {
        let mgr = SessionManager::new(test_config());
        mgr.create_session("w1", "orders");
        mgr.create_session("w2", "orders");
        mgr.create_session("w3", "payments");

        assert_eq!(mgr.list_sessions(Some("orders")).len(), 2);
        assert_eq!(mgr.list_sessions(Some("payments")).len(), 1);
        assert_eq!(mgr.list_sessions(None).len(), 3);
    }

    #[test]
    fn test_list_sessions_by_worker() {
        let mgr = SessionManager::new(test_config());
        mgr.create_session("w1", "orders");
        mgr.create_session("w1", "payments");
        mgr.create_session("w2", "orders");

        assert_eq!(mgr.list_sessions_by_worker("w1").len(), 2);
        assert_eq!(mgr.list_sessions_by_worker("w2").len(), 1);
    }

    #[test]
    fn test_cleanup_stale_sessions() {
        let mgr = SessionManager::new(test_config());
        mgr.create_session_at("w1", "orders", 1000);
        mgr.create_session_at("w2", "orders", 5000);

        // At time 3000, w1's session is stale (3000 - 1000 = 2000 > 1000 timeout)
        // w2's session is fresh (3000 - 5000 would be negative, so not stale)
        let cleaned = mgr.cleanup_stale_sessions(3000);
        assert_eq!(cleaned, 1);
        assert_eq!(mgr.active_session_count(), 1);
    }

    #[test]
    fn test_set_metadata() {
        let mgr = SessionManager::new(test_config());
        let id = mgr.create_session("w1", "orders");
        mgr.set_metadata(&id, "region", "us-east-1").unwrap();
        let s = mgr.get_session(&id).unwrap();
        assert_eq!(s.metadata.get("region").unwrap(), "us-east-1");
    }

    #[test]
    fn test_fail_session() {
        let mgr = SessionManager::new(test_config());
        let id = mgr.create_session("w1", "orders");
        mgr.fail_session(&id).unwrap();
        let s = mgr.get_session(&id).unwrap();
        assert_eq!(s.status, SessionStatus::Failed);
    }

    #[test]
    fn test_active_session_count() {
        let mgr = SessionManager::new(test_config());
        let id1 = mgr.create_session("w1", "q1");
        let id2 = mgr.create_session("w2", "q2");
        mgr.create_session("w3", "q3");

        assert_eq!(mgr.active_session_count(), 3);
        mgr.close_session(&id1).unwrap();
        assert_eq!(mgr.active_session_count(), 2);
        mgr.fail_session(&id2).unwrap();
        assert_eq!(mgr.active_session_count(), 1);
    }
}
