//! Event history storage with full payloads for workflow replay and audit.
//! Every state change is recorded as a history event with sequence number, timestamp, and data.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum HistoryEventType {
    WorkflowStarted = 1,
    WorkflowCompleted = 2,
    WorkflowFailed = 3,
    WorkflowCanceled = 4,
    WorkflowTerminated = 5,
    WorkflowTimedOut = 6,
    WorkflowContinuedAsNew = 7,
    StepCompleted = 10,
    ActivityScheduled = 20,
    ActivityStarted = 21,
    ActivityCompleted = 22,
    ActivityFailed = 23,
    ActivityTimedOut = 24,
    SignalReceived = 30,
    UpdateReceived = 31,
    QueryReceived = 32,
    TimerStarted = 40,
    TimerFired = 41,
    TimerCanceled = 42,
    ChildWorkflowStarted = 50,
    ChildWorkflowCompleted = 51,
    ChildWorkflowFailed = 52,
    MarkerRecorded = 60,
    WorkflowReset = 70,
}

#[derive(Debug, Clone)]
pub struct HistoryEvent {
    pub event_id: u64,
    pub event_type: HistoryEventType,
    pub workflow_key: u64,
    pub timestamp_ms: u64,
    pub payload: Vec<u8>,
    pub attributes: HashMap<String, String>,
}

pub struct WorkflowHistory {
    events: Vec<HistoryEvent>,
    next_event_id: AtomicU64,
}

impl WorkflowHistory {
    pub fn new() -> Self {
        Self {
            events: Vec::new(),
            next_event_id: AtomicU64::new(1),
        }
    }

    pub fn append(
        &mut self,
        event_type: HistoryEventType,
        workflow_key: u64,
        payload: Vec<u8>,
    ) -> u64 {
        let event_id = self.next_event_id.fetch_add(1, Ordering::Relaxed);
        self.events.push(HistoryEvent {
            event_id,
            event_type,
            workflow_key,
            timestamp_ms: 0,
            payload,
            attributes: HashMap::new(),
        });
        event_id
    }

    pub fn append_with_attrs(
        &mut self,
        event_type: HistoryEventType,
        workflow_key: u64,
        payload: Vec<u8>,
        attrs: HashMap<String, String>,
    ) -> u64 {
        let event_id = self.next_event_id.fetch_add(1, Ordering::Relaxed);
        self.events.push(HistoryEvent {
            event_id,
            event_type,
            workflow_key,
            timestamp_ms: 0,
            payload,
            attributes: attrs,
        });
        event_id
    }

    pub fn get_events(&self) -> &[HistoryEvent] {
        &self.events
    }

    pub fn get_event(&self, event_id: u64) -> Option<&HistoryEvent> {
        self.events.iter().find(|e| e.event_id == event_id)
    }

    pub fn get_events_by_type(&self, event_type: HistoryEventType) -> Vec<&HistoryEvent> {
        self.events
            .iter()
            .filter(|e| e.event_type == event_type)
            .collect()
    }

    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    pub fn last_event_id(&self) -> u64 {
        self.events.last().map_or(0, |e| e.event_id)
    }

    pub fn get_events_page(&self, start_event_id: u64, max_count: usize) -> Vec<&HistoryEvent> {
        self.events
            .iter()
            .filter(|e| e.event_id >= start_event_id)
            .take(max_count)
            .collect()
    }
}

impl Default for WorkflowHistory {
    fn default() -> Self {
        Self::new()
    }
}

/// Global history store managing histories for all workflows.
pub struct HistoryStore {
    histories: Mutex<HashMap<u64, WorkflowHistory>>,
}

impl HistoryStore {
    pub fn new() -> Self {
        Self {
            histories: Mutex::new(HashMap::new()),
        }
    }

    pub fn record_event(
        &self,
        workflow_key: u64,
        event_type: HistoryEventType,
        payload: Vec<u8>,
    ) -> u64 {
        let mut histories = self.histories.lock().unwrap();
        let history = histories.entry(workflow_key).or_default();
        history.append(event_type, workflow_key, payload)
    }

    pub fn record_event_with_attrs(
        &self,
        workflow_key: u64,
        event_type: HistoryEventType,
        payload: Vec<u8>,
        attrs: HashMap<String, String>,
    ) -> u64 {
        let mut histories = self.histories.lock().unwrap();
        let history = histories.entry(workflow_key).or_default();
        history.append_with_attrs(event_type, workflow_key, payload, attrs)
    }

    pub fn get_history(&self, workflow_key: u64) -> Option<Vec<HistoryEvent>> {
        self.histories
            .lock()
            .unwrap()
            .get(&workflow_key)
            .map(|h| h.events.clone())
    }

    pub fn get_history_page(
        &self,
        workflow_key: u64,
        start_event_id: u64,
        max_count: usize,
    ) -> Vec<HistoryEvent> {
        self.histories
            .lock()
            .unwrap()
            .get(&workflow_key)
            .map(|h| {
                h.get_events_page(start_event_id, max_count)
                    .into_iter()
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn event_count(&self, workflow_key: u64) -> usize {
        self.histories
            .lock()
            .unwrap()
            .get(&workflow_key)
            .map_or(0, |h| h.event_count())
    }

    pub fn workflow_count(&self) -> usize {
        self.histories.lock().unwrap().len()
    }

    pub fn remove_history(&self, workflow_key: u64) -> bool {
        self.histories
            .lock()
            .unwrap()
            .remove(&workflow_key)
            .is_some()
    }
}

impl Default for HistoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_history_append_and_retrieve() {
        let store = HistoryStore::new();
        let key = 42u64;

        store.record_event(key, HistoryEventType::WorkflowStarted, vec![1, 2, 3]);
        store.record_event(key, HistoryEventType::StepCompleted, vec![4, 5, 6]);
        store.record_event(key, HistoryEventType::WorkflowCompleted, vec![]);

        assert_eq!(store.event_count(key), 3);
        let history = store.get_history(key).unwrap();
        assert_eq!(history[0].event_type, HistoryEventType::WorkflowStarted);
        assert_eq!(history[1].event_type, HistoryEventType::StepCompleted);
        assert_eq!(history[2].event_type, HistoryEventType::WorkflowCompleted);
    }

    #[test]
    fn test_history_pagination() {
        let store = HistoryStore::new();
        let key = 100u64;
        for i in 0..10 {
            store.record_event(key, HistoryEventType::StepCompleted, vec![i as u8]);
        }
        let page = store.get_history_page(key, 3, 4);
        assert_eq!(page.len(), 4);
        assert_eq!(page[0].event_id, 3);
    }

    #[test]
    fn test_history_by_type() {
        let store = HistoryStore::new();
        let key = 200u64;
        store.record_event(key, HistoryEventType::WorkflowStarted, vec![]);
        store.record_event(key, HistoryEventType::SignalReceived, vec![1]);
        store.record_event(key, HistoryEventType::SignalReceived, vec![2]);
        store.record_event(key, HistoryEventType::WorkflowCompleted, vec![]);

        let history = store.get_history(key).unwrap();
        let signals: Vec<_> = history
            .iter()
            .filter(|e| e.event_type == HistoryEventType::SignalReceived)
            .collect();
        assert_eq!(signals.len(), 2);
    }
}
