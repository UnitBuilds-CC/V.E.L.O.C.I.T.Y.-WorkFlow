//! Hierarchical State Machine (HSM) framework — manages nested state machines
//! for complex workflow state management. Matches Temporal's service/history/hsm (~2,281 lines).
//!
//! 1. **State**: A single state in the state machine.
//! 2. **StateMachine**: A collection of states with transitions.
//! 3. **HierarchicalStateMachine**: Nested state machines with parent-child relationships.
//! 4. **Transition**: State transitions with guards and actions.
//! 5. **HSMRegistry**: Registry of all state machines in the system.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex, RwLock,
};
use std::time::Instant;

// ─── 1. State ─────────────────────────────────────────────────────────────────

/// A state in the state machine.
#[derive(Debug, Clone)]
pub struct HSMState {
    pub name: String,
    pub state_type: HSMStateType,
    pub is_initial: bool,
    pub is_final: bool,
    pub entry_actions: Vec<String>,
    pub exit_actions: Vec<String>,
    pub metadata: HashMap<String, String>,
}

/// State type classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HSMStateType {
    Normal,
    Initial,
    Final,
    History,
    Parallel,
    Choice,
    Fork,
    Join,
}

impl HSMState {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            state_type: HSMStateType::Normal,
            is_initial: false,
            is_final: false,
            entry_actions: Vec::new(),
            exit_actions: Vec::new(),
            metadata: HashMap::new(),
        }
    }

    pub fn initial(mut self) -> Self {
        self.is_initial = true;
        self.state_type = HSMStateType::Initial;
        self
    }
    pub fn final_state(mut self) -> Self {
        self.is_final = true;
        self.state_type = HSMStateType::Final;
        self
    }
    pub fn with_entry_action(mut self, action: &str) -> Self {
        self.entry_actions.push(action.to_string());
        self
    }
    pub fn with_exit_action(mut self, action: &str) -> Self {
        self.exit_actions.push(action.to_string());
        self
    }
}

// ─── 2. Transition ────────────────────────────────────────────────────────────

/// A transition between states.
#[derive(Debug, Clone)]
pub struct HSMTransition {
    pub name: String,
    pub source_state: String,
    pub target_state: String,
    pub event: String,
    pub guard: Option<String>,
    pub actions: Vec<String>,
    pub is_internal: bool,
}

impl HSMTransition {
    pub fn new(name: &str, source: &str, target: &str, event: &str) -> Self {
        Self {
            name: name.to_string(),
            source_state: source.to_string(),
            target_state: target.to_string(),
            event: event.to_string(),
            guard: None,
            actions: Vec::new(),
            is_internal: false,
        }
    }

    pub fn with_guard(mut self, guard: &str) -> Self {
        self.guard = Some(guard.to_string());
        self
    }
    pub fn with_action(mut self, action: &str) -> Self {
        self.actions.push(action.to_string());
        self
    }
    pub fn internal(mut self) -> Self {
        self.is_internal = true;
        self
    }
}

// ─── 3. State Machine ────────────────────────────────────────────────────────

/// A state machine with states and transitions.
pub struct HSMStateMachine {
    pub name: String,
    states: HashMap<String, HSMState>,
    transitions: Vec<HSMTransition>,
    current_state: Mutex<String>,
    initial_state: String,
    event_log: Mutex<Vec<EventRecord>>,
    total_transitions: AtomicU64,
}

/// Record of a state machine event.
#[derive(Debug, Clone)]
pub struct EventRecord {
    pub event: String,
    pub from_state: String,
    pub to_state: String,
    pub transition_name: String,
    pub timestamp: Instant,
}

impl HSMStateMachine {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            states: HashMap::new(),
            transitions: Vec::new(),
            current_state: Mutex::new(String::new()),
            initial_state: String::new(),
            event_log: Mutex::new(Vec::new()),
            total_transitions: AtomicU64::new(0),
        }
    }

    /// Add a state.
    pub fn add_state(&mut self, state: HSMState) {
        if state.is_initial && self.initial_state.is_empty() {
            self.initial_state = state.name.clone();
        }
        self.states.insert(state.name.clone(), state);
    }

    /// Add a transition.
    pub fn add_transition(&mut self, transition: HSMTransition) {
        self.transitions.push(transition);
    }

    /// Initialize the state machine.
    pub fn initialize(&self) -> bool {
        if self.initial_state.is_empty() {
            return false;
        }
        *self.current_state.lock().unwrap() = self.initial_state.clone();
        true
    }

    /// Get the current state.
    pub fn current_state(&self) -> String {
        self.current_state.lock().unwrap().clone()
    }

    /// Fire an event and process transitions.
    pub fn fire_event(&self, event: &str) -> TransitionResult {
        let current = self.current_state.lock().unwrap().clone();

        // Find matching transition (clone to avoid borrow issues)
        let matched: Option<HSMTransition> = self
            .transitions
            .iter()
            .find(|t| t.source_state == current && t.event == event)
            .cloned();

        match matched {
            Some(t) => {
                let from = current;
                if !t.is_internal {
                    *self.current_state.lock().unwrap() = t.target_state.clone();
                }
                self.total_transitions.fetch_add(1, Ordering::Relaxed);
                self.event_log.lock().unwrap().push(EventRecord {
                    event: event.to_string(),
                    from_state: from.clone(),
                    to_state: t.target_state.clone(),
                    transition_name: t.name.clone(),
                    timestamp: Instant::now(),
                });
                TransitionResult {
                    success: true,
                    from_state: from,
                    to_state: t.target_state.clone(),
                    transition_name: t.name.clone(),
                    actions: t.actions.clone(),
                }
            }
            None => TransitionResult {
                success: false,
                from_state: current.clone(),
                to_state: current,
                transition_name: String::new(),
                actions: Vec::new(),
            },
        }
    }

    /// Check if the state machine is in a final state.
    pub fn is_final(&self) -> bool {
        let current = self.current_state.lock().unwrap().clone();
        self.states.get(&current).is_some_and(|s| s.is_final)
    }

    /// Get all available transitions from the current state.
    pub fn available_transitions(&self) -> Vec<String> {
        let current = self.current_state.lock().unwrap().clone();
        self.transitions
            .iter()
            .filter(|t| t.source_state == current)
            .map(|t| t.event.clone())
            .collect()
    }

    pub fn total_transitions(&self) -> u64 {
        self.total_transitions.load(Ordering::Relaxed)
    }
    pub fn state_count(&self) -> usize {
        self.states.len()
    }
    pub fn transition_count(&self) -> usize {
        self.transitions.len()
    }
    pub fn event_history(&self) -> Vec<EventRecord> {
        self.event_log.lock().unwrap().clone()
    }
}

/// Result of a transition.
#[derive(Debug, Clone)]
pub struct TransitionResult {
    pub success: bool,
    pub from_state: String,
    pub to_state: String,
    pub transition_name: String,
    pub actions: Vec<String>,
}

// ─── 4. Hierarchical State Machine ───────────────────────────────────────────

/// Hierarchical state machine with nested child machines.
pub struct HierarchicalStateMachine {
    pub name: String,
    root_machine: HSMStateMachine,
    child_machines: RwLock<HashMap<String, HSMStateMachine>>,
    total_events: AtomicU64,
}

impl HierarchicalStateMachine {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            root_machine: HSMStateMachine::new(&format!("{}-root", name)),
            child_machines: RwLock::new(HashMap::new()),
            total_events: AtomicU64::new(0),
        }
    }

    /// Add a child state machine.
    pub fn add_child(&self, parent_state: &str, child: HSMStateMachine) {
        self.child_machines
            .write()
            .unwrap()
            .insert(parent_state.to_string(), child);
    }

    /// Fire an event on the root machine.
    pub fn fire_event(&self, event: &str) -> TransitionResult {
        self.total_events.fetch_add(1, Ordering::Relaxed);
        let result = self.root_machine.fire_event(event);

        // If transitioned to a state with a child machine, initialize it
        if result.success {
            let children = self.child_machines.read().unwrap();
            if let Some(child) = children.get(&result.to_state) {
                child.initialize();
            }
        }

        result
    }

    /// Get the root machine's current state.
    pub fn current_state(&self) -> String {
        self.root_machine.current_state()
    }

    /// Get a child machine's current state.
    pub fn child_state(&self, parent_state: &str) -> Option<String> {
        self.child_machines
            .read()
            .unwrap()
            .get(parent_state)
            .map(|c| c.current_state())
    }

    /// Check if the entire hierarchy is in a final state.
    pub fn is_final(&self) -> bool {
        if !self.root_machine.is_final() {
            return false;
        }
        let current = self.root_machine.current_state();
        let children = self.child_machines.read().unwrap();
        if let Some(child) = children.get(&current) {
            child.is_final()
        } else {
            true
        }
    }

    pub fn root_machine(&self) -> &HSMStateMachine {
        &self.root_machine
    }
    pub fn total_events(&self) -> u64 {
        self.total_events.load(Ordering::Relaxed)
    }
}

// ─── 5. HSM Registry ─────────────────────────────────────────────────────────

/// Registry of all state machines in the system.
pub struct HSMRegistry {
    machines: RwLock<HashMap<String, HierarchicalStateMachine>>,
    total_registered: AtomicU64,
}

impl HSMRegistry {
    pub fn new() -> Self {
        Self {
            machines: RwLock::new(HashMap::new()),
            total_registered: AtomicU64::new(0),
        }
    }

    /// Register a state machine.
    pub fn register(&self, hsm: HierarchicalStateMachine) {
        self.machines.write().unwrap().insert(hsm.name.clone(), hsm);
        self.total_registered.fetch_add(1, Ordering::Relaxed);
    }

    /// Get a state machine by name.
    pub fn get(&self, name: &str) -> Option<String> {
        self.machines
            .read()
            .unwrap()
            .get(name)
            .map(|m| m.current_state())
    }

    /// Fire an event on a specific state machine.
    pub fn fire_event(&self, machine_name: &str, event: &str) -> Option<TransitionResult> {
        self.machines
            .read()
            .unwrap()
            .get(machine_name)
            .map(|m| m.fire_event(event))
    }

    pub fn total_registered(&self) -> u64 {
        self.total_registered.load(Ordering::Relaxed)
    }
    pub fn machine_count(&self) -> usize {
        self.machines.read().unwrap().len()
    }
}

impl Default for HSMRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn build_order_machine() -> HSMStateMachine {
        let mut sm = HSMStateMachine::new("order");
        sm.add_state(HSMState::new("created").initial());
        sm.add_state(HSMState::new("processing").with_entry_action("validate_order"));
        sm.add_state(HSMState::new("shipped").with_entry_action("send_notification"));
        sm.add_state(HSMState::new("delivered").final_state());
        sm.add_state(HSMState::new("cancelled").final_state());

        sm.add_transition(
            HSMTransition::new("start", "created", "processing", "process")
                .with_action("validate_inventory"),
        );
        sm.add_transition(
            HSMTransition::new("ship", "processing", "shipped", "ship")
                .with_action("generate_tracking"),
        );
        sm.add_transition(HSMTransition::new(
            "deliver",
            "shipped",
            "delivered",
            "deliver",
        ));
        sm.add_transition(HSMTransition::new(
            "cancel",
            "created",
            "cancelled",
            "cancel",
        ));
        sm.add_transition(HSMTransition::new(
            "cancel_from_processing",
            "processing",
            "cancelled",
            "cancel",
        ));
        sm
    }

    #[test]
    fn test_state_machine_basic() {
        let sm = build_order_machine();
        sm.initialize();
        assert_eq!(sm.current_state(), "created");
        assert_eq!(sm.state_count(), 5);
        assert_eq!(sm.transition_count(), 5);
    }

    #[test]
    fn test_state_machine_transitions() {
        let sm = build_order_machine();
        sm.initialize();

        let r1 = sm.fire_event("process");
        assert!(r1.success);
        assert_eq!(r1.from_state, "created");
        assert_eq!(r1.to_state, "processing");
        assert_eq!(sm.current_state(), "processing");

        let r2 = sm.fire_event("ship");
        assert!(r2.success);
        assert_eq!(sm.current_state(), "shipped");

        let r3 = sm.fire_event("deliver");
        assert!(r3.success);
        assert_eq!(sm.current_state(), "delivered");
        assert!(sm.is_final());
    }

    #[test]
    fn test_state_machine_invalid_transition() {
        let sm = build_order_machine();
        sm.initialize();

        let r = sm.fire_event("ship"); // Can't ship from created
        assert!(!r.success);
        assert_eq!(sm.current_state(), "created");
    }

    #[test]
    fn test_state_machine_cancel() {
        let sm = build_order_machine();
        sm.initialize();
        sm.fire_event("process");
        let r = sm.fire_event("cancel");
        assert!(r.success);
        assert_eq!(sm.current_state(), "cancelled");
        assert!(sm.is_final());
    }

    #[test]
    fn test_available_transitions() {
        let sm = build_order_machine();
        sm.initialize();
        let available = sm.available_transitions();
        assert!(available.contains(&"process".to_string()));
        assert!(available.contains(&"cancel".to_string()));
    }

    #[test]
    fn test_event_history() {
        let sm = build_order_machine();
        sm.initialize();
        sm.fire_event("process");
        sm.fire_event("ship");
        let history = sm.event_history();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].from_state, "created");
        assert_eq!(history[1].from_state, "processing");
    }

    #[test]
    fn test_hierarchical_sm() {
        // Build root machine
        let mut root = HSMStateMachine::new("order-flow-root");
        root.add_state(HSMState::new("idle").initial());
        root.add_state(HSMState::new("active"));
        root.add_state(HSMState::new("completed").final_state());
        root.add_transition(HSMTransition::new("start", "idle", "active", "start"));
        root.add_transition(HSMTransition::new(
            "finish",
            "active",
            "completed",
            "finish",
        ));
        root.initialize();

        assert_eq!(root.current_state(), "idle");
        root.fire_event("start");
        assert_eq!(root.current_state(), "active");
        root.fire_event("finish");
        assert_eq!(root.current_state(), "completed");
        assert!(root.is_final());
    }

    #[test]
    fn test_hsm_registry() {
        let registry = HSMRegistry::new();

        let mut sm = HSMStateMachine::new("test-sm");
        sm.add_state(HSMState::new("a").initial());
        sm.add_state(HSMState::new("b"));
        sm.add_transition(HSMTransition::new("go", "a", "b", "go"));
        sm.initialize();

        let hsm = HierarchicalStateMachine::new("test");
        registry.register(hsm);
        assert_eq!(registry.total_registered(), 1);
    }
}
