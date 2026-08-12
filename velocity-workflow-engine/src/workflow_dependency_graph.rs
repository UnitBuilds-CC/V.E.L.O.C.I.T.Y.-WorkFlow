//! Workflow Dependency Graph — tracks relationships between workflow executions.
//!
//! Provides cross-workflow observability that Temporal lacks natively:
//! - Parent-child workflow relationships
//! - Signal sender/receiver tracking
//! - Update dependency tracking
//! - External workflow references
//! - Dependency visualization (topological sort, cycle detection)
//! - Impact analysis (what breaks if workflow X fails?)
//!
//! This enables:
//! - Root cause analysis for workflow failures
//! - Impact assessment before terminating workflows
//! - Visual dependency graphs for debugging
//! - Cascade failure detection

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    RwLock,
};

// ─── Dependency Types ────────────────────────────────────────────────────────

/// The type of relationship between two workflows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DependencyType {
    /// Parent spawned a child workflow.
    ParentChild,
    /// Workflow A sent a signal to workflow B.
    SignalSender,
    /// Workflow A sent an update to workflow B.
    UpdateTarget,
    /// Workflow A requested cancellation of workflow B.
    CancelRequest,
    /// Workflow A is waiting for workflow B to complete.
    AwaitingCompletion,
    /// Workflow A continues-as-new into workflow B.
    ContinuedAsNew,
    /// Saga step workflow — step in a saga orchestration.
    SagaStep,
    /// Saga compensation workflow — compensation for a failed step.
    SagaCompensation,
}

impl DependencyType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ParentChild => "parent_child",
            Self::SignalSender => "signal",
            Self::UpdateTarget => "update",
            Self::CancelRequest => "cancel_request",
            Self::AwaitingCompletion => "awaiting",
            Self::ContinuedAsNew => "continued_as_new",
            Self::SagaStep => "saga_step",
            Self::SagaCompensation => "saga_compensation",
        }
    }
}

/// A directed edge in the dependency graph.
#[derive(Debug, Clone)]
pub struct DependencyEdge {
    pub from_workflow_key: u64,
    pub to_workflow_key: u64,
    pub dep_type: DependencyType,
    pub created_at_ms: u64,
    pub metadata: Option<String>,
}

/// Direction of traversal for dependency queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DependencyDirection {
    /// Follow edges from source to target (downstream dependencies).
    Downstream,
    /// Follow edges from target to source (upstream dependencies).
    Upstream,
    /// Follow edges in both directions.
    Both,
}

// ─── Dependency Graph ────────────────────────────────────────────────────────

/// The workflow dependency graph.
pub struct WorkflowDependencyGraph {
    /// Adjacency list: workflow_key → outgoing edges.
    outgoing: RwLock<HashMap<u64, Vec<DependencyEdge>>>,
    /// Reverse adjacency list: workflow_key → incoming edges.
    incoming: RwLock<HashMap<u64, Vec<DependencyEdge>>>,
    /// Stats.
    stats: DependencyGraphStats,
}

#[derive(Debug, Default)]
pub struct DependencyGraphStats {
    pub edges_added: AtomicU64,
    pub edges_removed: AtomicU64,
    pub queries_executed: AtomicU64,
}

/// Result of a dependency traversal.
#[derive(Debug, Clone)]
pub struct DependencyTraversalResult {
    pub visited: Vec<u64>,
    pub edges_traversed: Vec<DependencyEdge>,
    pub depth: usize,
    pub has_cycle: bool,
}

/// Impact analysis result — what would be affected by removing/failing a workflow.
#[derive(Debug, Clone)]
pub struct ImpactAnalysis {
    pub affected_workflows: Vec<u64>,
    pub by_type: HashMap<DependencyType, Vec<u64>>,
    pub max_cascade_depth: usize,
    pub has_cycle: bool,
}

impl WorkflowDependencyGraph {
    pub fn new() -> Self {
        Self {
            outgoing: RwLock::new(HashMap::new()),
            incoming: RwLock::new(HashMap::new()),
            stats: DependencyGraphStats::default(),
        }
    }

    /// Add a dependency edge between two workflows.
    pub fn add_dependency(
        &self,
        from_workflow_key: u64,
        to_workflow_key: u64,
        dep_type: DependencyType,
        metadata: Option<String>,
    ) {
        let edge = DependencyEdge {
            from_workflow_key,
            to_workflow_key,
            dep_type,
            created_at_ms: now_ms(),
            metadata,
        };

        self.outgoing
            .write()
            .unwrap()
            .entry(from_workflow_key)
            .or_default()
            .push(edge.clone());

        self.incoming
            .write()
            .unwrap()
            .entry(to_workflow_key)
            .or_default()
            .push(edge);

        self.stats.edges_added.fetch_add(1, Ordering::Relaxed);
    }

    /// Remove all edges involving a workflow (both incoming and outgoing).
    pub fn remove_workflow(&self, workflow_key: u64) -> usize {
        let removed_out = self
            .outgoing
            .write()
            .unwrap()
            .remove(&workflow_key)
            .map(|v| v.len())
            .unwrap_or(0);

        let removed_in = self
            .incoming
            .write()
            .unwrap()
            .remove(&workflow_key)
            .map(|v| v.len())
            .unwrap_or(0);

        // Also clean up references in other workflows' adjacency lists
        let mut outgoing = self.outgoing.write().unwrap();
        for edges in outgoing.values_mut() {
            edges.retain(|e| e.to_workflow_key != workflow_key);
        }

        let mut incoming = self.incoming.write().unwrap();
        for edges in incoming.values_mut() {
            edges.retain(|e| e.from_workflow_key != workflow_key);
        }

        let total = removed_out + removed_in;
        self.stats
            .edges_removed
            .fetch_add(total as u64, Ordering::Relaxed);
        total
    }

    /// Get all direct dependencies of a workflow (outgoing edges).
    pub fn get_dependencies(&self, workflow_key: u64) -> Vec<DependencyEdge> {
        self.stats.queries_executed.fetch_add(1, Ordering::Relaxed);
        self.outgoing
            .read()
            .unwrap()
            .get(&workflow_key)
            .cloned()
            .unwrap_or_default()
    }

    /// Get all workflows that depend on this workflow (incoming edges).
    pub fn get_dependents(&self, workflow_key: u64) -> Vec<DependencyEdge> {
        self.stats.queries_executed.fetch_add(1, Ordering::Relaxed);
        self.incoming
            .read()
            .unwrap()
            .get(&workflow_key)
            .cloned()
            .unwrap_or_default()
    }

    /// Get dependencies filtered by type.
    pub fn get_dependencies_by_type(
        &self,
        workflow_key: u64,
        dep_type: DependencyType,
    ) -> Vec<DependencyEdge> {
        self.get_dependencies(workflow_key)
            .into_iter()
            .filter(|e| e.dep_type == dep_type)
            .collect()
    }

    /// BFS traversal of the dependency graph.
    pub fn traverse(
        &self,
        start_workflow_key: u64,
        direction: DependencyDirection,
        max_depth: usize,
    ) -> DependencyTraversalResult {
        self.stats.queries_executed.fetch_add(1, Ordering::Relaxed);

        let mut visited = HashSet::new();
        let mut edges_traversed = Vec::new();
        let mut queue: VecDeque<(u64, usize)> = VecDeque::new();
        let mut has_cycle = false;

        queue.push_back((start_workflow_key, 0));
        visited.insert(start_workflow_key);

        while let Some((current, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }

            let neighbors = match direction {
                DependencyDirection::Downstream | DependencyDirection::Both => {
                    self.get_dependencies(current)
                }
                DependencyDirection::Upstream => self.get_dependents(current),
            };

            for edge in neighbors {
                let next = match direction {
                    DependencyDirection::Upstream => edge.from_workflow_key,
                    _ => edge.to_workflow_key,
                };
                edges_traversed.push(edge);

                if visited.contains(&next) {
                    has_cycle = true;
                    continue;
                }

                visited.insert(next);
                queue.push_back((next, depth + 1));
            }

            // For Both direction, also follow incoming edges
            if direction == DependencyDirection::Both {
                let incoming = self.get_dependents(current);
                for edge in incoming {
                    let next = edge.from_workflow_key;
                    edges_traversed.push(edge);

                    if visited.contains(&next) {
                        has_cycle = true;
                        continue;
                    }

                    visited.insert(next);
                    queue.push_back((next, depth + 1));
                }
            }
        }

        let max_depth_reached = if visited.is_empty() {
            0
        } else {
            edges_traversed
                .iter()
                .map(|_| 1)
                .sum::<usize>()
                .min(max_depth)
        };

        DependencyTraversalResult {
            visited: visited.into_iter().collect(),
            edges_traversed,
            depth: max_depth_reached,
            has_cycle,
        }
    }

    /// Impact analysis — what would be affected if this workflow fails.
    pub fn analyze_impact(&self, workflow_key: u64) -> ImpactAnalysis {
        let traversal = self.traverse(workflow_key, DependencyDirection::Downstream, 100);
        let mut by_type: HashMap<DependencyType, Vec<u64>> = HashMap::new();

        for edge in &traversal.edges_traversed {
            by_type
                .entry(edge.dep_type)
                .or_default()
                .push(edge.to_workflow_key);
        }

        // Deduplicate
        for targets in by_type.values_mut() {
            targets.sort();
            targets.dedup();
        }

        let affected: Vec<u64> = traversal
            .visited
            .iter()
            .filter(|&&k| k != workflow_key)
            .copied()
            .collect();

        ImpactAnalysis {
            affected_workflows: affected,
            by_type,
            max_cascade_depth: traversal.depth,
            has_cycle: traversal.has_cycle,
        }
    }

    /// Detect cycles in the dependency graph using DFS.
    pub fn detect_cycles(&self) -> Vec<Vec<u64>> {
        let outgoing = self.outgoing.read().unwrap();
        let mut cycles = Vec::new();
        let mut visited = HashSet::new();
        let mut in_stack = HashSet::new();
        let mut path = Vec::new();

        for &node in outgoing.keys() {
            if !visited.contains(&node) {
                self.dfs_cycle(node, &outgoing, &mut visited, &mut in_stack, &mut path, &mut cycles);
            }
        }

        cycles
    }

    fn dfs_cycle(
        &self,
        node: u64,
        outgoing: &HashMap<u64, Vec<DependencyEdge>>,
        visited: &mut HashSet<u64>,
        in_stack: &mut HashSet<u64>,
        path: &mut Vec<u64>,
        cycles: &mut Vec<Vec<u64>>,
    ) {
        visited.insert(node);
        in_stack.insert(node);
        path.push(node);

        if let Some(edges) = outgoing.get(&node) {
            for edge in edges {
                let next = edge.to_workflow_key;
                if !visited.contains(&next) {
                    self.dfs_cycle(next, outgoing, visited, in_stack, path, cycles);
                } else if in_stack.contains(&next) {
                    // Found a cycle — extract it
                    let cycle_start = path.iter().position(|&n| n == next).unwrap();
                    let cycle = path[cycle_start..].to_vec();
                    cycles.push(cycle);
                }
            }
        }

        path.pop();
        in_stack.remove(&node);
    }

    /// Topological sort of the dependency graph (returns None if cycles exist).
    pub fn topological_sort(&self) -> Option<Vec<u64>> {
        let outgoing = self.outgoing.read().unwrap();
        let _incoming = self.incoming.read().unwrap();

        // Compute in-degrees
        let mut in_degree: HashMap<u64, usize> = HashMap::new();
        for &node in outgoing.keys() {
            in_degree.entry(node).or_insert(0);
            if let Some(edges) = outgoing.get(&node) {
                for edge in edges {
                    *in_degree.entry(edge.to_workflow_key).or_insert(0) += 1;
                }
            }
        }

        // Start with nodes that have no incoming edges
        let mut queue: VecDeque<u64> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(&node, _)| node)
            .collect();

        let mut result = Vec::new();

        while let Some(node) = queue.pop_front() {
            result.push(node);
            if let Some(edges) = outgoing.get(&node) {
                for edge in edges {
                    if let Some(deg) = in_degree.get_mut(&edge.to_workflow_key) {
                        *deg = deg.saturating_sub(1);
                        if *deg == 0 {
                            queue.push_back(edge.to_workflow_key);
                        }
                    }
                }
            }
        }

        if result.len() == in_degree.len() {
            Some(result)
        } else {
            None // Cycle detected
        }
    }

    /// Get the total number of edges in the graph.
    pub fn edge_count(&self) -> usize {
        self.outgoing
            .read()
            .unwrap()
            .values()
            .map(|v| v.len())
            .sum()
    }

    /// Get the total number of nodes (workflows) in the graph.
    pub fn node_count(&self) -> usize {
        let out_keys: HashSet<u64> = self.outgoing.read().unwrap().keys().copied().collect();
        let in_keys: HashSet<u64> = self.incoming.read().unwrap().keys().copied().collect();
        out_keys.union(&in_keys).count()
    }

    /// Get graph stats.
    pub fn stats(&self) -> (u64, u64, u64) {
        (
            self.stats.edges_added.load(Ordering::Relaxed),
            self.stats.edges_removed.load(Ordering::Relaxed),
            self.stats.queries_executed.load(Ordering::Relaxed),
        )
    }
}

impl Default for WorkflowDependencyGraph {
    fn default() -> Self {
        Self::new()
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_dependency() {
        let graph = WorkflowDependencyGraph::new();
        graph.add_dependency(1, 2, DependencyType::ParentChild, None);
        assert_eq!(graph.edge_count(), 1);
        assert_eq!(graph.node_count(), 2);
    }

    #[test]
    fn test_get_dependencies() {
        let graph = WorkflowDependencyGraph::new();
        graph.add_dependency(1, 2, DependencyType::ParentChild, None);
        graph.add_dependency(1, 3, DependencyType::SignalSender, None);
        graph.add_dependency(1, 4, DependencyType::ParentChild, None);

        let deps = graph.get_dependencies(1);
        assert_eq!(deps.len(), 3);
    }

    #[test]
    fn test_get_dependents() {
        let graph = WorkflowDependencyGraph::new();
        graph.add_dependency(1, 3, DependencyType::ParentChild, None);
        graph.add_dependency(2, 3, DependencyType::SignalSender, None);

        let dependents = graph.get_dependents(3);
        assert_eq!(dependents.len(), 2);
    }

    #[test]
    fn test_get_dependencies_by_type() {
        let graph = WorkflowDependencyGraph::new();
        graph.add_dependency(1, 2, DependencyType::ParentChild, None);
        graph.add_dependency(1, 3, DependencyType::SignalSender, None);
        graph.add_dependency(1, 4, DependencyType::ParentChild, None);

        let parent_deps = graph.get_dependencies_by_type(1, DependencyType::ParentChild);
        assert_eq!(parent_deps.len(), 2);

        let signal_deps = graph.get_dependencies_by_type(1, DependencyType::SignalSender);
        assert_eq!(signal_deps.len(), 1);
    }

    #[test]
    fn test_remove_workflow() {
        let graph = WorkflowDependencyGraph::new();
        graph.add_dependency(1, 2, DependencyType::ParentChild, None);
        graph.add_dependency(1, 3, DependencyType::SignalSender, None);
        graph.add_dependency(2, 3, DependencyType::ParentChild, None);

        let removed = graph.remove_workflow(2);
        assert!(removed > 0);

        // Workflow 1 should now only have 1 dependency (to 3)
        let deps = graph.get_dependencies(1);
        assert_eq!(deps.len(), 1);
        assert_eq!(deps[0].to_workflow_key, 3);
    }

    #[test]
    fn test_traverse_downstream() {
        let graph = WorkflowDependencyGraph::new();
        // 1 → 2 → 4
        // 1 → 3
        graph.add_dependency(1, 2, DependencyType::ParentChild, None);
        graph.add_dependency(1, 3, DependencyType::ParentChild, None);
        graph.add_dependency(2, 4, DependencyType::ParentChild, None);

        let result = graph.traverse(1, DependencyDirection::Downstream, 10);
        assert!(result.visited.contains(&2));
        assert!(result.visited.contains(&3));
        assert!(result.visited.contains(&4));
        assert!(!result.has_cycle);
    }

    #[test]
    fn test_traverse_upstream() {
        let graph = WorkflowDependencyGraph::new();
        graph.add_dependency(1, 3, DependencyType::ParentChild, None);
        graph.add_dependency(2, 3, DependencyType::SignalSender, None);

        let result = graph.traverse(3, DependencyDirection::Upstream, 10);
        assert!(result.visited.contains(&1));
        assert!(result.visited.contains(&2));
    }

    #[test]
    fn test_traverse_max_depth() {
        let graph = WorkflowDependencyGraph::new();
        graph.add_dependency(1, 2, DependencyType::ParentChild, None);
        graph.add_dependency(2, 3, DependencyType::ParentChild, None);
        graph.add_dependency(3, 4, DependencyType::ParentChild, None);

        // Only traverse 1 level deep
        let result = graph.traverse(1, DependencyDirection::Downstream, 1);
        assert!(result.visited.contains(&2));
        // 3 and 4 should not be visited at depth 1
    }

    #[test]
    fn test_cycle_detection() {
        let graph = WorkflowDependencyGraph::new();
        graph.add_dependency(1, 2, DependencyType::ParentChild, None);
        graph.add_dependency(2, 3, DependencyType::ParentChild, None);
        graph.add_dependency(3, 1, DependencyType::SignalSender, None); // cycle!

        let cycles = graph.detect_cycles();
        assert!(!cycles.is_empty());
    }

    #[test]
    fn test_no_cycle() {
        let graph = WorkflowDependencyGraph::new();
        graph.add_dependency(1, 2, DependencyType::ParentChild, None);
        graph.add_dependency(2, 3, DependencyType::ParentChild, None);

        let cycles = graph.detect_cycles();
        assert!(cycles.is_empty());
    }

    #[test]
    fn test_topological_sort() {
        let graph = WorkflowDependencyGraph::new();
        graph.add_dependency(1, 2, DependencyType::ParentChild, None);
        graph.add_dependency(1, 3, DependencyType::ParentChild, None);
        graph.add_dependency(2, 4, DependencyType::ParentChild, None);
        graph.add_dependency(3, 4, DependencyType::ParentChild, None);

        let sorted = graph.topological_sort();
        assert!(sorted.is_some());
        let order = sorted.unwrap();
        // 1 should come before 2 and 3
        let pos1 = order.iter().position(|&n| n == 1).unwrap();
        let pos2 = order.iter().position(|&n| n == 2).unwrap();
        let pos3 = order.iter().position(|&n| n == 3).unwrap();
        let pos4 = order.iter().position(|&n| n == 4).unwrap();
        assert!(pos1 < pos2);
        assert!(pos1 < pos3);
        assert!(pos2 < pos4);
        assert!(pos3 < pos4);
    }

    #[test]
    fn test_topological_sort_with_cycle() {
        let graph = WorkflowDependencyGraph::new();
        graph.add_dependency(1, 2, DependencyType::ParentChild, None);
        graph.add_dependency(2, 1, DependencyType::ParentChild, None); // cycle

        let sorted = graph.topological_sort();
        assert!(sorted.is_none());
    }

    #[test]
    fn test_impact_analysis() {
        let graph = WorkflowDependencyGraph::new();
        graph.add_dependency(1, 2, DependencyType::ParentChild, None);
        graph.add_dependency(1, 3, DependencyType::SignalSender, None);
        graph.add_dependency(2, 4, DependencyType::ParentChild, None);

        let impact = graph.analyze_impact(1);
        assert!(impact.affected_workflows.contains(&2));
        assert!(impact.affected_workflows.contains(&3));
        assert!(impact.affected_workflows.contains(&4));
        assert!(impact.by_type.contains_key(&DependencyType::ParentChild));
        assert!(impact.by_type.contains_key(&DependencyType::SignalSender));
    }

    #[test]
    fn test_empty_graph() {
        let graph = WorkflowDependencyGraph::new();
        assert_eq!(graph.edge_count(), 0);
        assert_eq!(graph.node_count(), 0);

        let deps = graph.get_dependencies(999);
        assert!(deps.is_empty());

        // Traversing a non-existent node returns just the start node in visited
        let traversal = graph.traverse(999, DependencyDirection::Downstream, 10);
        assert_eq!(traversal.edges_traversed.len(), 0);
    }

    #[test]
    fn test_stats() {
        let graph = WorkflowDependencyGraph::new();
        graph.add_dependency(1, 2, DependencyType::ParentChild, None);
        graph.add_dependency(1, 3, DependencyType::SignalSender, None);
        graph.get_dependencies(1);

        let (added, removed, queries) = graph.stats();
        assert_eq!(added, 2);
        assert_eq!(removed, 0);
        assert!(queries >= 1);
    }

    #[test]
    fn test_metadata() {
        let graph = WorkflowDependencyGraph::new();
        graph.add_dependency(
            1,
            2,
            DependencyType::ParentChild,
            Some("child workflow: ProcessOrder".into()),
        );

        let deps = graph.get_dependencies(1);
        assert_eq!(deps[0].metadata.as_deref(), Some("child workflow: ProcessOrder"));
    }

    #[test]
    fn test_dependency_type_strings() {
        assert_eq!(DependencyType::ParentChild.as_str(), "parent_child");
        assert_eq!(DependencyType::SignalSender.as_str(), "signal");
        assert_eq!(DependencyType::UpdateTarget.as_str(), "update");
        assert_eq!(DependencyType::CancelRequest.as_str(), "cancel_request");
        assert_eq!(DependencyType::AwaitingCompletion.as_str(), "awaiting");
        assert_eq!(DependencyType::ContinuedAsNew.as_str(), "continued_as_new");
        assert_eq!(DependencyType::SagaStep.as_str(), "saga_step");
        assert_eq!(DependencyType::SagaCompensation.as_str(), "saga_compensation");
    }
}
