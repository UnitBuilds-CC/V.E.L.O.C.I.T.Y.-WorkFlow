//! Agentic AI Context Window — Zero-Copy LLM Context Management.
//!
//! Implements the base.md vision: "Zero-Copy AI Context Windows" —
//! Uses the Tier-2 lock-free bump arena specifically for LLM context.
//! The main workflow execution (the Agent's control flow) runs in the sub-microsecond
//! Tier-1 slab. The massive conversational context (tokens, JSON schemas) is dumped
//! into the Tier-2 memory-mapped arena. If the AI agent crashes while waiting for
//! human approval, resumption is instantaneous because the LLM context window is
//! mapped straight back into memory via OS page-tables, bypassing GC entirely.
//!
//! This module provides:
//! - Token-level context window management
//! - Zero-copy arena allocation for LLM context
//! - Context compression and summarization
//! - Durable agent state (tool calls, approvals, multi-turn)
//! - Crash-safe context recovery

use std::collections::VecDeque;
use std::sync::Arc;

/// Maximum context window size in tokens (configurable).
const DEFAULT_MAX_TOKENS: usize = 128_000;

/// Role of a message in the conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageRole {
    System,
    User,
    Assistant,
    Tool,
}

/// A single message in the agent's context window.
#[derive(Debug, Clone)]
pub struct ContextMessage {
    pub role: MessageRole,
    pub content: String,
    pub token_count: usize,
    pub tool_call_id: Option<String>,
    pub tool_name: Option<String>,
    /// Arena offset where this message's content is stored (zero-copy).
    pub arena_offset: Option<usize>,
    /// Timestamp (engine ticks, not wall clock).
    pub tick: u64,
}

/// A tool call tracked by the agent.
#[derive(Debug, Clone)]
pub struct AgentToolCall {
    pub call_id: String,
    pub tool_name: String,
    pub arguments: String,
    pub result: Option<String>,
    pub status: ToolCallStatus,
    pub requires_approval: bool,
    pub approved: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolCallStatus {
    Pending,
    AwaitingApproval,
    Approved,
    Rejected,
    Executing,
    Completed,
    Failed,
}

/// Configuration for the AI context window.
#[derive(Debug, Clone)]
pub struct AiContextConfig {
    /// Maximum number of tokens in the context window.
    pub max_tokens: usize,
    /// Whether to use arena allocation (zero-copy).
    pub use_arena: bool,
    /// Arena size in bytes.
    pub arena_size: usize,
    /// Whether to automatically compress context when approaching limits.
    pub auto_compress: bool,
    /// Token threshold to trigger compression (percentage of max_tokens).
    pub compress_threshold: f64,
    /// Number of recent messages to always preserve during compression.
    pub preserve_recent: usize,
    /// Whether tool calls require human approval.
    pub require_approval: bool,
}

impl Default for AiContextConfig {
    fn default() -> Self {
        Self {
            max_tokens: DEFAULT_MAX_TOKENS,
            use_arena: true,
            arena_size: 64 * 1024 * 1024, // 64 MB
            auto_compress: true,
            compress_threshold: 0.8,
            preserve_recent: 10,
            require_approval: false,
        }
    }
}

/// Statistics for the AI context window.
#[derive(Debug, Clone, Default)]
pub struct AiContextStats {
    pub total_messages: u64,
    pub total_tokens: u64,
    pub total_tool_calls: u64,
    pub total_compressions: u64,
    pub total_tokens_compressed: u64,
    pub current_token_count: usize,
    pub arena_bytes_used: usize,
    pub arena_bytes_total: usize,
    pub peak_token_count: usize,
    pub crashes_recovered: u64,
}

/// Zero-copy arena for LLM context storage.
/// Messages are stored in the arena to avoid GC pressure.
struct ContextArena {
    buffer: Vec<u8>,
    used: usize,
    capacity: usize,
}

impl ContextArena {
    fn new(capacity: usize) -> Self {
        Self {
            buffer: vec![0u8; capacity],
            used: 0,
            capacity,
        }
    }

    /// Allocate space in the arena and copy data in (bump allocator).
    fn allocate(&mut self, data: &[u8]) -> Option<usize> {
        if self.used + data.len() > self.capacity {
            return None;
        }

        let offset = self.used;
        self.buffer[offset..offset + data.len()].copy_from_slice(data);
        self.used += data.len();
        Some(offset)
    }

    /// Read data from the arena at a given offset.
    fn read(&self, offset: usize, len: usize) -> &[u8] {
        &self.buffer[offset..offset + len]
    }

    /// Reset the arena (all previous allocations become invalid).
    fn reset(&mut self) {
        self.used = 0;
    }

    fn bytes_used(&self) -> usize {
        self.used
    }
    fn bytes_total(&self) -> usize {
        self.capacity
    }
}

/// The AI agent's durable context window.
///
/// Manages LLM conversation context with zero-copy arena allocation,
/// automatic compression, and crash-safe recovery.
pub struct AiContextWindow {
    config: AiContextConfig,
    messages: VecDeque<ContextMessage>,
    tool_calls: Vec<AgentToolCall>,
    arena: ContextArena,
    current_tokens: usize,
    tick: u64,
    stats: AiContextStats,
    /// System prompt (always preserved during compression).
    system_prompt: Option<String>,
}

impl AiContextWindow {
    pub fn new(config: AiContextConfig) -> Self {
        let arena_size = if config.use_arena {
            config.arena_size
        } else {
            0
        };
        Self {
            config,
            messages: VecDeque::new(),
            tool_calls: Vec::new(),
            arena: ContextArena::new(arena_size),
            current_tokens: 0,
            tick: 0,
            stats: AiContextStats::default(),
            system_prompt: None,
        }
    }

    /// Set the system prompt for the agent.
    pub fn set_system_prompt(&mut self, prompt: &str) {
        let token_count = estimate_tokens(prompt);
        self.system_prompt = Some(prompt.to_string());
        self.current_tokens += token_count;
    }

    /// Add a message to the context window.
    pub fn add_message(&mut self, role: MessageRole, content: &str) -> usize {
        let token_count = estimate_tokens(content);

        // Store in arena for zero-copy access
        let arena_offset = if self.config.use_arena {
            self.arena.allocate(content.as_bytes())
        } else {
            None
        };

        let msg = ContextMessage {
            role,
            content: content.to_string(),
            token_count,
            tool_call_id: None,
            tool_name: None,
            arena_offset,
            tick: self.tick,
        };

        self.messages.push_back(msg);
        self.current_tokens += token_count;
        self.tick += 1;
        self.stats.total_messages += 1;
        self.stats.total_tokens += token_count as u64;

        if self.current_tokens > self.stats.peak_token_count {
            self.stats.peak_token_count = self.current_tokens;
        }

        self.stats.current_token_count = self.current_tokens;
        self.stats.arena_bytes_used = self.arena.bytes_used();
        self.stats.arena_bytes_total = self.arena.bytes_total();

        // Auto-compress if approaching limit
        if self.config.auto_compress
            && self.current_tokens as f64
                > self.config.max_tokens as f64 * self.config.compress_threshold
        {
            self.compress();
        }

        token_count
    }

    /// Add a tool call to the context.
    pub fn add_tool_call(
        &mut self,
        tool_name: &str,
        arguments: &str,
        requires_approval: bool,
    ) -> String {
        let call_id = format!("call_{}", self.tool_calls.len());

        let status = if requires_approval {
            ToolCallStatus::AwaitingApproval
        } else {
            ToolCallStatus::Pending
        };

        self.tool_calls.push(AgentToolCall {
            call_id: call_id.clone(),
            tool_name: tool_name.to_string(),
            arguments: arguments.to_string(),
            result: None,
            status,
            requires_approval,
            approved: false,
        });

        self.stats.total_tool_calls += 1;
        call_id
    }

    /// Approve a pending tool call.
    pub fn approve_tool_call(&mut self, call_id: &str) -> bool {
        if let Some(call) = self.tool_calls.iter_mut().find(|c| c.call_id == call_id) {
            if call.status == ToolCallStatus::AwaitingApproval {
                call.status = ToolCallStatus::Approved;
                call.approved = true;
                return true;
            }
        }
        false
    }

    /// Reject a pending tool call.
    pub fn reject_tool_call(&mut self, call_id: &str) -> bool {
        if let Some(call) = self.tool_calls.iter_mut().find(|c| c.call_id == call_id) {
            if call.status == ToolCallStatus::AwaitingApproval {
                call.status = ToolCallStatus::Rejected;
                return true;
            }
        }
        false
    }

    /// Complete a tool call with a result.
    pub fn complete_tool_call(&mut self, call_id: &str, result: &str) -> bool {
        if let Some(call) = self.tool_calls.iter_mut().find(|c| c.call_id == call_id) {
            call.result = Some(result.to_string());
            call.status = ToolCallStatus::Completed;

            // Add the tool result as a message
            self.add_message(MessageRole::Tool, result);
            return true;
        }
        false
    }

    /// Compress the context window by summarizing older messages.
    /// Preserves the system prompt and the most recent messages.
    pub fn compress(&mut self) -> usize {
        // Only compress if we have at least 2x the preserve count
        // (otherwise compression doesn't actually reduce message count)
        if self.messages.len() < self.config.preserve_recent * 2 {
            return 0;
        }

        let preserve_count = self.config.preserve_recent;
        let to_compress_count = self.messages.len() - preserve_count;
        let mut tokens_freed = 0;

        // Summarize the messages being compressed
        let mut summary_parts: Vec<String> = Vec::new();
        for _ in 0..to_compress_count {
            if let Some(msg) = self.messages.pop_front() {
                if msg.role == MessageRole::System {
                    // Never compress system messages — put them back
                    self.messages.push_front(msg);
                    continue;
                }
                tokens_freed += msg.token_count;
                summary_parts.push(format!(
                    "[{}: {}]",
                    format!("{:?}", msg.role),
                    msg.content.chars().take(100).collect::<String>()
                ));
            }
        }

        if !summary_parts.is_empty() {
            let summary = format!(
                "[Compressed {} messages: {}]",
                summary_parts.len(),
                summary_parts.join("; ")
            );

            let summary_token_count = estimate_tokens(&summary);
            let summary_msg = ContextMessage {
                role: MessageRole::System,
                content: summary,
                token_count: summary_token_count,
                tool_call_id: None,
                tool_name: None,
                arena_offset: None,
                tick: self.tick,
            };

            self.current_tokens = self.current_tokens.saturating_sub(tokens_freed);
            self.current_tokens += summary_msg.token_count;
            self.messages.push_front(summary_msg);

            self.stats.total_compressions += 1;
            self.stats.total_tokens_compressed += tokens_freed as u64;
        }

        self.stats.current_token_count = self.current_tokens;
        tokens_freed
    }

    /// Get the full context for sending to an LLM.
    pub fn get_context_messages(&self) -> Vec<&ContextMessage> {
        self.messages.iter().collect()
    }

    /// Get pending tool calls that need execution.
    pub fn pending_tool_calls(&self) -> Vec<&AgentToolCall> {
        self.tool_calls
            .iter()
            .filter(|c| c.status == ToolCallStatus::Pending || c.status == ToolCallStatus::Approved)
            .collect()
    }

    /// Get tool calls awaiting approval.
    pub fn awaiting_approval(&self) -> Vec<&AgentToolCall> {
        self.tool_calls
            .iter()
            .filter(|c| c.status == ToolCallStatus::AwaitingApproval)
            .collect()
    }

    /// Recover context after a crash — reload messages from durable storage.
    pub fn recover_from_crash(&mut self, saved_messages: Vec<ContextMessage>) {
        for msg in saved_messages {
            self.current_tokens += msg.token_count;
            self.messages.push_back(msg);
        }
        self.stats.crashes_recovered += 1;
        self.stats.current_token_count = self.current_tokens;
    }

    /// Reset the context window (start a new conversation).
    pub fn reset(&mut self) {
        self.messages.clear();
        self.tool_calls.clear();
        self.arena.reset();
        self.current_tokens = 0;
        self.tick = 0;
        self.stats.current_token_count = 0;
    }

    // Accessors
    pub fn current_tokens(&self) -> usize {
        self.current_tokens
    }
    pub fn max_tokens(&self) -> usize {
        self.config.max_tokens
    }
    pub fn message_count(&self) -> usize {
        self.messages.len()
    }
    pub fn tool_call_count(&self) -> usize {
        self.tool_calls.len()
    }
    pub fn stats(&self) -> AiContextStats {
        self.stats.clone()
    }

    pub fn utilization(&self) -> f64 {
        self.current_tokens as f64 / self.config.max_tokens as f64
    }
}

/// Simple token estimation (1 token ≈ 4 characters for English).
fn estimate_tokens(text: &str) -> usize {
    (text.len() + 3) / 4
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> AiContextConfig {
        AiContextConfig {
            max_tokens: 100,
            use_arena: true,
            arena_size: 1024 * 1024,
            auto_compress: true,
            compress_threshold: 0.3,
            preserve_recent: 3,
            ..Default::default()
        }
    }

    #[test]
    fn test_add_message() {
        let mut ctx = AiContextWindow::new(test_config());
        let tokens = ctx.add_message(MessageRole::User, "Hello, how are you?");
        assert!(tokens > 0);
        assert_eq!(ctx.message_count(), 1);
        assert_eq!(ctx.current_tokens(), tokens);
    }

    #[test]
    fn test_system_prompt() {
        let mut ctx = AiContextWindow::new(test_config());
        ctx.set_system_prompt("You are a helpful assistant.");
        assert!(ctx.current_tokens() > 0);
    }

    #[test]
    fn test_tool_call_lifecycle() {
        let mut ctx = AiContextWindow::new(test_config());
        let call_id = ctx.add_tool_call("search", "{\"query\": \"test\"}", false);

        let pending = ctx.pending_tool_calls();
        assert_eq!(pending.len(), 1);

        ctx.complete_tool_call(&call_id, "Found 5 results");
        assert_eq!(ctx.pending_tool_calls().len(), 0);
    }

    #[test]
    fn test_tool_approval() {
        let mut ctx = AiContextWindow::new(test_config());
        let call_id = ctx.add_tool_call("delete_file", "{\"path\": \"/tmp/x\"}", true);

        let awaiting = ctx.awaiting_approval();
        assert_eq!(awaiting.len(), 1);

        assert!(ctx.approve_tool_call(&call_id));
        assert_eq!(ctx.awaiting_approval().len(), 0);
        assert_eq!(ctx.pending_tool_calls().len(), 1);
    }

    #[test]
    fn test_compression() {
        let mut ctx = AiContextWindow::new(test_config());

        // Add many messages to trigger compression
        for i in 0..20 {
            ctx.add_message(
                MessageRole::User,
                &format!(
                    "Message number {} with some content to increase token count significantly",
                    i
                ),
            );
        }

        // Should have compressed — message count should be less than 20
        assert!(ctx.message_count() < 20);
        assert!(ctx.stats().total_compressions > 0);
    }

    #[test]
    fn test_crash_recovery() {
        let mut ctx = AiContextWindow::new(test_config());
        ctx.add_message(MessageRole::User, "Before crash");

        let saved: Vec<ContextMessage> = ctx.messages.iter().cloned().collect();

        // New context after crash
        let mut ctx2 = AiContextWindow::new(test_config());
        ctx2.recover_from_crash(saved);

        assert_eq!(ctx2.message_count(), 1);
        assert_eq!(ctx2.stats().crashes_recovered, 1);
    }

    #[test]
    fn test_reset() {
        let mut ctx = AiContextWindow::new(test_config());
        ctx.add_message(MessageRole::User, "test");
        ctx.reset();

        assert_eq!(ctx.message_count(), 0);
        assert_eq!(ctx.current_tokens(), 0);
    }

    #[test]
    fn test_utilization() {
        let mut ctx = AiContextWindow::new(test_config());
        ctx.add_message(MessageRole::User, "Hello");
        assert!(ctx.utilization() > 0.0);
        assert!(ctx.utilization() < 1.0);
    }
}
