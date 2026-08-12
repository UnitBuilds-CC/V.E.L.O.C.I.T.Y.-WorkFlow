// Queue Processor tests — verify transfer, timer, and visibility queue processing
//
// Tests the core queue processors that drive workflow state transitions:
// 1. TransferQueueProcessor — activity dispatch, child workflow starts, etc.
// 2. TimerQueueProcessor — timer firing, workflow timeouts
// 3. VisibilityQueueProcessor — visibility record updates
// 4. ReplicationQueueProcessor — replication for multi-cluster

use std::collections::HashMap;
use std::sync::Arc;
use velocity_workflow_engine::queue_processing::{
    QueueProcessorConfig, QueueProcessorStats, QueueProcessorStatus, ReplicationQueueProcessor,
    ReplicationQueueTask, ReplicationQueueTaskType, TaskExecutionResult, TimerQueueProcessor,
    TimerQueueTask, TimerQueueTaskType, TransferQueueProcessor, TransferQueueTask,
    TransferQueueTaskType, VisibilityQueueProcessor, VisibilityQueueTask, VisibilityQueueTaskType,
};

fn default_queue_config() -> QueueProcessorConfig {
    QueueProcessorConfig::new("test")
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// ============================================================================
// Transfer Queue Processor Tests
// ============================================================================

#[test]
fn test_transfer_queue_creation() {
    let config = default_queue_config();
    let processor = TransferQueueProcessor::new(config);
    let stats = processor.stats();
    assert_eq!(stats.status, QueueProcessorStatus::Idle);
    assert_eq!(stats.total_tasks_submitted, 0);
    assert_eq!(stats.total_tasks_completed, 0);
}

#[test]
fn test_transfer_queue_submit_single_task() {
    let config = default_queue_config();
    let processor = TransferQueueProcessor::new(config);
    let task = TransferQueueTask {
        task_id: 1,
        workflow_key: 42,
        task_type: TransferQueueTaskType::ActivityTask,
        target_event_id: 10,
        target_namespace_id: 1,
        target_task_queue: "default-task-queue".to_string(),
        visibility_time_ms: 0,
        attempt: 0,
        created_at_ms: now_ms(),
    };
    processor.submit(task);
    let stats = processor.stats();
    assert_eq!(stats.total_tasks_submitted, 1);
}

#[test]
fn test_transfer_queue_submit_multiple_tasks() {
    let config = default_queue_config();
    let processor = TransferQueueProcessor::new(config);

    for i in 0..10 {
        processor.submit(TransferQueueTask {
            task_id: i,
            workflow_key: i * 10,
            task_type: TransferQueueTaskType::ActivityTask,
            target_event_id: i,
            target_namespace_id: 1,
            target_task_queue: "default-task-queue".to_string(),
            visibility_time_ms: 0,
            attempt: 0,
            created_at_ms: now_ms(),
        });
    }
    let stats = processor.stats();
    assert_eq!(stats.total_tasks_submitted, 10);
}

#[test]
fn test_transfer_queue_submit_batch() {
    let config = default_queue_config();
    let processor = TransferQueueProcessor::new(config);

    let tasks: Vec<TransferQueueTask> = (0..5)
        .map(|i| TransferQueueTask {
            task_id: i,
            workflow_key: i,
            task_type: TransferQueueTaskType::StartChildExecution,
            target_event_id: i,
            target_namespace_id: 1,
            target_task_queue: "".to_string(),
            visibility_time_ms: 0,
            attempt: 0,
            created_at_ms: now_ms(),
        })
        .collect();
    processor.submit_batch(tasks);
    let stats = processor.stats();
    assert_eq!(stats.total_tasks_submitted, 5);
}

#[test]
fn test_transfer_queue_different_task_types() {
    let config = default_queue_config();
    let processor = TransferQueueProcessor::new(config);

    let types = vec![
        TransferQueueTaskType::ActivityTask,
        TransferQueueTaskType::StartChildExecution,
        TransferQueueTaskType::SignalExternalWorkflow,
        TransferQueueTaskType::CancelExternalWorkflow,
        TransferQueueTaskType::CloseExecution,
        TransferQueueTaskType::ContinueAsNew,
        TransferQueueTaskType::RecordWorkflowStarted,
        TransferQueueTaskType::DeleteExecution,
        TransferQueueTaskType::UpsertSearchAttributes,
        TransferQueueTaskType::CancelCell,
    ];

    for (i, tt) in types.iter().enumerate() {
        let task = TransferQueueTask {
            task_id: i as u64,
            workflow_key: i as u64,
            task_type: *tt,
            target_event_id: 0,
            target_namespace_id: 1,
            target_task_queue: if *tt == TransferQueueTaskType::ActivityTask {
                "queue".to_string()
            } else {
                "".to_string()
            },
            visibility_time_ms: 0,
            attempt: 0,
            created_at_ms: now_ms(),
        };
        processor.submit(task);
    }
    assert_eq!(processor.stats().total_tasks_submitted, 10);
}

#[test]
fn test_transfer_queue_process_batch() {
    let config = default_queue_config();
    let processor = TransferQueueProcessor::new(config);

    processor.submit(TransferQueueTask {
        task_id: 1,
        workflow_key: 42,
        task_type: TransferQueueTaskType::ActivityTask,
        target_event_id: 10,
        target_namespace_id: 1,
        target_task_queue: "default-task-queue".to_string(),
        visibility_time_ms: 0,
        attempt: 0,
        created_at_ms: now_ms(),
    });

    let results = processor.process_batch();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, 1); // task_id
    assert!(matches!(results[0].1, TaskExecutionResult::Success));

    let stats = processor.stats();
    assert_eq!(stats.total_tasks_completed, 1);
}

#[test]
fn test_transfer_queue_empty_task_queue_fails() {
    let config = default_queue_config();
    let processor = TransferQueueProcessor::new(config);

    processor.submit(TransferQueueTask {
        task_id: 1,
        workflow_key: 42,
        task_type: TransferQueueTaskType::ActivityTask,
        target_event_id: 10,
        target_namespace_id: 1,
        target_task_queue: "".to_string(), // Empty task queue
        visibility_time_ms: 0,
        attempt: 0,
        created_at_ms: now_ms(),
    });

    let results = processor.process_batch();
    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0].1,
        TaskExecutionResult::NonRetryableError(_)
    ));
}

#[test]
fn test_transfer_queue_status_transitions() {
    let config = default_queue_config();
    let processor = TransferQueueProcessor::new(config);
    assert_eq!(processor.stats().status, QueueProcessorStatus::Idle);

    processor.start();
    assert_eq!(processor.stats().status, QueueProcessorStatus::Running);

    processor.pause();
    assert_eq!(processor.stats().status, QueueProcessorStatus::Paused);

    processor.start();
    assert_eq!(processor.stats().status, QueueProcessorStatus::Running);

    processor.stop();
    assert_eq!(processor.stats().status, QueueProcessorStatus::Stopped);
}

#[test]
fn test_transfer_queue_depth() {
    let config = default_queue_config();
    let processor = TransferQueueProcessor::new(config);
    assert_eq!(processor.depth(), 0);

    for i in 0..5 {
        processor.submit(TransferQueueTask {
            task_id: i,
            workflow_key: i,
            task_type: TransferQueueTaskType::CloseExecution,
            target_event_id: 0,
            target_namespace_id: 1,
            target_task_queue: "".to_string(),
            visibility_time_ms: 0,
            attempt: 0,
            created_at_ms: now_ms(),
        });
    }
    assert_eq!(processor.depth(), 5);

    processor.process_batch();
    assert_eq!(processor.depth(), 0);
}

// ============================================================================
// Timer Queue Processor Tests
// ============================================================================

#[test]
fn test_timer_queue_creation() {
    let config = default_queue_config();
    let processor = TimerQueueProcessor::new(config);
    let stats = processor.stats();
    assert_eq!(stats.status, QueueProcessorStatus::Idle);
    assert_eq!(stats.total_tasks_submitted, 0);
}

#[test]
fn test_timer_queue_schedule_timer() {
    let config = default_queue_config();
    let processor = TimerQueueProcessor::new(config);

    let task = TimerQueueTask {
        task_id: 1,
        workflow_key: 42,
        task_type: TimerQueueTaskType::UserTimer,
        timer_id: 100,
        expiry_time_ms: now_ms() + 5000,
        attempt: 0,
        created_at_ms: now_ms(),
    };
    processor.schedule(task);
    let stats = processor.stats();
    assert_eq!(stats.total_tasks_submitted, 1);
    assert_eq!(processor.depth(), 1);
}

#[test]
fn test_timer_queue_process_expired() {
    let config = default_queue_config();
    let processor = TimerQueueProcessor::new(config);

    // Submit an already-expired timer
    processor.schedule(TimerQueueTask {
        task_id: 1,
        workflow_key: 42,
        task_type: TimerQueueTaskType::UserTimer,
        timer_id: 100,
        expiry_time_ms: now_ms() - 1000, // Already expired
        attempt: 0,
        created_at_ms: now_ms(),
    });

    let results = processor.process_expired();
    assert_eq!(results.len(), 1);
    assert!(matches!(results[0].1, TaskExecutionResult::Success));
    let stats = processor.stats();
    assert_eq!(stats.total_tasks_completed, 1);
}

#[test]
fn test_timer_queue_future_timer_not_processed() {
    let config = default_queue_config();
    let processor = TimerQueueProcessor::new(config);

    // Submit a timer far in the future
    processor.schedule(TimerQueueTask {
        task_id: 1,
        workflow_key: 42,
        task_type: TimerQueueTaskType::UserTimer,
        timer_id: 100,
        expiry_time_ms: now_ms() + 60_000, // 60 seconds from now
        attempt: 0,
        created_at_ms: now_ms(),
    });

    let results = processor.process_expired();
    assert!(results.is_empty(), "Future timer should not be processed");
    assert_eq!(processor.depth(), 1);
}

#[test]
fn test_timer_queue_different_task_types() {
    let config = default_queue_config();
    let processor = TimerQueueProcessor::new(config);

    let types = vec![
        TimerQueueTaskType::UserTimer,
        TimerQueueTaskType::ActivityTimeout,
        TimerQueueTaskType::WorkflowRunTimeout,
        TimerQueueTaskType::WorkflowExecutionTimeout,
        TimerQueueTaskType::WorkflowTaskTimeout,
        TimerQueueTaskType::DeleteHistoryEvent,
        TimerQueueTaskType::ActivityRetryTimer,
    ];

    for (i, tt) in types.iter().enumerate() {
        processor.schedule(TimerQueueTask {
            task_id: i as u64,
            workflow_key: i as u64,
            task_type: *tt,
            timer_id: i as u64,
            expiry_time_ms: now_ms() - 1000, // All expired
            attempt: 0,
            created_at_ms: now_ms(),
        });
    }

    let results = processor.process_expired();
    assert_eq!(results.len(), 7);
}

#[test]
fn test_timer_queue_next_expiry() {
    let config = default_queue_config();
    let processor = TimerQueueProcessor::new(config);

    assert!(processor.next_expiry().is_none());

    let future_time = now_ms() + 10_000;
    processor.schedule(TimerQueueTask {
        task_id: 1,
        workflow_key: 1,
        task_type: TimerQueueTaskType::UserTimer,
        timer_id: 1,
        expiry_time_ms: future_time,
        attempt: 0,
        created_at_ms: now_ms(),
    });

    assert!(processor.next_expiry().is_some());
}

#[test]
fn test_timer_queue_stats() {
    let config = default_queue_config();
    let processor = TimerQueueProcessor::new(config);

    for i in 0..3 {
        processor.schedule(TimerQueueTask {
            task_id: i,
            workflow_key: i,
            task_type: TimerQueueTaskType::UserTimer,
            timer_id: i,
            expiry_time_ms: now_ms() - 1000,
            attempt: 0,
            created_at_ms: now_ms(),
        });
    }

    let stats = processor.stats();
    assert_eq!(stats.total_tasks_submitted, 3);
    assert_eq!(stats.queue_depth, 3);
}

// ============================================================================
// Visibility Queue Processor Tests
// ============================================================================

#[test]
fn test_visibility_queue_creation() {
    let config = default_queue_config();
    let processor = VisibilityQueueProcessor::new(config);
    let (submitted, completed, failed) = processor.stats();
    assert_eq!(submitted, 0);
    assert_eq!(completed, 0);
    assert_eq!(failed, 0);
}

#[test]
fn test_visibility_queue_submit_task() {
    let config = default_queue_config();
    let processor = VisibilityQueueProcessor::new(config);

    let task = VisibilityQueueTask {
        task_id: 1,
        workflow_key: 42,
        task_type: VisibilityQueueTaskType::RecordStart,
        namespace_id: 1,
        workflow_type: "TestWorkflow".to_string(),
        status: 1,
        start_time_ms: now_ms(),
        close_time_ms: None,
        search_attributes: HashMap::new(),
        created_at_ms: now_ms(),
    };
    processor.submit(task);
    let (submitted, _, _) = processor.stats();
    assert_eq!(submitted, 1);
}

#[test]
fn test_visibility_queue_process_batch() {
    let config = default_queue_config();
    let processor = VisibilityQueueProcessor::new(config);

    for i in 0..5 {
        processor.submit(VisibilityQueueTask {
            task_id: i,
            workflow_key: i,
            task_type: VisibilityQueueTaskType::RecordStart,
            namespace_id: 1,
            workflow_type: "TestWorkflow".to_string(),
            status: 1,
            start_time_ms: now_ms(),
            close_time_ms: None,
            search_attributes: HashMap::new(),
            created_at_ms: now_ms(),
        });
    }

    let processed = processor.process_batch();
    assert_eq!(processed, 5);
    let (submitted, completed, _) = processor.stats();
    assert_eq!(submitted, 5);
    assert_eq!(completed, 5);
}

#[test]
fn test_visibility_queue_depth() {
    let config = default_queue_config();
    let processor = VisibilityQueueProcessor::new(config);
    assert_eq!(processor.depth(), 0);

    processor.submit(VisibilityQueueTask {
        task_id: 1,
        workflow_key: 1,
        task_type: VisibilityQueueTaskType::UpsertSearchAttributes,
        namespace_id: 1,
        workflow_type: "TestWorkflow".to_string(),
        status: 1,
        start_time_ms: now_ms(),
        close_time_ms: None,
        search_attributes: HashMap::from([("key".to_string(), b"value".to_vec())]),
        created_at_ms: now_ms(),
    });
    assert_eq!(processor.depth(), 1);
}

#[test]
fn test_visibility_queue_task_types() {
    let config = default_queue_config();
    let processor = VisibilityQueueProcessor::new(config);

    let types = vec![
        VisibilityQueueTaskType::RecordStart,
        VisibilityQueueTaskType::RecordClose,
        VisibilityQueueTaskType::UpsertSearchAttributes,
        VisibilityQueueTaskType::DeleteExecution,
    ];

    for (i, tt) in types.iter().enumerate() {
        processor.submit(VisibilityQueueTask {
            task_id: i as u64,
            workflow_key: i as u64,
            task_type: *tt,
            namespace_id: 1,
            workflow_type: "TestWorkflow".to_string(),
            status: 1,
            start_time_ms: now_ms(),
            close_time_ms: None,
            search_attributes: HashMap::new(),
            created_at_ms: now_ms(),
        });
    }
    assert_eq!(processor.depth(), 4);
}

// ============================================================================
// Replication Queue Processor Tests
// ============================================================================

#[test]
fn test_replication_queue_creation() {
    let config = default_queue_config();
    let processor = ReplicationQueueProcessor::new(config);
    let (submitted, completed, failed, _repl_bytes) = processor.stats();
    assert_eq!(submitted, 0);
    assert_eq!(completed, 0);
    assert_eq!(failed, 0);
}

#[test]
fn test_replication_queue_submit() {
    let config = default_queue_config();
    let processor = ReplicationQueueProcessor::new(config);

    processor.submit(ReplicationQueueTask {
        task_id: 1,
        workflow_key: 42,
        task_type: ReplicationQueueTaskType::HistoryReplication,
        source_cluster: "cluster-a".to_string(),
        target_clusters: vec!["cluster-b".to_string()],
        first_event_id: 1,
        next_event_id: 10,
        branch_token: vec![1, 2, 3],
        version: 1,
        created_at_ms: now_ms(),
    });

    let (submitted, _, _, _) = processor.stats();
    assert_eq!(submitted, 1);
}

#[test]
fn test_replication_queue_process_batch() {
    let config = default_queue_config();
    let processor = ReplicationQueueProcessor::new(config);

    for i in 0..3 {
        processor.submit(ReplicationQueueTask {
            task_id: i,
            workflow_key: i,
            task_type: ReplicationQueueTaskType::SyncActivity,
            source_cluster: "cluster-a".to_string(),
            target_clusters: vec!["cluster-b".to_string(), "cluster-c".to_string()],
            first_event_id: 1,
            next_event_id: 5,
            branch_token: vec![],
            version: 1,
            created_at_ms: now_ms(),
        });
    }

    let processed = processor.process_batch();
    assert_eq!(processed, 3);
    let (submitted, completed, _, _) = processor.stats();
    assert_eq!(submitted, 3);
    assert_eq!(completed, 3);
}

// ============================================================================
// Queue Processor Config Tests
// ============================================================================

#[test]
fn test_queue_config_defaults() {
    let config = QueueProcessorConfig::new("test-queue");
    assert_eq!(config.name, "test-queue");
    assert_eq!(config.max_batch_size, 100);
    assert_eq!(config.poll_interval_ms, 100);
    assert_eq!(config.retry_max_attempts, 3);
    assert!(!config.enable_rate_limiting);
}

#[test]
fn test_queue_config_custom() {
    let mut config = QueueProcessorConfig::new("custom");
    config.max_batch_size = 500;
    config.poll_interval_ms = 5;
    config.retry_max_attempts = 5;
    config.enable_rate_limiting = true;
    config.rate_limit_per_second = 500;
    assert_eq!(config.max_batch_size, 500);
    assert!(config.enable_rate_limiting);
}

// ============================================================================
// Queue Processor Status Tests
// ============================================================================

#[test]
fn test_queue_processor_status_enum() {
    assert_ne!(QueueProcessorStatus::Idle, QueueProcessorStatus::Running);
    assert_ne!(QueueProcessorStatus::Running, QueueProcessorStatus::Stopped);
    assert_ne!(
        QueueProcessorStatus::Paused,
        QueueProcessorStatus::ShuttingDown
    );
    assert_eq!(QueueProcessorStatus::Idle, QueueProcessorStatus::Idle);
}

// ============================================================================
// Concurrent Queue Tests
// ============================================================================

#[test]
fn test_transfer_queue_concurrent_submit() {
    let config = default_queue_config();
    let processor = Arc::new(TransferQueueProcessor::new(config));
    let mut handles = vec![];

    for t in 0..4 {
        let proc = Arc::clone(&processor);
        let handle = std::thread::spawn(move || {
            for i in 0..25 {
                proc.submit(TransferQueueTask {
                    task_id: (t * 25 + i) as u64,
                    workflow_key: (t * 25 + i) as u64,
                    task_type: TransferQueueTaskType::CloseExecution,
                    target_event_id: 0,
                    target_namespace_id: 1,
                    target_task_queue: "".to_string(),
                    visibility_time_ms: 0,
                    attempt: 0,
                    created_at_ms: now_ms(),
                });
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(processor.stats().total_tasks_submitted, 100);
}

#[test]
fn test_timer_queue_concurrent_submit() {
    let config = default_queue_config();
    let processor = Arc::new(TimerQueueProcessor::new(config));
    let mut handles = vec![];

    for t in 0..4 {
        let proc = Arc::clone(&processor);
        let handle = std::thread::spawn(move || {
            for i in 0..25 {
                proc.schedule(TimerQueueTask {
                    task_id: (t * 25 + i) as u64,
                    workflow_key: (t * 25 + i) as u64,
                    task_type: TimerQueueTaskType::UserTimer,
                    timer_id: (t * 25 + i) as u64,
                    expiry_time_ms: now_ms() + 60_000,
                    attempt: 0,
                    created_at_ms: now_ms(),
                });
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(processor.stats().total_tasks_submitted, 100);
}

// ============================================================================
// Queue Processor Error Handling Tests
// ============================================================================

#[test]
fn test_transfer_queue_empty_process() {
    let config = default_queue_config();
    let processor = TransferQueueProcessor::new(config);
    let results = processor.process_batch();
    assert!(results.is_empty());
}

#[test]
fn test_timer_queue_empty_process() {
    let config = default_queue_config();
    let processor = TimerQueueProcessor::new(config);
    let results = processor.process_expired();
    assert!(results.is_empty());
}

#[test]
fn test_visibility_queue_empty_process() {
    let config = default_queue_config();
    let processor = VisibilityQueueProcessor::new(config);
    let processed = processor.process_batch();
    assert_eq!(processed, 0);
}
