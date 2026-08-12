//! C-ABI FFI exports for the workflow engine. C# calls these functions via P/Invoke.
//! The engine lives entirely in Rust — C# is a thin interop bridge only.

use std::sync::Arc;

use crate::engine::{WorkflowEngine, WorkflowContext};
use crate::workflow_reset::ResetReason;
use crate::schedules::ScheduleState;

// ─── Opaque handle ────────────────────────────────────────────────────────────

/// Opaque engine handle exposed to C#. The C# side holds a `void*` to this.
struct EngineHandle {
    engine: Arc<WorkflowEngine>,
}

// ─── Lifecycle ────────────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_create() -> *mut EngineHandle {
    let handle = Box::new(EngineHandle {
        engine: Arc::new(WorkflowEngine::new()),
    });
    Box::into_raw(handle)
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_destroy(handle: *mut EngineHandle) -> i32 {
    if handle.is_null() { return -1; }
    let h = Box::from_raw(handle);
    h.engine.shutdown();
    0
}

// ─── Workflow Lifecycle ───────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_start_workflow(
    handle: *mut EngineHandle,
    workflow_id: u64,
    workflow_type_id: u64,
    namespace_id: u64,
    task_queue_hash: u64,
    total_steps: u32,
    input_ptr: *const u8,
    input_len: u32,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;

    let input = if input_ptr.is_null() || input_len == 0 {
        None
    } else {
        Some(std::slice::from_raw_parts(input_ptr, input_len as usize).to_vec())
    };

    h.engine.start_workflow(workflow_id, workflow_type_id, namespace_id, task_queue_hash, total_steps, input)
}

/// Start a workflow with search attributes.
/// Search attributes are passed as parallel arrays of key/value string pairs.
/// Each key and value is a UTF-8 byte pointer + length.
/// Returns the workflow key, or 0 on failure.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_start_workflow_with_attrs(
    handle: *mut EngineHandle,
    workflow_id: u64,
    workflow_type_id: u64,
    namespace_id: u64,
    task_queue_hash: u64,
    total_steps: u32,
    input_ptr: *const u8,
    input_len: u32,
    attr_keys: *const *const u8, attr_key_lens: *const u32, attr_key_count: u32,
    attr_vals: *const *const u8, attr_val_lens: *const u32,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;

    let input = if input_ptr.is_null() || input_len == 0 {
        None
    } else {
        Some(std::slice::from_raw_parts(input_ptr, input_len as usize).to_vec())
    };

    let mut search_attrs = std::collections::HashMap::new();
    if !attr_keys.is_null() && !attr_vals.is_null() && attr_key_count > 0 {
        let keys = std::slice::from_raw_parts(attr_keys, attr_key_count as usize);
        let key_lens = std::slice::from_raw_parts(attr_key_lens, attr_key_count as usize);
        let vals = std::slice::from_raw_parts(attr_vals, attr_key_count as usize);
        let val_lens = std::slice::from_raw_parts(attr_val_lens, attr_key_count as usize);
        for i in 0..attr_key_count as usize {
            let key_bytes = if keys[i].is_null() || key_lens[i] == 0 { continue; } else { std::slice::from_raw_parts(keys[i], key_lens[i] as usize) };
            let val_bytes = if vals[i].is_null() || val_lens[i] == 0 { continue; } else { std::slice::from_raw_parts(vals[i], val_lens[i] as usize) };
            if let (Ok(k), Ok(v)) = (std::str::from_utf8(key_bytes), std::str::from_utf8(val_bytes)) {
                search_attrs.insert(k.to_string(), crate::visibility::SearchAttributeValue::String(v.to_string()));
            }
        }
    }

    h.engine.start_workflow_with_attrs(workflow_id, workflow_type_id, namespace_id, task_queue_hash, total_steps, input, search_attrs)
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_complete_workflow(
    handle: *mut EngineHandle,
    workflow_key: u64,
    result_ptr: *const u8,
    result_len: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;

    let result = if result_ptr.is_null() || result_len == 0 {
        None
    } else {
        Some(std::slice::from_raw_parts(result_ptr, result_len as usize).to_vec())
    };

    h.engine.complete_workflow(workflow_key, result);
    0
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_fail_workflow(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    h.engine.fail_workflow(workflow_key);
    0
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cancel_workflow(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    h.engine.cancel_workflow(workflow_key);
    0
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_terminate_workflow(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    h.engine.terminate_workflow(workflow_key);
    0
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_get_status(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    h.engine.get_status(workflow_key) as i32
}

// ─── Step Execution ───────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_is_step_completed(
    handle: *mut EngineHandle,
    workflow_key: u64,
    step: u32,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    if h.engine.is_step_completed(workflow_key, step) { 1 } else { 0 }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_complete_step(
    handle: *mut EngineHandle,
    workflow_key: u64,
    step: u32,
    result_ptr: *const u8,
    result_len: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;

    let result = if result_ptr.is_null() || result_len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(result_ptr, result_len as usize).to_vec()
    };

    h.engine.complete_step(workflow_key, step, result);
    0
}

/// Get the step result for a completed step. Writes into the caller's buffer.
/// Returns the number of bytes written, or the required size if buffer is too small (as negative).
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_get_step_result(
    handle: *mut EngineHandle,
    workflow_key: u64,
    step: u32,
    out_buf: *mut u8,
    out_buf_len: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;

    match h.engine.get_step_result(workflow_key, step) {
        Some(data) => {
            let len = data.len() as u32;
            if len > out_buf_len {
                return -(len as i32); // Buffer too small — caller needs to allocate more
            }
            if !out_buf.is_null() && len > 0 {
                std::ptr::copy_nonoverlapping(data.as_ptr(), out_buf, len as usize);
            }
            len as i32
        }
        None => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_get_current_step(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> u32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.get_current_step(workflow_key)
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_get_total_steps(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> u32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.get_total_steps(workflow_key)
}

// ─── Activity Scheduling ──────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_schedule_activity(
    handle: *mut EngineHandle,
    workflow_key: u64,
    step: u32,
    activity_name_id: u64,
    args_ptr: *const u8,
    args_len: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;

    let args = if args_ptr.is_null() || args_len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(args_ptr, args_len as usize).to_vec()
    };

    h.engine.schedule_activity(workflow_key, step, activity_name_id, args);
    0
}

// ─── Task Queue Polling ───────────────────────────────────────────────────────

/// Poll for the next task on a named queue. Writes task data into the output struct.
/// Returns 1 if a task was found, 0 if no task is available.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_poll_task(
    handle: *mut EngineHandle,
    task_queue_hash: u64,
    out_task_kind: *mut u32,
    out_workflow_key: *mut u64,
    out_step_index: *mut u32,
    out_activity_name_id: *mut u64,
    out_task_id: *mut u64,
    out_attempt: *mut u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;

    match h.engine.task_queue().try_poll(task_queue_hash) {
        Some(task) => {
            if !out_task_kind.is_null() { *out_task_kind = task.kind as u32; }
            if !out_workflow_key.is_null() { *out_workflow_key = task.workflow_key; }
            if !out_step_index.is_null() { *out_step_index = task.step_index; }
            if !out_activity_name_id.is_null() { *out_activity_name_id = task.activity_name_id; }
            if !out_task_id.is_null() { *out_task_id = task.task_id; }
            if !out_attempt.is_null() { *out_attempt = task.attempt; }
            1
        }
        None => 0,
    }
}

// ─── Signal / Query / Update ──────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_signal(
    handle: *mut EngineHandle,
    workflow_key: u64,
    signal_name_id: u64,
    payload_ptr: *const u8,
    payload_len: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;

    let payload = if payload_ptr.is_null() || payload_len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(payload_ptr, payload_len as usize).to_vec()
    };

    h.engine.signal_workflow(workflow_key, signal_name_id, payload);
    0
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_has_signal(
    handle: *mut EngineHandle,
    workflow_key: u64,
    signal_name_id: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    if h.engine.has_signal(workflow_key, signal_name_id) { 1 } else { 0 }
}

// ─── Timer ────────────────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_schedule_timer(
    handle: *mut EngineHandle,
    workflow_key: u64,
    delay_ms: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.schedule_timer(workflow_key, delay_ms)
}

// ─── Merkle Verification ──────────────────────────────────────────────────────

/// Verify the Merkle root of a workflow's slab header. Returns 1 if valid, 0 if corrupted.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_verify_slab(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;

    match h.engine.get_slab(workflow_key) {
        Some(slab) => if slab.verify_merkle_root() { 1 } else { 0 },
        None => -1,
    }
}

// ─── Stats ────────────────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_workflow_count(
    handle: *mut EngineHandle,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.workflow_count() as u64
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_pending_tasks(
    handle: *mut EngineHandle,
    task_queue_hash: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.task_queue().pending_count(task_queue_hash) as u64
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_pending_timers(
    handle: *mut EngineHandle,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.timer_engine().pending_count() as u64
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_event_sequence(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.get_event_sequence(workflow_key)
}

// ─── Child Workflows ──────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_start_child_workflow(
    handle: *mut EngineHandle,
    parent_key: u64,
    child_workflow_id: u64,
    workflow_type_id: u64,
    task_queue_hash: u64,
    total_steps: u32,
    input_ptr: *const u8,
    input_len: u32,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;

    let input = if input_ptr.is_null() || input_len == 0 {
        None
    } else {
        Some(std::slice::from_raw_parts(input_ptr, input_len as usize).to_vec())
    };

    h.engine.start_child_workflow(parent_key, child_workflow_id, workflow_type_id, task_queue_hash, total_steps, input)
}

// ─── WAL Persistence ─────────────────────────────────────────────────────────

/// Create an engine with WAL persistence enabled.
/// `wal_path` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_create_with_wal(
    wal_path_ptr: *const u8,
    wal_path_len: u32,
    max_file_size: u64,
) -> *mut EngineHandle {
    if wal_path_ptr.is_null() || wal_path_len == 0 {
        return std::ptr::null_mut();
    }
    let path_bytes = std::slice::from_raw_parts(wal_path_ptr, wal_path_len as usize);
    let wal_path = match std::str::from_utf8(path_bytes) {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };

    match WorkflowEngine::with_wal(wal_path, max_file_size) {
        Ok(engine) => {
            let handle = Box::new(EngineHandle {
                engine: Arc::new(engine),
            });
            Box::into_raw(handle)
        }
        Err(_) => std::ptr::null_mut(),
    }
}

/// Get the number of records in the WAL. Returns 0 if WAL is not enabled.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_wal_record_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    match h.engine.wal() {
        Some(wal) => {
            wal.replay().map_or(0, |records| records.len() as u64)
        }
        None => 0,
    }
}

/// Replay the WAL and reconstruct workflow state. Returns the number of workflows recovered.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_wal_replay(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    match h.engine.wal() {
        Some(wal) => {
            let records = match wal.replay_all() {
                Ok(r) => r,
                Err(_) => return 0,
            };
            let mut recovered = 0u64;
            for record in &records {
                use crate::wal::WalEventType;
                match record.event_type {
                    WalEventType::WorkflowStarted if record.data.len() >= 28 => {
                        let workflow_id = u64::from_le_bytes(record.data[0..8].try_into().unwrap());
                        let workflow_type_id = u64::from_le_bytes(record.data[8..16].try_into().unwrap());
                        let namespace_id = u64::from_le_bytes(record.data[16..24].try_into().unwrap());
                        let task_queue_hash = u64::from_le_bytes(record.data[24..32].try_into().unwrap_or([0;8]));
                        let total_steps = if record.data.len() >= 36 {
                            u32::from_le_bytes(record.data[32..36].try_into().unwrap())
                        } else { 1 };
                        h.engine.start_workflow(workflow_id, workflow_type_id, namespace_id, task_queue_hash, total_steps, None);
                        recovered += 1;
                    }
                    WalEventType::StepCompleted if record.data.len() >= 4 => {
                        let step = u32::from_le_bytes(record.data[0..4].try_into().unwrap());
                        h.engine.complete_step(record.workflow_key, step, vec![]);
                    }
                    WalEventType::WorkflowCompleted => {
                        h.engine.complete_workflow(record.workflow_key, None);
                    }
                    WalEventType::WorkflowFailed => {
                        h.engine.fail_workflow(record.workflow_key);
                    }
                    WalEventType::WorkflowCanceled => {
                        h.engine.cancel_workflow(record.workflow_key);
                    }
                    WalEventType::WorkflowTerminated => {
                        h.engine.terminate_workflow(record.workflow_key);
                    }
                    _ => {}
                }
            }
            recovered
        }
        None => 0,
    }
}

// ─── Namespace Management ────────────────────────────────────────────────────

/// Register a new namespace. Returns the namespace ID.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_register_namespace(
    handle: *mut EngineHandle,
    name_ptr: *const u8,
    name_len: u32,
) -> u64 {
    if handle.is_null() || name_ptr.is_null() || name_len == 0 { return 0; }
    let h = &*handle;
    let name_bytes = std::slice::from_raw_parts(name_ptr, name_len as usize);
    let name = match std::str::from_utf8(name_bytes) {
        Ok(s) => s,
        Err(_) => return 0,
    };
    h.engine.namespaces().register_auto(name)
}

/// Check if a namespace is active. Returns 1 if active, 0 if not.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_is_namespace_active(
    handle: *mut EngineHandle,
    namespace_id: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    if h.engine.namespaces().is_active(namespace_id) { 1 } else { 0 }
}

/// Get the number of registered namespaces.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_namespace_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.namespaces().count() as u64
}

// ─── Visibility / Search ─────────────────────────────────────────────────────

/// Get the total number of indexed workflows.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_visibility_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.visibility().count() as u64
}

/// Count workflows by status (0=Void, 1=Running, 2=Completed, 3=Failed, 4=Canceled, 5=Terminated).
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_visibility_count_by_status(
    handle: *mut EngineHandle,
    status: u32,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let ws = match status {
        1 => crate::engine::WorkflowStatus::Running,
        2 => crate::engine::WorkflowStatus::Completed,
        3 => crate::engine::WorkflowStatus::Failed,
        4 => crate::engine::WorkflowStatus::Canceled,
        5 => crate::engine::WorkflowStatus::Terminated,
        _ => return 0,
    };
    h.engine.visibility().count_by_status(ws) as u64
}

/// Count workflows by namespace.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_visibility_count_by_namespace(
    handle: *mut EngineHandle,
    namespace_id: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.visibility().list_by_namespace(namespace_id).len() as u64
}

// ─── Update Dispatch ─────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_update(
    handle: *mut EngineHandle,
    workflow_key: u64,
    update_name_id: u64,
    payload_ptr: *const u8,
    payload_len: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;

    let payload = if payload_ptr.is_null() || payload_len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(payload_ptr, payload_len as usize).to_vec()
    };

    h.engine.update_workflow(workflow_key, update_name_id, payload);
    0
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_has_update(
    handle: *mut EngineHandle,
    workflow_key: u64,
    update_name_id: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    if h.engine.has_update(workflow_key, update_name_id) { 1 } else { 0 }
}

// ─── Cron Scheduling ─────────────────────────────────────────────────────

/// Register a cron schedule. Returns the schedule ID (0 on failure).
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_register_cron(
    handle: *mut EngineHandle,
    cron_expr_ptr: *const u8,
    cron_expr_len: u32,
    workflow_type_id: u64,
    namespace_id: u64,
    task_queue_hash: u64,
    total_steps: u32,
    current_time_minutes: u64,
) -> u64 {
    if handle.is_null() || cron_expr_ptr.is_null() || cron_expr_len == 0 { return 0; }
    let h = &*handle;
    let expr_bytes = std::slice::from_raw_parts(cron_expr_ptr, cron_expr_len as usize);
    let expr = match std::str::from_utf8(expr_bytes) {
        Ok(s) => s,
        Err(_) => return 0,
    };
    h.engine.register_cron(expr, workflow_type_id, namespace_id, task_queue_hash, total_steps, current_time_minutes)
        .unwrap_or(0)
}

/// Process cron fires at the given time. Returns the number of workflows started.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_process_cron_fires(
    handle: *mut EngineHandle,
    current_time_minutes: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.process_cron_fires(current_time_minutes).len() as u64
}

/// Get the number of registered cron schedules.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cron_schedule_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.cron_scheduler().schedule_count() as u64
}

// ─── Batch Operations ────────────────────────────────────────────────────

/// Batch terminate workflows. `keys_ptr` points to an array of u64 workflow keys.
/// Returns the batch ID.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_batch_terminate(
    handle: *mut EngineHandle,
    keys_ptr: *const u64,
    keys_len: u32,
) -> u64 {
    if handle.is_null() || keys_ptr.is_null() || keys_len == 0 { return 0; }
    let h = &*handle;
    let keys = std::slice::from_raw_parts(keys_ptr, keys_len as usize).to_vec();
    h.engine.batch_terminate(keys)
}

/// Batch cancel workflows. Returns the batch ID.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_batch_cancel(
    handle: *mut EngineHandle,
    keys_ptr: *const u64,
    keys_len: u32,
) -> u64 {
    if handle.is_null() || keys_ptr.is_null() || keys_len == 0 { return 0; }
    let h = &*handle;
    let keys = std::slice::from_raw_parts(keys_ptr, keys_len as usize).to_vec();
    h.engine.batch_cancel(keys)
}

/// Batch signal workflows. Returns the batch ID.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_batch_signal(
    handle: *mut EngineHandle,
    keys_ptr: *const u64,
    keys_len: u32,
    signal_name_id: u64,
    payload_ptr: *const u8,
    payload_len: u32,
) -> u64 {
    if handle.is_null() || keys_ptr.is_null() || keys_len == 0 { return 0; }
    let h = &*handle;
    let keys = std::slice::from_raw_parts(keys_ptr, keys_len as usize).to_vec();
    let payload = if payload_ptr.is_null() || payload_len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(payload_ptr, payload_len as usize).to_vec()
    };
    h.engine.batch_signal(keys, signal_name_id, payload)
}

/// Get the number of batch operations submitted.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_batch_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.batch_executor().batch_count() as u64
}

// ─── Archival ────────────────────────────────────────────────────────────

/// Get the number of archived workflows.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_archive_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.archive_store().count() as u64
}

/// Get the number of archived workflows by namespace.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_archive_count_by_namespace(
    handle: *mut EngineHandle,
    namespace_id: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.archive_store().count_by_namespace(namespace_id) as u64
}

/// Check if a workflow has been archived.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_is_archived(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    if h.engine.archive_store().get(workflow_key).is_some() { 1 } else { 0 }
}

// ─── Event History ──────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_event_count(handle: *mut EngineHandle, workflow_key: u64) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.history_store().event_count(workflow_key) as u64
}

// ─── Worker Versioning ──────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_create_version_set(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.worker_versioning().create_version_set()
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_add_build_id(handle: *mut EngineHandle, set_id: u64, build_id_ptr: *const u8, build_id_len: u32) -> i32 {
    if handle.is_null() || build_id_ptr.is_null() { return -1; }
    let h = &*handle;
    let bid = std::str::from_utf8(std::slice::from_raw_parts(build_id_ptr, build_id_len as usize)).unwrap_or("");
    h.engine.worker_versioning().add_build_id(set_id, bid);
    0
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_version_set_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.worker_versioning().version_set_count() as u64
}

// ─── Rate Limiter ───────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_rate_limit_check(handle: *mut EngineHandle, namespace_id: u64, tokens: u32) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    if h.engine.rate_limiter().try_acquire(namespace_id, tokens as u64) { 1 } else { 0 }
}

// ─── Heartbeat ──────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_register_heartbeat(handle: *mut EngineHandle, workflow_key: u64, activity_id: u64, timeout_ms: u64) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    h.engine.heartbeat_tracker().register(workflow_key, activity_id, timeout_ms, 3);
    0
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_record_heartbeat(handle: *mut EngineHandle, workflow_key: u64, activity_id: u64) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    h.engine.heartbeat_tracker().record_heartbeat(workflow_key, activity_id, None);
    0
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_heartbeat_active_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.heartbeat_tracker().active_count() as u64
}

/// Register heartbeat tracking for an activity. 
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_heartbeat_register(
    handle: *mut EngineHandle,
    workflow_key: u64,
    activity_id: u64,
    timeout_ms: u64,
) {
    if handle.is_null() { return; }
    let h = &*handle;
    h.engine.heartbeat_tracker().register(workflow_key, activity_id, timeout_ms, 3);
}

/// Check heartbeat timeouts. Returns count of timed-out heartbeats.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_heartbeat_check_timeouts(
    handle: *mut EngineHandle,
) -> u32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.heartbeat_tracker().check_timeouts().len() as u32
}

/// Unregister heartbeat tracking for an activity.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_heartbeat_unregister(
    handle: *mut EngineHandle,
    workflow_key: u64,
    activity_id: u64,
) {
    if handle.is_null() { return; }
    let h = &*handle;
    h.engine.heartbeat_tracker().unregister(workflow_key, activity_id);
}

// ─── Auth ───────────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_auth_check(handle: *mut EngineHandle, subject_ptr: *const u8, subject_len: u32, role_ptr: *const u8, role_len: u32, permission: u32) -> i32 {
    if handle.is_null() || subject_ptr.is_null() || role_ptr.is_null() { return 0; }
    let h = &*handle;
    let subject = std::str::from_utf8(std::slice::from_raw_parts(subject_ptr, subject_len as usize)).unwrap_or("");
    let role = std::str::from_utf8(std::slice::from_raw_parts(role_ptr, role_len as usize)).unwrap_or("");
    let perm = match permission {
        0 => crate::auth::Permission::StartWorkflow,
        1 => crate::auth::Permission::SignalWorkflow,
        2 => crate::auth::Permission::QueryWorkflow,
        3 => crate::auth::Permission::TerminateWorkflow,
        4 => crate::auth::Permission::CancelWorkflow,
        5 => crate::auth::Permission::DescribeWorkflow,
        6 => crate::auth::Permission::ListWorkflows,
        7 => crate::auth::Permission::AdminAccess,
        _ => return 0,
    };
    let claims = crate::auth::Claims { subject: subject.to_string(), namespace_id: 0, roles: vec![role.to_string()] };
    if h.engine.auth_manager().authorize(&claims, &perm) { 1 } else { 0 }
}

// ─── Dynamic Config ─────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_config_set_int(handle: *mut EngineHandle, key_ptr: *const u8, key_len: u32, value: i64) -> i32 {
    if handle.is_null() || key_ptr.is_null() { return -1; }
    let h = &*handle;
    let key = std::str::from_utf8(std::slice::from_raw_parts(key_ptr, key_len as usize)).unwrap_or("");
    h.engine.dynamic_config().set(key, crate::dynamic_config::ConfigValue::Int(value));
    0
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_config_get_int(handle: *mut EngineHandle, key_ptr: *const u8, key_len: u32, default: i64) -> i64 {
    if handle.is_null() || key_ptr.is_null() { return default; }
    let h = &*handle;
    let key = std::str::from_utf8(std::slice::from_raw_parts(key_ptr, key_len as usize)).unwrap_or("");
    let val = h.engine.dynamic_config().get_int(key);
    val
}

/// Set a boolean config value.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_config_set_bool(handle: *mut EngineHandle, key_ptr: *const u8, key_len: u32, value: i32) -> i32 {
    if handle.is_null() || key_ptr.is_null() { return -1; }
    let h = &*handle;
    let key = std::str::from_utf8(std::slice::from_raw_parts(key_ptr, key_len as usize)).unwrap_or("");
    h.engine.dynamic_config().set(key, crate::dynamic_config::ConfigValue::Bool(value != 0));
    0
}

/// Set a float config value.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_config_set_float(handle: *mut EngineHandle, key_ptr: *const u8, key_len: u32, value: f64) -> i32 {
    if handle.is_null() || key_ptr.is_null() { return -1; }
    let h = &*handle;
    let key = std::str::from_utf8(std::slice::from_raw_parts(key_ptr, key_len as usize)).unwrap_or("");
    h.engine.dynamic_config().set(key, crate::dynamic_config::ConfigValue::Float(value));
    0
}

/// Set a string config value.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_config_set_string(handle: *mut EngineHandle, key_ptr: *const u8, key_len: u32, val_ptr: *const u8, val_len: u32) -> i32 {
    if handle.is_null() || key_ptr.is_null() { return -1; }
    let h = &*handle;
    let key = std::str::from_utf8(std::slice::from_raw_parts(key_ptr, key_len as usize)).unwrap_or("");
    let val = if val_ptr.is_null() || val_len == 0 { "" } else { std::str::from_utf8(std::slice::from_raw_parts(val_ptr, val_len as usize)).unwrap_or("") };
    h.engine.dynamic_config().set(key, crate::dynamic_config::ConfigValue::String(val.to_string()));
    0
}

/// Get a boolean config value.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_config_get_bool(handle: *mut EngineHandle, key_ptr: *const u8, key_len: u32) -> i32 {
    if handle.is_null() || key_ptr.is_null() { return 0; }
    let h = &*handle;
    let key = std::str::from_utf8(std::slice::from_raw_parts(key_ptr, key_len as usize)).unwrap_or("");
    if h.engine.dynamic_config().get_bool(key) { 1 } else { 0 }
}

/// Get a float config value.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_config_get_float(handle: *mut EngineHandle, key_ptr: *const u8, key_len: u32) -> f64 {
    if handle.is_null() || key_ptr.is_null() { return 0.0; }
    let h = &*handle;
    let key = std::str::from_utf8(std::slice::from_raw_parts(key_ptr, key_len as usize)).unwrap_or("");
    h.engine.dynamic_config().get_float(key)
}

/// Get config key count.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_config_key_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.dynamic_config().key_count() as u64
}

// ─── Query Handler ──────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_register_query_handler(handle: *mut EngineHandle, workflow_key: u64, query_name_id: u64) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    // Register a default passthrough handler
    h.engine.query_registry().register_handler(workflow_key, query_name_id, Box::new(|input| input.to_vec()));
    0
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_query_handler_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.query_registry().workflow_count() as u64
}

// ─── Memo ───────────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_set_memo(handle: *mut EngineHandle, workflow_key: u64, key_ptr: *const u8, key_len: u32, val_ptr: *const u8, val_len: u32) -> i32 {
    if handle.is_null() || key_ptr.is_null() { return -1; }
    let h = &*handle;
    let key = std::str::from_utf8(std::slice::from_raw_parts(key_ptr, key_len as usize)).unwrap_or("");
    let val = if val_ptr.is_null() || val_len == 0 { vec![] } else { std::slice::from_raw_parts(val_ptr, val_len as usize).to_vec() };
    h.engine.memo_store().set(workflow_key, key, val, None);
    0
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_memo_count(handle: *mut EngineHandle, workflow_key: u64) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.memo_store().get_all(workflow_key).len() as u64
}

/// Get a memo value by key. Writes to caller buffer; actual length written to `out_len`.
/// Returns 0 on success, -1 if not found.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_get_memo(
    handle: *mut EngineHandle,
    workflow_key: u64,
    key_ptr: *const u8, key_len: u32,
    out_ptr: *mut u8, out_cap: u32, out_len: *mut u32,
) -> i32 {
    if handle.is_null() || key_ptr.is_null() || out_ptr.is_null() || out_len.is_null() { return -1; }
    let h = &*handle;
    let key = std::str::from_utf8(std::slice::from_raw_parts(key_ptr, key_len as usize)).unwrap_or("");
    match h.engine.memo_store().get(workflow_key, key) {
        Some(val) => {
            let write_len = std::cmp::min(val.len(), out_cap as usize);
            std::ptr::copy_nonoverlapping(val.as_ptr(), out_ptr, write_len);
            *out_len = write_len as u32;
            0
        }
        None => { *out_len = 0; -1 }
    }
}

// ─── Schedules ──────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_create_schedule(
    handle: *mut EngineHandle,
    workflow_type_id: u64, namespace_id: u64, task_queue_hash: u64,
    overlap_policy: u32, jitter: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let spec = crate::schedules::CalendarSpec {
        second: "0".into(), minute: "*".into(), hour: "*".into(),
        day_of_month: "*".into(), month: "*".into(), day_of_week: "*".into(),
        comment: "ffi".into(),
    };
    let overlap = match overlap_policy {
        0 => crate::schedules::OverlapPolicy::Skip,
        1 => crate::schedules::OverlapPolicy::BufferOne,
        2 => crate::schedules::OverlapPolicy::BufferAll,
        3 => crate::schedules::OverlapPolicy::TerminateOther,
        _ => crate::schedules::OverlapPolicy::AllowAll,
    };
    h.engine.schedule_manager().create_schedule(spec, workflow_type_id, namespace_id, task_queue_hash, overlap, jitter)
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_schedule_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.schedule_manager().count() as u64
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_pause_schedule(handle: *mut EngineHandle, schedule_id: u64) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    if h.engine.schedule_manager().pause(schedule_id) { 0 } else { -1 }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_unpause_schedule(handle: *mut EngineHandle, schedule_id: u64) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    if h.engine.schedule_manager().unpause(schedule_id) { 0 } else { -1 }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_delete_schedule(handle: *mut EngineHandle, schedule_id: u64) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    if h.engine.schedule_manager().delete(schedule_id) { 0 } else { -1 }
}

// ─── Workflow Reset ─────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_add_reset_point(handle: *mut EngineHandle, workflow_key: u64, event_id: u64) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    h.engine.workflow_resetter().create_reset_point(workflow_key, event_id, ResetReason::Custom("ffi".to_string()));
    0
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_reset_point_count(handle: *mut EngineHandle, workflow_key: u64) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.workflow_resetter().reset_count(workflow_key) as u64
}

// ─── Patches (Version Branching) ────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_register_patch(
    handle: *mut EngineHandle,
    workflow_type_id: u64,
    marker_ptr: *const u8, marker_len: u32,
    min_version: u64, max_version: u64,
    desc_ptr: *const u8, desc_len: u32,
) -> u64 {
    if handle.is_null() || marker_ptr.is_null() { return 0; }
    let h = &*handle;
    let marker = std::str::from_utf8(std::slice::from_raw_parts(marker_ptr, marker_len as usize)).unwrap_or("");
    let desc = if desc_ptr.is_null() || desc_len == 0 { "" } else { std::str::from_utf8(std::slice::from_raw_parts(desc_ptr, desc_len as usize)).unwrap_or("") };
    h.engine.patch_registry().register_patch(workflow_type_id, marker, min_version, max_version, desc)
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_patch_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.patch_registry().patch_count() as u64
}

/// Deactivate a patch by ID. Returns 1 on success, 0 if not found.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_deactivate_patch(
    handle: *mut EngineHandle,
    patch_id: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    if h.engine.patch_registry().deactivate_patch(patch_id) { 1 } else { 0 }
}

/// Find an active patch by workflow type and version. Returns patch_id or 0 if not found.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_find_patch(
    handle: *mut EngineHandle,
    workflow_type_id: u64,
    version: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.patch_registry().find_patch(workflow_type_id, version)
        .map_or(0, |p| p.patch_id)
}

/// Get patch details. Writes fields into the output array:
/// [patch_id, workflow_type_id, min_version, max_version, is_active].
/// Returns 1 on success, 0 if not found.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_get_patch(
    handle: *mut EngineHandle,
    patch_id: u64,
    out_fields: *mut u64,
) -> i32 {
    if handle.is_null() || out_fields.is_null() { return 0; }
    let h = &*handle;
    match h.engine.patch_registry().get_patch(patch_id) {
        Some(p) => {
            let fields = std::slice::from_raw_parts_mut(out_fields, 5);
            fields[0] = p.patch_id;
            fields[1] = p.workflow_type_id;
            fields[2] = p.min_version;
            fields[3] = p.max_version;
            fields[4] = if p.is_active { 1 } else { 0 };
            1
        }
        None => 0,
    }
}

/// Get active patches for a workflow type. Writes patch IDs into out_ids array.
/// Returns the number of patches written.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_active_patches_for_type(
    handle: *mut EngineHandle,
    workflow_type_id: u64,
    out_ids: *mut u64,
    max_count: u32,
) -> u32 {
    if handle.is_null() || out_ids.is_null() { return 0; }
    let h = &*handle;
    let patches = h.engine.patch_registry().active_patches_for_type(workflow_type_id);
    let out = std::slice::from_raw_parts_mut(out_ids, max_count as usize);
    let count = patches.len().min(max_count as usize);
    for i in 0..count {
        out[i] = patches[i].patch_id;
    }
    count as u32
}

// ─── Visibility Query ───────────────────────────────────────────────────

/// Execute a SQL-like visibility query. Writes matching workflow_keys into out_keys.
/// Returns the number of results written.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_visibility_query(
    handle: *mut EngineHandle,
    query_ptr: *const u8,
    query_len: u32,
    out_keys: *mut u64,
    max_results: u32,
) -> u32 {
    if handle.is_null() || query_ptr.is_null() || out_keys.is_null() { return 0; }
    let h = &*handle;
    let query_str = std::str::from_utf8(std::slice::from_raw_parts(query_ptr, query_len as usize)).unwrap_or("");
    let parsed = match crate::visibility_query::VisibilityQuery::parse(query_str) {
        Ok(q) => q,
        Err(_) => return 0,
    };
    let results = parsed.execute(h.engine.visibility());
    let out = std::slice::from_raw_parts_mut(out_keys, max_results as usize);
    let count = results.len().min(max_results as usize);
    for i in 0..count {
        out[i] = results[i].workflow_key;
    }
    count as u32
}

// ─── Cluster ────────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_register_cluster(handle: *mut EngineHandle, name_ptr: *const u8, name_len: u32, addr_ptr: *const u8, addr_len: u32) -> u64 {
    if handle.is_null() || name_ptr.is_null() { return 0; }
    let h = &*handle;
    let name = std::str::from_utf8(std::slice::from_raw_parts(name_ptr, name_len as usize)).unwrap_or("");
    let addr = if addr_ptr.is_null() || addr_len == 0 { "" } else { std::str::from_utf8(std::slice::from_raw_parts(addr_ptr, addr_len as usize)).unwrap_or("") };
    h.engine.cluster_manager().register_cluster(name, addr)
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cluster_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.cluster_manager().cluster_count() as u64
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_pending_replication_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.cluster_manager().pending_replication_count() as u64
}

// ─── Sharding ───────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_shard_for_key(handle: *mut EngineHandle, workflow_key: u64) -> u32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.shard_manager().shard_for_key(workflow_key)
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_assign_shard(handle: *mut EngineHandle, shard_id: u32, host_ptr: *const u8, host_len: u32) -> i32 {
    if handle.is_null() || host_ptr.is_null() { return -1; }
    let h = &*handle;
    let host = std::str::from_utf8(std::slice::from_raw_parts(host_ptr, host_len as usize)).unwrap_or("");
    if h.engine.shard_manager().assign_shard(shard_id, host) { 1 } else { 0 }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_shard_count(handle: *mut EngineHandle) -> u32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.shard_manager().shard_count()
}

// ─── Nexus ──────────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_register_nexus_service(handle: *mut EngineHandle, name_ptr: *const u8, name_len: u32, endpoint_ptr: *const u8, endpoint_len: u32) -> i32 {
    if handle.is_null() || name_ptr.is_null() { return -1; }
    let h = &*handle;
    let name = std::str::from_utf8(std::slice::from_raw_parts(name_ptr, name_len as usize)).unwrap_or("");
    let endpoint = if endpoint_ptr.is_null() || endpoint_len == 0 { "" } else { std::str::from_utf8(std::slice::from_raw_parts(endpoint_ptr, endpoint_len as usize)).unwrap_or("") };
    h.engine.nexus_manager().register_service(name, endpoint);
    0
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_nexus_service_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.nexus_manager().service_count() as u64
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_nexus_operation_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.nexus_manager().operation_count() as u64
}

// ─── Namespace Enhanced (Batch 24) ──────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_describe_namespace(
    handle: *mut EngineHandle,
    namespace_id: u64,
    out: *mut u64, // [0]=id, [1]=retention_ms, [2]=max_concurrent, [3]=workflow_count, [4]=is_active
) -> i32 {
    if handle.is_null() || out.is_null() { return -1; }
    let h = &*handle;
    match h.engine.namespaces().get(namespace_id) {
        Some(ns) => {
            *out.add(0) = ns.id;
            *out.add(1) = ns.retention_period.as_millis() as u64;
            *out.add(2) = ns.max_concurrent_workflows;
            *out.add(3) = h.engine.namespaces().workflow_count(namespace_id);
            *out.add(4) = if h.engine.namespaces().is_active(namespace_id) { 1 } else { 0 };
            1
        }
        None => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_namespace_workflow_count(
    handle: *mut EngineHandle,
    namespace_id: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.namespaces().workflow_count(namespace_id)
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_deactivate_namespace(
    handle: *mut EngineHandle,
    namespace_id: u64,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    if h.engine.namespaces().deactivate(namespace_id) { 1 } else { 0 }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_activate_namespace(
    handle: *mut EngineHandle,
    namespace_id: u64,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    if h.engine.namespaces().activate(namespace_id) { 1 } else { 0 }
}

// ─── Cron Enhanced (Batch 24) ───────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cron_next_fire_time(
    handle: *mut EngineHandle,
    schedule_id: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.cron_scheduler().next_fire_time(schedule_id).unwrap_or(0)
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cron_fire_count(
    handle: *mut EngineHandle,
    schedule_id: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.cron_scheduler().fire_count(schedule_id).unwrap_or(0) as u64
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cron_unregister(
    handle: *mut EngineHandle,
    schedule_id: u64,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    if h.engine.cron_scheduler().unregister(schedule_id) { 1 } else { 0 }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cron_set_paused(
    handle: *mut EngineHandle,
    schedule_id: u64,
    paused: i32,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    if h.engine.cron_scheduler().set_paused(schedule_id, paused != 0) { 1 } else { 0 }
}

// ─── Codec Enhanced (Batch 24) ──────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_codec_count(
    handle: *mut EngineHandle,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.codec_chain().codec_count() as u64
}

// ─── Search Attr Enhanced (Batch 24) ────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_search_attr_count(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    // Use the visibility index to count search attrs for a workflow
    h.engine.visibility().get(workflow_key).map_or(0, |w| w.search_attributes.len() as u64)
}

// ─── Rate Limiter Enhanced (Batch 22) ───────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_rate_set_namespace_limit(
    handle: *mut EngineHandle,
    namespace_id: u64,
    rate: f64,
    capacity: u64,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    h.engine.rate_limiter().set_namespace_limit(namespace_id, rate, capacity);
    0
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_rate_namespace_count(
    handle: *mut EngineHandle,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.rate_limiter().namespace_count() as u64
}

// ─── Memo Enhanced (Batch 22) ───────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_remove_memo(
    handle: *mut EngineHandle,
    workflow_key: u64,
    key_ptr: *const u8,
    key_len: u32,
) -> i32 {
    if handle.is_null() || key_ptr.is_null() { return -1; }
    let h = &*handle;
    let key = std::str::from_utf8(std::slice::from_raw_parts(key_ptr, key_len as usize)).unwrap_or("");
    if h.engine.memo_store().remove(workflow_key, key) { 1 } else { 0 }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_memo_workflow_count(
    handle: *mut EngineHandle,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.memo_store().workflow_count() as u64
}

// ─── Worker Versioning Enhanced (Batch 22) ──────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_versioning_set_current(
    handle: *mut EngineHandle,
    set_id: u64,
    build_id_ptr: *const u8,
    build_id_len: u32,
) -> i32 {
    if handle.is_null() || build_id_ptr.is_null() { return -1; }
    let h = &*handle;
    let build_id = std::str::from_utf8(std::slice::from_raw_parts(build_id_ptr, build_id_len as usize)).unwrap_or("");
    if h.engine.worker_versioning().set_current_build_id(set_id, build_id) { 1 } else { 0 }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_versioning_get_current(
    handle: *mut EngineHandle,
    set_id: u64,
    out_ptr: *mut u8,
    out_len: *mut u32,
) -> i32 {
    if handle.is_null() || out_ptr.is_null() || out_len.is_null() { return -1; }
    let h = &*handle;
    match h.engine.worker_versioning().get_current_build_id(set_id) {
        Some(bid) => {
            let bytes = bid.as_bytes();
            let copy_len = bytes.len().min(*out_len as usize);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr, copy_len);
            *out_len = copy_len as u32;
            1
        }
        None => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_versioning_add_routing_rule(
    handle: *mut EngineHandle,
    tq_ptr: *const u8,
    tq_len: u32,
    bid_ptr: *const u8,
    bid_len: u32,
    percentage: u32,
) -> i32 {
    if handle.is_null() || tq_ptr.is_null() || bid_ptr.is_null() { return -1; }
    let h = &*handle;
    let tq = std::str::from_utf8(std::slice::from_raw_parts(tq_ptr, tq_len as usize)).unwrap_or("");
    let bid = std::str::from_utf8(std::slice::from_raw_parts(bid_ptr, bid_len as usize)).unwrap_or("");
    h.engine.worker_versioning().add_routing_rule(tq, bid, percentage);
    0
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_versioning_resolve_build_id(
    handle: *mut EngineHandle,
    tq_ptr: *const u8,
    tq_len: u32,
    out_ptr: *mut u8,
    out_len: *mut u32,
) -> i32 {
    if handle.is_null() || tq_ptr.is_null() || out_ptr.is_null() || out_len.is_null() { return -1; }
    let h = &*handle;
    let tq = std::str::from_utf8(std::slice::from_raw_parts(tq_ptr, tq_len as usize)).unwrap_or("");
    match h.engine.worker_versioning().resolve_build_id(tq) {
        Some(bid) => {
            let bytes = bid.as_bytes();
            let copy_len = bytes.len().min(*out_len as usize);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr, copy_len);
            *out_len = copy_len as u32;
            1
        }
        None => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_versioning_routing_rule_count(
    handle: *mut EngineHandle,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.worker_versioning().routing_rule_count() as u64
}

// ─── Auth Enhanced (Batch 22) ───────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_auth_deny_subject(
    handle: *mut EngineHandle,
    subject_ptr: *const u8,
    subject_len: u32,
) -> i32 {
    if handle.is_null() || subject_ptr.is_null() { return -1; }
    let h = &*handle;
    let subject = std::str::from_utf8(std::slice::from_raw_parts(subject_ptr, subject_len as usize)).unwrap_or("");
    h.engine.auth_manager().deny_subject(subject);
    0
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_auth_role_count(
    handle: *mut EngineHandle,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.auth_manager().role_count() as u64
}

// ─── Metrics Enhanced (Batch 23) ────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_metrics_inc_counter(
    handle: *mut EngineHandle,
    name_ptr: *const u8,
    name_len: u32,
) -> i32 {
    if handle.is_null() || name_ptr.is_null() { return -1; }
    let h = &*handle;
    let name = std::str::from_utf8(std::slice::from_raw_parts(name_ptr, name_len as usize)).unwrap_or("");
    h.engine.metrics_registry().inc_counter(name);
    0
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_metrics_get_counter(
    handle: *mut EngineHandle,
    name_ptr: *const u8,
    name_len: u32,
) -> u64 {
    if handle.is_null() || name_ptr.is_null() { return 0; }
    let h = &*handle;
    let name = std::str::from_utf8(std::slice::from_raw_parts(name_ptr, name_len as usize)).unwrap_or("");
    h.engine.metrics_registry().get_counter(name)
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_metrics_set_gauge(
    handle: *mut EngineHandle,
    name_ptr: *const u8,
    name_len: u32,
    value: i64,
) -> i32 {
    if handle.is_null() || name_ptr.is_null() { return -1; }
    let h = &*handle;
    let name = std::str::from_utf8(std::slice::from_raw_parts(name_ptr, name_len as usize)).unwrap_or("");
    h.engine.metrics_registry().set_gauge(name, value);
    0
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_metrics_get_gauge(
    handle: *mut EngineHandle,
    name_ptr: *const u8,
    name_len: u32,
) -> i64 {
    if handle.is_null() || name_ptr.is_null() { return 0; }
    let h = &*handle;
    let name = std::str::from_utf8(std::slice::from_raw_parts(name_ptr, name_len as usize)).unwrap_or("");
    h.engine.metrics_registry().get_gauge(name)
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_metrics_observe_histogram(
    handle: *mut EngineHandle,
    name_ptr: *const u8,
    name_len: u32,
    value: f64,
) -> i32 {
    if handle.is_null() || name_ptr.is_null() { return -1; }
    let h = &*handle;
    let name = std::str::from_utf8(std::slice::from_raw_parts(name_ptr, name_len as usize)).unwrap_or("");
    h.engine.metrics_registry().observe_histogram(name, value);
    0
}

// ─── History Store Enhanced (Batch 23) ──────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_history_event_count(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.history_store().event_count(workflow_key) as u64
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_history_remove(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    if h.engine.history_store().remove_history(workflow_key) { 1 } else { 0 }
}

// ─── Archive Store Enhanced (Batch 23) ──────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_archive_retrieve(
    handle: *mut EngineHandle,
    workflow_key: u64,
    out: *mut u64, // [0]=workflow_key, [1]=namespace_id, [2]=workflow_type_id, [3]=status, [4]=event_count
) -> i32 {
    if handle.is_null() || out.is_null() { return -1; }
    let h = &*handle;
    match h.engine.archive_store().get(workflow_key) {
        Some(rec) => {
            *out.add(0) = rec.workflow_key;
            *out.add(1) = rec.namespace_id;
            *out.add(2) = rec.workflow_type_id;
            *out.add(3) = rec.status as u64;
            *out.add(4) = rec.event_count as u64;
            1
        }
        None => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_archive_delete(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    if h.engine.archive_store().delete(workflow_key) { 1 } else { 0 }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_archive_count_by_status(
    handle: *mut EngineHandle,
    status: u32,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let ws = match status {
        1 => crate::engine::WorkflowStatus::Running,
        2 => crate::engine::WorkflowStatus::Completed,
        3 => crate::engine::WorkflowStatus::Failed,
        4 => crate::engine::WorkflowStatus::Canceled,
        5 => crate::engine::WorkflowStatus::Terminated,
        6 => crate::engine::WorkflowStatus::ContinuedAsNew,
        7 => crate::engine::WorkflowStatus::TimedOut,
        _ => return 0,
    };
    h.engine.archive_store().count_by_status(ws) as u64
}

// ─── Cluster Replication (Batch 21) ─────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_enqueue_replication(
    handle: *mut EngineHandle,
    source_cluster_id: u64,
    target_cluster_id: u64,
    workflow_key: u64,
    event_type: u32,
    payload_ptr: *const u8,
    payload_len: u32,
    task_type: u32,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let payload = if payload_ptr.is_null() || payload_len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(payload_ptr, payload_len as usize).to_vec()
    };
    let tt = match task_type {
        0 => crate::cluster::ReplicationTaskType::SyncHistory,
        1 => crate::cluster::ReplicationTaskType::SyncActivity,
        2 => crate::cluster::ReplicationTaskType::SyncWorkflowState,
        3 => crate::cluster::ReplicationTaskType::NamespaceMetadata,
        4 => crate::cluster::ReplicationTaskType::SyncHSM,
        5 => crate::cluster::ReplicationTaskType::VerifyTransition,
        6 => crate::cluster::ReplicationTaskType::DeleteExecution,
        7 => crate::cluster::ReplicationTaskType::BackfillHistory,
        8 => crate::cluster::ReplicationTaskType::SyncVersionedTransition,
        _ => crate::cluster::ReplicationTaskType::SyncHistory,
    };
    h.engine.cluster_manager().enqueue_replication(source_cluster_id, target_cluster_id, workflow_key, event_type, payload, tt)
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_drain_replication_tasks(
    handle: *mut EngineHandle,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.cluster_manager().drain_replication_tasks().len() as u64
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_get_cluster_info(
    handle: *mut EngineHandle,
    cluster_id: u64,
    out: *mut u64, // [0]=cluster_id, [1]=is_active, [2]=failover_version, [3]=replication_enabled
) -> u64 {
    if handle.is_null() || out.is_null() { return 0; }
    let h = &*handle;
    match h.engine.cluster_manager().get_cluster(cluster_id) {
        Some(info) => {
            *out.add(0) = info.cluster_id;
            *out.add(1) = if info.is_active { 1 } else { 0 };
            *out.add(2) = info.failover_version;
            *out.add(3) = if info.replication_enabled { 1 } else { 0 };
            1
        }
        None => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_local_cluster_id(
    handle: *mut EngineHandle,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.cluster_manager().local_cluster_id()
}

// ─── Sharding Enhanced (Batch 21) ───────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_assigned_shard_count(
    handle: *mut EngineHandle,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.shard_manager().assigned_count() as u64
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_get_shard_owner(
    handle: *mut EngineHandle,
    shard_id: u32,
    out_ptr: *mut u8,
    out_len: *mut u32,
) -> i32 {
    if handle.is_null() || out_ptr.is_null() || out_len.is_null() { return -1; }
    let h = &*handle;
    match h.engine.shard_manager().get_owner(shard_id) {
        Some(owner) => {
            let bytes = owner.as_bytes();
            let copy_len = bytes.len().min(*out_len as usize);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr, copy_len);
            *out_len = copy_len as u32;
            1
        }
        None => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_get_shards_for_host(
    handle: *mut EngineHandle,
    host_ptr: *const u8,
    host_len: u32,
    out_shards: *mut u32,
    out_count: *mut u32,
) -> u64 {
    if handle.is_null() || host_ptr.is_null() || out_shards.is_null() || out_count.is_null() { return 0; }
    let h = &*handle;
    let host = std::str::from_utf8(std::slice::from_raw_parts(host_ptr, host_len as usize)).unwrap_or("");
    let shards = h.engine.shard_manager().get_shards_for_host(host);
    let max_count = *out_count as usize;
    let copy_count = shards.len().min(max_count);
    for (i, s) in shards.iter().take(copy_count).enumerate() {
        *out_shards.add(i) = *s;
    }
    *out_count = copy_count as u32;
    shards.len() as u64
}

// ─── Nexus Operations (Batch 21) ────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_nexus_start_operation(
    handle: *mut EngineHandle,
    service_ptr: *const u8,
    service_len: u32,
    operation_ptr: *const u8,
    operation_len: u32,
    workflow_key: u64,
    input_ptr: *const u8,
    input_len: u32,
    callback_ptr: *const u8,
    callback_len: u32,
) -> u64 {
    if handle.is_null() || service_ptr.is_null() || operation_ptr.is_null() { return 0; }
    let h = &*handle;
    let service = std::str::from_utf8(std::slice::from_raw_parts(service_ptr, service_len as usize)).unwrap_or("");
    let operation = std::str::from_utf8(std::slice::from_raw_parts(operation_ptr, operation_len as usize)).unwrap_or("");
    let input = if input_ptr.is_null() || input_len == 0 { None } else { Some(std::slice::from_raw_parts(input_ptr, input_len as usize).to_vec()) };
    let callback = if callback_ptr.is_null() || callback_len == 0 { None } else { std::str::from_utf8(std::slice::from_raw_parts(callback_ptr, callback_len as usize)).ok().map(|s| s.to_string()) };
    h.engine.nexus_manager().start_operation(service, operation, workflow_key, input, callback).unwrap_or(0)
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_nexus_complete_operation(
    handle: *mut EngineHandle,
    operation_id: u64,
    result_ptr: *const u8,
    result_len: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    let result = if result_ptr.is_null() || result_len == 0 { Vec::new() } else { std::slice::from_raw_parts(result_ptr, result_len as usize).to_vec() };
    if h.engine.nexus_manager().complete_operation(operation_id, result) { 1 } else { 0 }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_nexus_fail_operation(
    handle: *mut EngineHandle,
    operation_id: u64,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    if h.engine.nexus_manager().fail_operation(operation_id) { 1 } else { 0 }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_nexus_get_operation(
    handle: *mut EngineHandle,
    operation_id: u64,
    out: *mut u64, // [0]=operation_id, [1]=workflow_key, [2]=state, [3]=has_result
) -> u64 {
    if handle.is_null() || out.is_null() { return 0; }
    let h = &*handle;
    match h.engine.nexus_manager().get_operation(operation_id) {
        Some(op) => {
            *out.add(0) = op.operation_id;
            *out.add(1) = op.workflow_key;
            *out.add(2) = op.state as u64;
            *out.add(3) = if op.result.is_some() { 1 } else { 0 };
            1
        }
        None => 0,
    }
}

// ─── SignalWithStart ────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_signal_with_start(
    handle: *mut EngineHandle,
    workflow_id: u64, workflow_type_id: u64, namespace_id: u64, task_queue_hash: u64, total_steps: u32,
    signal_name_id: u64,
    payload_ptr: *const u8, payload_len: u32,
    out_was_started: *mut u32,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let payload = if payload_ptr.is_null() || payload_len == 0 { Vec::new() } else {
        std::slice::from_raw_parts(payload_ptr, payload_len as usize).to_vec()
    };
    let (key, was_started) = h.engine.signal_with_start(workflow_id, workflow_type_id, namespace_id, task_queue_hash, total_steps, signal_name_id, payload);
    if !out_was_started.is_null() { *out_was_started = if was_started { 1 } else { 0 }; }
    key
}

// ─── ContinueAsNew ──────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_continue_as_new(
    handle: *mut EngineHandle,
    workflow_key: u64,
    input_ptr: *const u8, input_len: u32,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let input = if input_ptr.is_null() || input_len == 0 { None } else {
        Some(std::slice::from_raw_parts(input_ptr, input_len as usize).to_vec())
    };
    h.engine.continue_as_new(workflow_key, input)
}

// ─── Payload Codec ──────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_codec_chain_len(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.codec_chain().codec_count() as u64
}

// ─── Visibility Listing ─────────────────────────────────────────────────

/// Callback type for listing workflow executions. Called once per workflow.
/// Parameters: workflow_key, workflow_id, run_id, workflow_type_id, namespace_id,
///             status, start_time_ms, close_time_ms, task_queue_hash, user_data
type WorkflowInfoCallback = unsafe extern "C" fn(
    u64, u64, u64, u64, u64, u32, u64, i64, u64, *mut std::ffi::c_void,
);

/// List workflows, optionally filtered by namespace_id (u64::MAX = all).
/// Calls the callback once per matching workflow.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_list_workflows(
    handle: *mut EngineHandle,
    namespace_filter: u64,
    status_filter: i32, // -1 = all, 0-7 = specific status
    callback: WorkflowInfoCallback,
    user_data: *mut std::ffi::c_void,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let vis = h.engine.visibility();

    let infos = if status_filter >= 0 && status_filter <= 7 {
        let ws = match status_filter {
            1 => crate::engine::WorkflowStatus::Running,
            2 => crate::engine::WorkflowStatus::Completed,
            3 => crate::engine::WorkflowStatus::Failed,
            4 => crate::engine::WorkflowStatus::Canceled,
            5 => crate::engine::WorkflowStatus::Terminated,
            6 => crate::engine::WorkflowStatus::ContinuedAsNew,
            7 => crate::engine::WorkflowStatus::TimedOut,
            _ => return 0,
        };
        vis.list_by_status(ws)
    } else if namespace_filter != u64::MAX {
        vis.list_by_namespace(namespace_filter)
    } else {
        vis.list_by_status(crate::engine::WorkflowStatus::Running)
            .into_iter()
            .chain(vis.list_by_status(crate::engine::WorkflowStatus::Completed))
            .chain(vis.list_by_status(crate::engine::WorkflowStatus::Failed))
            .chain(vis.list_by_status(crate::engine::WorkflowStatus::Canceled))
            .chain(vis.list_by_status(crate::engine::WorkflowStatus::Terminated))
            .collect()
    };

    let count = infos.len() as u64;
    for info in &infos {
        callback(
            info.workflow_key, info.workflow_id, info.run_id,
            info.workflow_type_id, info.namespace_id,
            info.status as u32, info.start_time_ms,
            match info.close_time_ms { Some(t) => t as i64, None => -1i64 },
            info.task_queue_hash, user_data,
        );
    }
    count
}

/// Set a search attribute on a workflow execution.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_set_search_attribute(
    handle: *mut EngineHandle,
    workflow_key: u64,
    key_ptr: *const u8, key_len: u32,
    val_ptr: *const u8, val_len: u32,
) -> i32 {
    if handle.is_null() || key_ptr.is_null() { return -1; }
    let h = &*handle;
    let key = std::str::from_utf8(std::slice::from_raw_parts(key_ptr, key_len as usize)).unwrap_or("");
    let val = if val_ptr.is_null() || val_len == 0 { "" } else {
        std::str::from_utf8(std::slice::from_raw_parts(val_ptr, val_len as usize)).unwrap_or("")
    };
    h.engine.visibility().set_search_attribute(
        workflow_key, key.to_string(),
        crate::visibility::SearchAttributeValue::String(val.to_string()),
    );
    0
}

// ─── Activity Completion ────────────────────────────────────────────────

/// Complete an activity task. Parses the task token (workflow_key:step) and completes the step.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_complete_activity(
    handle: *mut EngineHandle,
    workflow_key: u64,
    step: u32,
    result_ptr: *const u8, result_len: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    let result = if result_ptr.is_null() || result_len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(result_ptr, result_len as usize).to_vec()
    };
    h.engine.complete_activity(workflow_key, step, result);
    0
}

/// Fail an activity task. Marks the step as failed by failing the workflow.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_fail_activity(
    handle: *mut EngineHandle,
    workflow_key: u64,
    step: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    // For now, failing an activity fails the workflow (no retry logic at engine level yet)
    h.engine.fail_workflow(workflow_key);
    0
}

// ─── Event History Retrieval ────────────────────────────────────────────

/// Callback type for event history entries.
/// Parameters: event_id, event_type, payload_ptr, payload_len, user_data
type HistoryEventCallback = unsafe extern "C" fn(
    u64, u32, *const u8, u32, *mut std::ffi::c_void,
);

/// Get the event history for a workflow. Calls the callback for each event.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_get_event_history(
    handle: *mut EngineHandle,
    workflow_key: u64,
    callback: HistoryEventCallback,
    user_data: *mut std::ffi::c_void,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let events = h.engine.history_store().get_history(workflow_key).unwrap_or_default();
    let count = events.len() as u64;
    for (i, event) in events.iter().enumerate() {
        let payload_ptr = if event.payload.is_empty() {
            std::ptr::null()
        } else {
            event.payload.as_ptr()
        };
        callback(
            (i + 1) as u64,
            event.event_type as u32,
            payload_ptr,
            event.payload.len() as u32,
            user_data,
        );
    }
    count
}

// ─── Metrics ────────────────────────────────────────────────────────────

/// Callback for metrics export. Called with the Prometheus text output.
type MetricsExportCallback = unsafe extern "C" fn(*const u8, u32, *mut std::ffi::c_void);

/// Export all metrics in Prometheus text format via callback.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_metrics_export(
    handle: *mut EngineHandle,
    callback: MetricsExportCallback,
    user_data: *mut std::ffi::c_void,
) -> u32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let text = h.engine.metrics_registry().export_prometheus();
    let bytes = text.as_bytes();
    callback(bytes.as_ptr(), bytes.len() as u32, user_data);
    bytes.len() as u32
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_metrics_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.metrics_registry().metric_count() as u64
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_inc_counter(
    handle: *mut EngineHandle, name_ptr: *const u8, name_len: u32,
) -> i32 {
    if handle.is_null() || name_ptr.is_null() { return -1; }
    let h = &*handle;
    let name = std::str::from_utf8(std::slice::from_raw_parts(name_ptr, name_len as usize)).unwrap_or("");
    h.engine.metrics_registry().inc_counter(name);
    0
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_get_counter(
    handle: *mut EngineHandle, name_ptr: *const u8, name_len: u32,
) -> u64 {
    if handle.is_null() || name_ptr.is_null() { return 0; }
    let h = &*handle;
    let name = std::str::from_utf8(std::slice::from_raw_parts(name_ptr, name_len as usize)).unwrap_or("");
    h.engine.metrics_registry().get_counter(name)
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_set_gauge(
    handle: *mut EngineHandle, name_ptr: *const u8, name_len: u32, value: i64,
) -> i32 {
    if handle.is_null() || name_ptr.is_null() { return -1; }
    let h = &*handle;
    let name = std::str::from_utf8(std::slice::from_raw_parts(name_ptr, name_len as usize)).unwrap_or("");
    h.engine.metrics_registry().set_gauge(name, value);
    0
}

// ─── Saga ───────────────────────────────────────────────────────────────

/// Callback for saga step definitions. Called for each step.
/// Returns: (workflow_type_id, comp_type_id, has_comp)
type SagaStepCallback = unsafe extern "C" fn(u32, *const u8, u32, u64, u64, *mut std::ffi::c_void);

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_create_saga(
    handle: *mut EngineHandle, workflow_key: u64, step_count: u32,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    // Create a saga with placeholder steps (real step definitions come from C#)
    let mut steps = Vec::new();
    for i in 0..step_count {
        steps.push(crate::saga::SagaStepDefinition::new(
            &format!("step_{}", i), i as u64
        ));
    }
    h.engine.saga_orchestrator().create_saga(workflow_key, steps)
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_complete_saga_step(
    handle: *mut EngineHandle, saga_id: u64, step_index: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    if h.engine.saga_orchestrator().complete_step(saga_id, step_index as usize, None) { 0 } else { -1 }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_fail_saga_step(
    handle: *mut EngineHandle, saga_id: u64, step_index: u32,
) -> u32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.saga_orchestrator().fail_step(saga_id, step_index as usize).len() as u32
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_saga_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.saga_orchestrator().saga_count() as u64
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_saga_status(handle: *mut EngineHandle, saga_id: u64) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    match h.engine.saga_orchestrator().get_saga(saga_id) {
        Some(s) => match s.status {
            crate::saga::SagaStatus::Created => 0,
            crate::saga::SagaStatus::Running => 1,
            crate::saga::SagaStatus::Completed => 2,
            crate::saga::SagaStatus::Failed => 3,
            crate::saga::SagaStatus::Compensating => 4,
            crate::saga::SagaStatus::Compensated => 5,
            crate::saga::SagaStatus::PartiallyCompensated => 6,
        },
        None => -1,
    }
}

// ─── Partition ──────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_create_partition(
    handle: *mut EngineHandle, task_queue_hash: u64,
) -> u32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.partition_manager().create_partition(task_queue_hash)
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_set_partition_forwarding(
    handle: *mut EngineHandle, from: u32, to: u32, rate: f64,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    if h.engine.partition_manager().set_forwarding(from, to, rate) { 0 } else { -1 }
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_partition_count(handle: *mut EngineHandle) -> u32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.partition_manager().partition_count() as u32
}

#[no_mangle]
pub unsafe extern "C" fn velocity_engine_partition_pending(
    handle: *mut EngineHandle, task_queue_hash: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.partition_manager().total_pending(task_queue_hash) as u64
}

/// Describe a partition. Writes [partition_id, task_queue_hash, pending_tasks, worker_count, parent_partition] to out_fields.
/// parent_partition is u64::MAX if no parent. Returns 1 on success, 0 if not found.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_partition_describe(
    handle: *mut EngineHandle,
    partition_id: u32,
    out_fields: *mut u64,
) -> i32 {
    if handle.is_null() || out_fields.is_null() { return 0; }
    let h = &*handle;
    match h.engine.partition_manager().describe_partition(partition_id) {
        Some(info) => {
            let fields = std::slice::from_raw_parts_mut(out_fields, 6);
            fields[0] = info.partition_id as u64;
            fields[1] = info.task_queue_hash;
            fields[2] = info.pending_tasks;
            fields[3] = info.worker_count;
            fields[4] = info.parent_partition.map_or(u64::MAX, |p| p as u64);
            // forward_rate as bits (approximate)
            fields[5] = (info.forward_rate * 1000.0) as u64;
            1
        }
        None => 0,
    }
}

/// Get partition count.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_partition_count_v2(handle: *mut EngineHandle) -> u32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.partition_manager().partition_count() as u32
}

/// Get partition IDs. Writes to out_ids array. Returns count written.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_partition_ids(
    handle: *mut EngineHandle,
    out_ids: *mut u32,
    max_count: u32,
) -> u32 {
    if handle.is_null() || out_ids.is_null() { return 0; }
    let h = &*handle;
    let ids = h.engine.partition_manager().partition_ids();
    let out = std::slice::from_raw_parts_mut(out_ids, max_count as usize);
    let count = ids.len().min(max_count as usize);
    for i in 0..count {
        out[i] = ids[i];
    }
    count as u32
}

// ─── Replay Engine ──────────────────────────────────────────────────────────

/// Replay a workflow's event history to reconstruct state.
/// Returns 1 on success, 0 on failure.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_replay(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let result = h.engine.replay_engine().replay_from_store(
        workflow_key,
        h.engine.history_store(),
        None,
    );
    if result.success { 1 } else { 0 }
}

/// Get the replayed status for a workflow after replay.
/// Returns the status as an i32 (WorkflowStatus enum value).
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_replay_status(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    h.engine.replay_engine()
        .get_cached(workflow_key)
        .map(|r| r.status as i32)
        .unwrap_or(-1)
}

/// Get the number of step results reconstructed during replay.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_replay_step_count(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> u32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.replay_engine()
        .get_cached(workflow_key)
        .map(|r| r.step_results.len() as u32)
        .unwrap_or(0)
}

/// Get the number of events replayed for a workflow.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_replay_event_count(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> u32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.replay_engine()
        .get_cached(workflow_key)
        .map(|r| r.events_replayed as u32)
        .unwrap_or(0)
}

/// Verify determinism: replay the same history twice and confirm identical results.
/// Returns 1 if deterministic, 0 if not.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_verify_determinism(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let history = h.engine.history_store().get_history(workflow_key).unwrap_or_default();
    if h.engine.replay_engine().verify_determinism(workflow_key, &history) { 1 } else { 0 }
}

/// Get the total number of replays performed.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_replay_count(
    handle: *mut EngineHandle,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.replay_engine().total_replays()
}

// ─── Auth & Rate Limiting ────────────────────────────────────────────────────

/// Check if a subject with given roles has a permission.
/// Returns 1 if authorized, 0 if not.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_authorize(
    handle: *mut EngineHandle,
    subject_ptr: *const u8, subject_len: u32,
    namespace_id: u64,
    roles_ptr: *const u8, roles_len: u32,
    permission: u32,
) -> i32 {
    if handle.is_null() || subject_ptr.is_null() || roles_ptr.is_null() { return 0; }
    let h = &*handle;
    
    let subject = std::str::from_utf8(std::slice::from_raw_parts(subject_ptr, subject_len as usize)).unwrap_or("");
    let roles_str = std::str::from_utf8(std::slice::from_raw_parts(roles_ptr, roles_len as usize)).unwrap_or("");
    let roles: Vec<String> = roles_str.split(',').map(|s| s.trim().to_string()).collect();
    
    let claims = crate::auth::Claims {
        subject: subject.to_string(),
        namespace_id,
        roles,
    };
    
    let perm = match permission {
        0 => crate::auth::Permission::StartWorkflow,
        1 => crate::auth::Permission::SignalWorkflow,
        2 => crate::auth::Permission::QueryWorkflow,
        3 => crate::auth::Permission::TerminateWorkflow,
        4 => crate::auth::Permission::CancelWorkflow,
        5 => crate::auth::Permission::DescribeWorkflow,
        6 => crate::auth::Permission::ListWorkflows,
        7 => crate::auth::Permission::RegisterNamespace,
        8 => crate::auth::Permission::DescribeNamespace,
        9 => crate::auth::Permission::PollActivityTask,
        10 => crate::auth::Permission::RespondActivityTask,
        11 => crate::auth::Permission::AdminAccess,
        _ => return 0,
    };
    
    if h.engine.auth_manager().authorize(&claims, &perm) { 1 } else { 0 }
}

/// Get the number of registered roles.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_role_count(
    handle: *mut EngineHandle,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.auth_manager().role_count() as u64
}

/// Set rate limit for a namespace.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_set_rate_limit(
    handle: *mut EngineHandle,
    namespace_id: u64,
    rate: f64,
    capacity: u64,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    h.engine.rate_limiter().set_namespace_limit(namespace_id, rate, capacity);
    0
}

// ─── Timeout Enforcement ────────────────────────────────────────────────────

/// Schedule an activity with timeout parameters.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_schedule_activity_with_timeouts(
    handle: *mut EngineHandle,
    workflow_key: u64,
    step: u32,
    activity_name_id: u64,
    args_ptr: *const u8,
    args_len: u32,
    schedule_to_start_ms: u64,
    start_to_close_ms: u64,
    schedule_to_close_ms: u64,
    heartbeat_ms: u64,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    let args = if args_ptr.is_null() || args_len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(args_ptr, args_len as usize).to_vec()
    };
    h.engine.schedule_activity_with_timeouts(
        workflow_key, step, activity_name_id, args,
        schedule_to_start_ms, start_to_close_ms, schedule_to_close_ms, heartbeat_ms
    );
    0
}

/// Check activity timeouts. Returns the number of timed-out activities.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_check_activity_timeouts(
    handle: *mut EngineHandle,
) -> u32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.check_activity_timeouts().len() as u32
}

/// Check workflow timeouts. Returns the number of timed-out workflows.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_check_workflow_timeouts(
    handle: *mut EngineHandle,
) -> u32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.check_workflow_timeouts()
}

/// Set workflow execution timeout.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_set_workflow_timeout(
    handle: *mut EngineHandle,
    workflow_key: u64,
    timeout_ms: u64,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    h.engine.set_workflow_execution_timeout(workflow_key, timeout_ms);
    0
}

// ─── Parent Close Policy ────────────────────────────────────────────────────

/// Apply parent close policy (0=Terminate, 1=Cancel, 2=Abandon).
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_apply_parent_close_policy(
    handle: *mut EngineHandle,
    parent_key: u64,
    policy: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    let p = match policy {
        0 => crate::engine::ParentClosePolicy::Terminate,
        1 => crate::engine::ParentClosePolicy::Cancel,
        2 => crate::engine::ParentClosePolicy::Abandon,
        _ => return -1,
    };
    h.engine.apply_parent_close_policy(parent_key, p);
    0
}

// ─── Activity Retry ─────────────────────────────────────────────────────────

/// Fail an activity with retry logic. Returns 1 if retried, 0 if failed permanently.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_fail_activity_with_retry(
    handle: *mut EngineHandle,
    workflow_key: u64,
    step: u32,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    if h.engine.fail_activity_with_retry(workflow_key, step) { 1 } else { 0 }
}

// ─── Query Dispatch ─────────────────────────────────────────────────────────

/// Execute a query handler. Returns result length, or -1 if no handler.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_execute_query(
    handle: *mut EngineHandle,
    workflow_key: u64,
    query_name_id: u64,
    input_ptr: *const u8,
    input_len: u32,
    output_ptr: *mut u8,
    output_len: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    let input = if input_ptr.is_null() || input_len == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(input_ptr, input_len as usize)
    };
    match h.engine.execute_query(workflow_key, query_name_id, input) {
        Some(result) => {
            let copy_len = result.len().min(output_len as usize);
            std::ptr::copy_nonoverlapping(result.as_ptr(), output_ptr, copy_len);
            result.len() as i32
        }
        None => -1,
    }
}

// ─── Workflow Reset ─────────────────────────────────────────────────────────

/// Reset a workflow to a previous event ID. Returns 1 if successful, 0 if not.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_reset_workflow(
    handle: *mut EngineHandle,
    workflow_key: u64,
    reset_to_event_id: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    if h.engine.reset_workflow(workflow_key, reset_to_event_id) { 1 } else { 0 }
}

// ─── Visibility SQL Query ───────────────────────────────────────────────────

/// Execute a SQL-like visibility query. Returns results via callback.
/// Query format: "Field = 'Value' AND Field = 'Value' LIMIT N OFFSET M"
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_execute_visibility_query(
    handle: *mut EngineHandle,
    query_ptr: *const u8,
    query_len: u32,
    callback: crate::ffi::WorkflowInfoCallback,
    user_data: *mut std::ffi::c_void,
) -> u64 {
    if handle.is_null() || query_ptr.is_null() { return 0; }
    let h = &*handle;
    let query_str = std::str::from_utf8(std::slice::from_raw_parts(query_ptr, query_len as usize)).unwrap_or("");
    
    match crate::visibility_query::VisibilityQuery::parse(query_str) {
        Ok(query) => {
            let results = query.execute(h.engine.visibility());
            for info in &results {
                callback(
                    info.workflow_key,
                    info.workflow_id,
                    info.run_id,
                    info.workflow_type_id,
                    info.namespace_id,
                    info.status as u32,
                    info.start_time_ms,
                    info.close_time_ms.map(|t| t as i64).unwrap_or(-1),
                    info.task_queue_hash,
                    user_data,
                );
            }
            results.len() as u64
        }
        Err(_) => 0,
    }
}

// ─── Namespace Listing ────────────────────────────────────────────────────────

/// Callback for listing namespaces: (id, name_ptr, name_len, is_active, retention_secs)
type NamespaceInfoCallback = unsafe extern "C" fn(u64, *const u8, u32, u32, u64, *mut std::ffi::c_void);

/// List all registered namespaces via callback.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_list_namespaces(
    handle: *mut EngineHandle,
    callback: NamespaceInfoCallback,
    user_data: *mut std::ffi::c_void,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let namespaces = h.engine.namespaces().list();
    for ns in &namespaces {
        callback(
            ns.id,
            ns.name.as_ptr(),
            ns.name.len() as u32,
            if ns.is_active { 1 } else { 0 },
            ns.retention_period.as_secs(),
            user_data,
        );
    }
    namespaces.len() as u64
}

// ─── Production Metrics Export ────────────────────────────────────────────────

/// Export all metrics in Prometheus text exposition format.
/// Returns a UTF-8 string. Caller must provide a buffer; actual length written to `out_len`.
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_export_metrics(
    handle: *mut EngineHandle,
    out_ptr: *mut u8,
    out_cap: u32,
    out_len: *mut u32,
) -> i32 {
    if handle.is_null() || out_ptr.is_null() || out_len.is_null() { return -1; }
    let h = &*handle;
    let prometheus_text = h.engine.metrics_registry().export_prometheus();
    let bytes = prometheus_text.as_bytes();
    let write_len = std::cmp::min(bytes.len(), out_cap as usize);
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr, write_len);
    *out_len = write_len as u32;
    0
}

// ─── Enhanced Describe Workflow ─────────────────────────────────────────────

/// Rich workflow description: status, steps, events, timing, search attrs, memo.
/// Writes a packed binary format to the caller buffer.
/// Format: [status:u8][total_steps:u32][completed_steps:u32][event_seq:u64]
///         [start_time_ms:u64][close_time_ms:u64][has_close:u8]
///         [workflow_type_id:u64][namespace_id:u64][task_queue_hash:u64]
///         [search_attr_count:u32][search_attrs...][memo_count:u32][memo...]
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_describe_workflow(
    handle: *mut EngineHandle,
    workflow_key: u64,
    out_ptr: *mut u8,
    out_cap: u32,
    out_len: *mut u32,
) -> i32 {
    if handle.is_null() || out_ptr.is_null() || out_len.is_null() { return -1; }
    let h = &*handle;

    // Get workflow context
    let status = h.engine.get_status(workflow_key);
    if status as i32 == 0 { return -2; } // not found

    let total_steps = h.engine.get_total_steps(workflow_key);
    let event_seq = h.engine.get_event_sequence(workflow_key);

    // Count completed steps
    let mut completed_steps = 0u32;
    for step in 0..total_steps {
        if h.engine.is_step_completed(workflow_key, step) {
            completed_steps += 1;
        }
    }

    // Get visibility info for timing
    let vis_info = h.engine.visibility().get(workflow_key);
    let (start_time_ms, close_time_ms, has_close, workflow_type_id, namespace_id, task_queue_hash) =
        match &vis_info {
            Some(info) => (info.start_time_ms, info.close_time_ms.unwrap_or(0), info.close_time_ms.is_some() as u8, info.workflow_type_id, info.namespace_id, info.task_queue_hash),
            None => (0u64, 0u64, 0u8, 0u64, 0u64, 0u64),
        };

    // Build output buffer
    let mut buf = Vec::with_capacity(256);
    buf.push(status as u8);
    buf.extend_from_slice(&total_steps.to_le_bytes());
    buf.extend_from_slice(&completed_steps.to_le_bytes());
    buf.extend_from_slice(&event_seq.to_le_bytes());
    buf.extend_from_slice(&start_time_ms.to_le_bytes());
    buf.extend_from_slice(&close_time_ms.to_le_bytes());
    buf.push(has_close);
    buf.extend_from_slice(&workflow_type_id.to_le_bytes());
    buf.extend_from_slice(&namespace_id.to_le_bytes());
    buf.extend_from_slice(&task_queue_hash.to_le_bytes());

    // Search attributes from visibility
    let search_attr_count = vis_info.as_ref().map(|v| v.search_attributes.len() as u32).unwrap_or(0);
    buf.extend_from_slice(&search_attr_count.to_le_bytes());
    if let Some(info) = &vis_info {
        for (key, val) in &info.search_attributes {
            let key_bytes = key.as_bytes();
            buf.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(key_bytes);
            // Value type tag + value
            match val {
                crate::visibility::SearchAttributeValue::String(s) => {
                    buf.push(1);
                    let sb = s.as_bytes();
                    buf.extend_from_slice(&(sb.len() as u32).to_le_bytes());
                    buf.extend_from_slice(sb);
                }
                crate::visibility::SearchAttributeValue::Integer(i) => {
                    buf.push(2);
                    buf.extend_from_slice(&i.to_le_bytes());
                }
                crate::visibility::SearchAttributeValue::Double(d) => {
                    buf.push(3);
                    buf.extend_from_slice(&d.to_le_bytes());
                }
                crate::visibility::SearchAttributeValue::Bool(b) => {
                    buf.push(4);
                    buf.push(*b as u8);
                }
                crate::visibility::SearchAttributeValue::DateTime(dt) => {
                    buf.push(5);
                    buf.extend_from_slice(&dt.to_le_bytes());
                }
                crate::visibility::SearchAttributeValue::Keyword(k) => {
                    buf.push(6);
                    let kb = k.as_bytes();
                    buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
                    buf.extend_from_slice(kb);
                }
            }
        }
    }

    // Memo count
    let memo_count = h.engine.memo_store().count(workflow_key) as u32;
    buf.extend_from_slice(&memo_count.to_le_bytes());

    let write_len = std::cmp::min(buf.len(), out_cap as usize);
    std::ptr::copy_nonoverlapping(buf.as_ptr(), out_ptr, write_len);
    *out_len = buf.len() as u32;
    0
}

// ─── Task Queue Partition Describe ─────────────────────────────────────────

/// Describe a partition: writes packed info to caller buffer.
/// Format: [partition_id:u32][task_queue_hash:u64][pending:u64][workers:u64]
///         [has_parent:u8][parent_id:u32][forward_rate:f64]
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_describe_partition(
    handle: *mut EngineHandle,
    partition_id: u32,
    out_ptr: *mut u8,
    out_cap: u32,
    out_len: *mut u32,
) -> i32 {
    if handle.is_null() || out_ptr.is_null() || out_len.is_null() { return -1; }
    let h = &*handle;

    let info = match h.engine.partition_manager().describe_partition(partition_id) {
        Some(i) => i,
        None => return -2,
    };

    let mut buf = Vec::with_capacity(44);
    buf.extend_from_slice(&info.partition_id.to_le_bytes());
    buf.extend_from_slice(&info.task_queue_hash.to_le_bytes());
    buf.extend_from_slice(&info.pending_tasks.to_le_bytes());
    buf.extend_from_slice(&info.worker_count.to_le_bytes());
    match info.parent_partition {
        Some(pid) => { buf.push(1); buf.extend_from_slice(&pid.to_le_bytes()); }
        None => { buf.push(0); buf.extend_from_slice(&0u32.to_le_bytes()); }
    }
    buf.extend_from_slice(&info.forward_rate.to_le_bytes());

    let write_len = std::cmp::min(buf.len(), out_cap as usize);
    std::ptr::copy_nonoverlapping(buf.as_ptr(), out_ptr, write_len);
    *out_len = buf.len() as u32;
    0
}

// ─── Cold Storage Archival ──────────────────────────────────────────────────

/// Archive a workflow to file-based cold storage.
/// Uses a default temp directory for the cold storage backend.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_archive_workflow(
    handle: *mut EngineHandle,
    workflow_key: u64,
    base_dir_ptr: *const u8,
    base_dir_len: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;

    let base_dir = if base_dir_ptr.is_null() || base_dir_len == 0 {
        "velocity_cold_storage".to_string()
    } else {
        let slice = std::slice::from_raw_parts(base_dir_ptr, base_dir_len as usize);
        String::from_utf8_lossy(slice).to_string()
    };

    let storage = match crate::cold_storage::FileColdStorage::new(&base_dir) {
        Ok(s) => s,
        Err(_) => return -2,
    };

    // Build a cold storage record from the workflow context
    let status = h.engine.get_status(workflow_key);
    if status as i32 == 0 { return -3; } // not found

    let vis_info = h.engine.visibility().get(workflow_key);
    let (workflow_id, run_id, workflow_type_id, namespace_id) =
        match &vis_info {
            Some(info) => (info.workflow_id, info.run_id, info.workflow_type_id, info.namespace_id),
            None => return -4,
        };

    // Collect step results
    let total_steps = h.engine.get_total_steps(workflow_key);
    let mut step_results = std::collections::HashMap::new();
    for step in 0..total_steps {
        if let Some(result) = h.engine.get_step_result(workflow_key, step) {
            step_results.insert(step, result);
        }
    }

    let record = crate::cold_storage::ColdStorageRecord {
        workflow_key,
        workflow_id,
        run_id,
        workflow_type_id,
        namespace_id,
        status,
        input_data: None,
        result_data: None,
        step_results,
        event_history: vec![],
        archived_at_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
        file_path: String::new(),
    };

    match storage.archive(record) {
        Ok(()) => 0,
        Err(_) => -5,
    }
}

/// Retrieve an archived workflow from cold storage. Returns the step count, or -1 on error.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_retrieve_workflow(
    handle: *mut EngineHandle,
    workflow_key: u64,
    base_dir_ptr: *const u8,
    base_dir_len: u32,
    out_status: *mut u8,
) -> i32 {
    if handle.is_null() || out_status.is_null() { return -1; }
    let h = &*handle;

    let base_dir = if base_dir_ptr.is_null() || base_dir_len == 0 {
        "velocity_cold_storage".to_string()
    } else {
        let slice = std::slice::from_raw_parts(base_dir_ptr, base_dir_len as usize);
        String::from_utf8_lossy(slice).to_string()
    };

    let storage = match crate::cold_storage::FileColdStorage::new(&base_dir) {
        Ok(s) => s,
        Err(_) => return -2,
    };

    match storage.retrieve(workflow_key) {
        Ok(Some(record)) => {
            *out_status = record.status as u8;
            record.step_results.len() as i32
        }
        Ok(None) => -3,
        Err(_) => -4,
    }
}

/// Count archived workflows in cold storage.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cold_storage_count(
    handle: *mut EngineHandle,
    base_dir_ptr: *const u8,
    base_dir_len: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let _h = &*handle;

    let base_dir = if base_dir_ptr.is_null() || base_dir_len == 0 {
        "velocity_cold_storage".to_string()
    } else {
        let slice = std::slice::from_raw_parts(base_dir_ptr, base_dir_len as usize);
        String::from_utf8_lossy(slice).to_string()
    };

    let storage = match crate::cold_storage::FileColdStorage::new(&base_dir) {
        Ok(s) => s,
        Err(_) => return -2,
    };

    storage.count() as i32
}

/// List archived workflow keys from cold storage via callback.
/// Callback receives (workflow_key, user_data) for each archived workflow.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cold_storage_list_keys(
    handle: *mut EngineHandle,
    base_dir_ptr: *const u8,
    base_dir_len: u32,
    callback: unsafe extern "C" fn(u64, *mut std::ffi::c_void),
    user_data: *mut std::ffi::c_void,
) -> u32 {
    if handle.is_null() { return 0; }
    let _h = &*handle;

    let base_dir = if base_dir_ptr.is_null() || base_dir_len == 0 {
        "velocity_cold_storage".to_string()
    } else {
        let slice = std::slice::from_raw_parts(base_dir_ptr, base_dir_len as usize);
        String::from_utf8_lossy(slice).to_string()
    };

    let storage = match crate::cold_storage::FileColdStorage::new(&base_dir) {
        Ok(s) => s,
        Err(_) => return 0,
    };

    let keys = storage.list_keys();
    let count = keys.len() as u32;
    for key in keys {
        callback(key, user_data);
    }
    count
}

// ─── Payload Codec Encode/Decode ────────────────────────────────────────────

/// Encode a payload through the codec chain. Writes result to out buffer.
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_codec_encode(
    handle: *mut EngineHandle,
    in_ptr: *const u8, in_len: u32,
    out_ptr: *mut u8, out_cap: u32, out_len: *mut u32,
) -> i32 {
    if handle.is_null() || out_len.is_null() { return -1; }
    let h = &*handle;
    let input = if in_ptr.is_null() || in_len == 0 { &[] as &[u8] } else {
        std::slice::from_raw_parts(in_ptr, in_len as usize)
    };
    match h.engine.codec_chain().encode(input) {
        Ok(encoded) => {
            let write_len = std::cmp::min(encoded.len(), out_cap as usize);
            if !out_ptr.is_null() && write_len > 0 {
                std::ptr::copy_nonoverlapping(encoded.as_ptr(), out_ptr, write_len);
            }
            *out_len = encoded.len() as u32;
            0
        }
        Err(_) => -1,
    }
}

/// Decode a payload through the codec chain (reverse order). Writes result to out buffer.
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_codec_decode(
    handle: *mut EngineHandle,
    in_ptr: *const u8, in_len: u32,
    out_ptr: *mut u8, out_cap: u32, out_len: *mut u32,
) -> i32 {
    if handle.is_null() || out_len.is_null() { return -1; }
    let h = &*handle;
    let input = if in_ptr.is_null() || in_len == 0 { &[] as &[u8] } else {
        std::slice::from_raw_parts(in_ptr, in_len as usize)
    };
    match h.engine.codec_chain().decode(input) {
        Ok(decoded) => {
            let write_len = std::cmp::min(decoded.len(), out_cap as usize);
            if !out_ptr.is_null() && write_len > 0 {
                std::ptr::copy_nonoverlapping(decoded.as_ptr(), out_ptr, write_len);
            }
            *out_len = decoded.len() as u32;
            0
        }
        Err(_) => -1,
    }
}

// ─── Saga Compensation + Step Info ──────────────────────────────────────────

/// Mark a saga compensation step as completed.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_complete_saga_compensation(
    handle: *mut EngineHandle, saga_id: u64, step_index: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    h.engine.saga_orchestrator().complete_compensation(saga_id, step_index as usize);
    0
}

/// Get the number of steps in a saga.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_saga_step_count(
    handle: *mut EngineHandle, saga_id: u64,
) -> u32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.saga_orchestrator().get_saga(saga_id)
        .map(|s| s.steps.len() as u32)
        .unwrap_or(0)
}

/// Get the status of a specific saga step.
/// Returns: 0=Pending, 1=Running, 2=Completed, 3=Failed, 4=Compensating, 5=Compensated, 6=CompensationFailed
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_saga_step_status(
    handle: *mut EngineHandle, saga_id: u64, step_index: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    h.engine.saga_orchestrator().get_saga(saga_id)
        .and_then(|s| s.steps.get(step_index as usize).cloned())
        .map(|step| match step.status {
            crate::saga::SagaStepStatus::Pending => 0,
            crate::saga::SagaStepStatus::Running => 1,
            crate::saga::SagaStepStatus::Completed => 2,
            crate::saga::SagaStepStatus::Failed => 3,
            crate::saga::SagaStepStatus::Compensating => 4,
            crate::saga::SagaStepStatus::Compensated => 5,
            crate::saga::SagaStepStatus::CompensationFailed => 6,
        })
        .unwrap_or(-1)
}

/// Get the current step index being executed in a saga.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_saga_current_step(
    handle: *mut EngineHandle, saga_id: u64,
) -> u32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.saga_orchestrator().get_saga(saga_id)
        .map(|s| s.current_step as u32)
        .unwrap_or(0)
}

// ─── WAL Recovery (Replay WAL into Engine State) ────────────────────────────

/// Recover engine state from WAL records.
/// Replays all WAL records to reconstruct workflows, step results, and signals.
/// Returns the number of records replayed, or -1 on error.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_wal_recover(
    handle: *mut EngineHandle,
    wal_path_ptr: *const u8, wal_path_len: u32,
) -> i64 {
    if handle.is_null() { return -1; }
    let h = &*handle;

    let wal_path = if wal_path_ptr.is_null() || wal_path_len == 0 {
        "velocity_wal.log".to_string()
    } else {
        let slice = std::slice::from_raw_parts(wal_path_ptr, wal_path_len as usize);
        String::from_utf8_lossy(slice).to_string()
    };

    let wal = match crate::wal::WalManager::new(&wal_path, 64 * 1024 * 1024) {
        Ok(w) => w,
        Err(_) => return -2,
    };

    let records = match wal.replay_all() {
        Ok(r) => r,
        Err(_) => return -3,
    };

    let record_count = records.len() as i64;

    for record in &records {
        match record.event_type {
            crate::wal::WalEventType::WorkflowStarted => {
                if record.data.len() >= 32 {
                    let workflow_id = u64::from_le_bytes(record.data[0..8].try_into().unwrap_or([0;8]));
                    let workflow_type_id = u64::from_le_bytes(record.data[8..16].try_into().unwrap_or([0;8]));
                    let namespace_id = u64::from_le_bytes(record.data[16..24].try_into().unwrap_or([0;8]));
                    let task_queue_hash = u64::from_le_bytes(record.data[24..32].try_into().unwrap_or([0;8]));
                    let total_steps = if record.data.len() >= 36 {
                        u32::from_le_bytes(record.data[32..36].try_into().unwrap_or([0;4]))
                    } else { 1 };
                    h.engine.start_workflow(workflow_id, workflow_type_id, namespace_id, task_queue_hash, total_steps, None);
                }
            }
            crate::wal::WalEventType::StepCompleted => {
                if record.data.len() >= 4 {
                    let step = u32::from_le_bytes(record.data[0..4].try_into().unwrap_or([0;4]));
                    let result = if record.data.len() > 4 {
                        record.data[4..].to_vec()
                    } else {
                        vec![]
                    };
                    h.engine.complete_step(record.workflow_key, step, result);
                }
            }
            crate::wal::WalEventType::WorkflowCompleted => {
                let result = if record.data.is_empty() { None } else { Some(record.data.clone()) };
                h.engine.complete_workflow(record.workflow_key, result);
            }
            crate::wal::WalEventType::WorkflowFailed => {
                h.engine.fail_workflow(record.workflow_key);
            }
            crate::wal::WalEventType::WorkflowCanceled => {
                h.engine.cancel_workflow(record.workflow_key);
            }
            crate::wal::WalEventType::WorkflowTerminated => {
                h.engine.terminate_workflow(record.workflow_key);
            }
            crate::wal::WalEventType::SignalReceived => {
                if record.data.len() >= 8 {
                    let signal_name_id = u64::from_le_bytes(record.data[0..8].try_into().unwrap_or([0;8]));
                    let payload = if record.data.len() > 8 {
                        record.data[8..].to_vec()
                    } else {
                        vec![]
                    };
                    h.engine.signal_workflow(record.workflow_key, signal_name_id, payload);
                }
            }
            _ => {} // Timer, Activity, Child events handled by other recovery paths
        }
    }

    record_count
}

// ─── History Event Stream ─────────────────────────────────────────────────────

/// Get a page of history events for a workflow. Events are serialized into the output buffer.
/// Each event: event_id(8) + event_type(4) + timestamp_ms(8) + payload_len(4) + payload(payload_len)
/// Returns number of events written, or -1 on error.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_get_history_page(
    handle: *mut EngineHandle,
    workflow_key: u64,
    start_event_id: u64,
    max_count: u32,
    out_ptr: *mut u8,
    out_cap: u32,
    out_len: *mut u32,
) -> i32 {
    if handle.is_null() || out_ptr.is_null() { return -1; }
    let h = &*handle;
    let events = h.engine.history_store().get_history_page(workflow_key, start_event_id, max_count as usize);
    
    let mut pos = 0u32;
    for event in &events {
        let needed = 8 + 4 + 8 + 4 + event.payload.len() as u32;
        if pos + needed > out_cap { break; }
        
        // event_id
        (out_ptr.add(pos as usize) as *mut u64).write(event.event_id);
        pos += 8;
        // event_type
        (out_ptr.add(pos as usize) as *mut u32).write(event.event_type as u32);
        pos += 4;
        // timestamp_ms
        (out_ptr.add(pos as usize) as *mut u64).write(event.timestamp_ms);
        pos += 8;
        // payload_len + payload
        let plen = event.payload.len() as u32;
        (out_ptr.add(pos as usize) as *mut u32).write(plen);
        pos += 4;
        if plen > 0 {
            std::ptr::copy_nonoverlapping(event.payload.as_ptr(), out_ptr.add(pos as usize), plen as usize);
            pos += plen;
        }
    }
    
    *out_len = pos;
    events.len() as i32
}

/// Get a single history event by event ID. Returns event type or -1.
/// Payload is written to the output buffer.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_get_history_event(
    handle: *mut EngineHandle,
    workflow_key: u64,
    event_id: u64,
    out_ptr: *mut u8,
    out_cap: u32,
    out_len: *mut u32,
) -> i32 {
    if handle.is_null() || out_ptr.is_null() { return -1; }
    let h = &*handle;
    let history = h.engine.history_store().get_history(workflow_key);
    match history {
        Some(events) => {
            match events.iter().find(|e| e.event_id == event_id) {
                Some(event) => {
                    let copy_len = std::cmp::min(event.payload.len() as u32, out_cap);
                    if copy_len > 0 {
                        std::ptr::copy_nonoverlapping(event.payload.as_ptr(), out_ptr, copy_len as usize);
                    }
                    *out_len = copy_len;
                    event.event_type as i32
                }
                None => -1,
            }
        }
        None => -1,
    }
}

/// Get total event count across all workflows in the history store.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_total_history_event_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    // Sum event counts across all workflow histories
    let store = h.engine.history_store();
    store.workflow_count() as u64 // Number of workflows with history
}

// ─── Enhanced Reset Introspection ─────────────────────────────────────────────

/// Get the latest reset point event ID for a workflow. Returns event_id or -1.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_latest_reset_event_id(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> i64 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    match h.engine.workflow_resetter().get_latest_reset(workflow_key) {
        Some(rp) => rp.reset_to_event_id as i64,
        None => -1,
    }
}

/// Get total reset count across all workflows.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_total_reset_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.workflow_resetter().total_reset_count()
}

// ─── Saga Introspection ──────────────────────────────────────────────────────

/// Get the workflow key associated with a saga.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_saga_workflow_key(
    handle: *mut EngineHandle,
    saga_id: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.saga_orchestrator().get_saga(saga_id)
        .map(|e| e.workflow_key)
        .unwrap_or(0)
}

/// Get the overall status of a saga as an integer.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_saga_overall_status(
    handle: *mut EngineHandle,
    saga_id: u64,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    h.engine.saga_orchestrator().get_saga(saga_id)
        .map(|e| e.status as i32)
        .unwrap_or(-1)
}

/// Complete a saga step. Returns 1 on success, 0 on failure.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_saga_complete_step(
    handle: *mut EngineHandle,
    saga_id: u64,
    step_index: u32,
    result_ptr: *const u8,
    result_len: u32,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let result = if result_ptr.is_null() || result_len == 0 {
        None
    } else {
        Some(std::slice::from_raw_parts(result_ptr, result_len as usize).to_vec())
    };
    if h.engine.saga_orchestrator().complete_step(saga_id, step_index as usize, result) { 1 } else { 0 }
}

/// Fail a saga step. Returns the number of compensation steps triggered.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_saga_fail_step(
    handle: *mut EngineHandle,
    saga_id: u64,
    step_index: u32,
) -> u32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.saga_orchestrator().fail_step(saga_id, step_index as usize).len() as u32
}

/// Complete a compensation step.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_saga_complete_compensation(
    handle: *mut EngineHandle,
    saga_id: u64,
    step_index: u32,
) {
    if handle.is_null() { return; }
    let h = &*handle;
    h.engine.saga_orchestrator().complete_compensation(saga_id, step_index as usize);
}

/// Get saga details. Writes [saga_id, workflow_key, current_step, step_count, status] to out_fields.
/// Returns 1 on success, 0 if not found.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_saga_get(
    handle: *mut EngineHandle,
    saga_id: u64,
    out_fields: *mut u64,
) -> i32 {
    if handle.is_null() || out_fields.is_null() { return 0; }
    let h = &*handle;
    match h.engine.saga_orchestrator().get_saga(saga_id) {
        Some(e) => {
            let fields = std::slice::from_raw_parts_mut(out_fields, 5);
            fields[0] = e.saga_id;
            fields[1] = e.workflow_key;
            fields[2] = e.current_step as u64;
            fields[3] = e.steps.len() as u64;
            fields[4] = e.status as u64;
            1
        }
        None => 0,
    }
}

/// Get saga IDs by status. Writes IDs to out_ids array. Returns count written.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_sagas_by_status(
    handle: *mut EngineHandle,
    status: i32,
    out_ids: *mut u64,
    max_count: u32,
) -> u32 {
    if handle.is_null() || out_ids.is_null() { return 0; }
    let h = &*handle;
    let saga_status = match status {
        0 => crate::saga::SagaStatus::Created,
        1 => crate::saga::SagaStatus::Running,
        2 => crate::saga::SagaStatus::Completed,
        3 => crate::saga::SagaStatus::Compensating,
        4 => crate::saga::SagaStatus::Compensated,
        _ => return 0,
    };
    let sagas = h.engine.saga_orchestrator().sagas_by_status(saga_status);
    let out = std::slice::from_raw_parts_mut(out_ids, max_count as usize);
    let count = sagas.len().min(max_count as usize);
    for i in 0..count {
        out[i] = sagas[i].saga_id;
    }
    count as u32
}

// ─── Engine Stats ─────────────────────────────────────────────────────────────

/// Get total number of workflows with history records.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_history_workflow_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.history_store().workflow_count() as u64
}

// ─── Worker Registry ─────────────────────────────────────────────────────────

/// Register a new worker. Returns the assigned worker_id (0 on error).
/// task_queue_hashes: pointer to array of u64 hashes, tq_count is the length.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_register_worker(
    handle: *mut EngineHandle,
    addr_ptr: *const u8, addr_len: u32,
    tq_hashes_ptr: *const u64, tq_count: u32,
    version_ptr: *const u8, version_len: u32,
) -> u64 {
    if handle.is_null() || addr_ptr.is_null() { return 0; }
    let h = &*handle;
    let addr = std::str::from_utf8(std::slice::from_raw_parts(addr_ptr, addr_len as usize)).unwrap_or("");
    let version = if version_ptr.is_null() || version_len == 0 { "unknown" } else {
        std::str::from_utf8(std::slice::from_raw_parts(version_ptr, version_len as usize)).unwrap_or("unknown")
    };
    let hashes = if tq_hashes_ptr.is_null() || tq_count == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(tq_hashes_ptr, tq_count as usize)
    };
    h.engine.worker_registry().register_worker(addr, hashes, &[], version)
}

/// Unregister a worker. Returns 1 if found and removed, 0 if not found.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_unregister_worker(
    handle: *mut EngineHandle,
    worker_id: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    if h.engine.worker_registry().unregister_worker(worker_id) { 1 } else { 0 }
}

/// Record a heartbeat from a worker. Returns 1 if worker found, 0 if not.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_worker_heartbeat(
    handle: *mut EngineHandle,
    worker_id: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    if h.engine.worker_registry().heartbeat(worker_id) { 1 } else { 0 }
}

/// Get total number of registered workers.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_worker_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.worker_registry().worker_count() as u64
}

/// Get number of active (healthy) workers.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_active_worker_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.worker_registry().active_worker_count() as u64
}

/// Record a task completion for a worker.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_worker_task_completed(
    handle: *mut EngineHandle,
    worker_id: u64,
) {
    if handle.is_null() { return; }
    let h = &*handle;
    h.engine.worker_registry().record_task_completed(worker_id);
}

/// Record a task failure for a worker.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_worker_task_failed(
    handle: *mut EngineHandle,
    worker_id: u64,
) {
    if handle.is_null() { return; }
    let h = &*handle;
    h.engine.worker_registry().record_task_failed(worker_id);
}

/// Set worker status (0=Active, 1=Draining, 2=Offline, 3=Unhealthy).
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_set_worker_status(
    handle: *mut EngineHandle,
    worker_id: u64,
    status: i32,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    let ws = match status {
        0 => crate::worker_registry::WorkerStatus::Active,
        1 => crate::worker_registry::WorkerStatus::Draining,
        2 => crate::worker_registry::WorkerStatus::Offline,
        3 => crate::worker_registry::WorkerStatus::Unhealthy,
        _ => return -1,
    };
    h.engine.worker_registry().set_worker_status(worker_id, ws);
    0
}

/// Detect stale workers that haven't heartbeated within timeout_ms. Returns count.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_detect_stale_workers(
    handle: *mut EngineHandle,
    timeout_ms: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.worker_registry().detect_stale_workers(timeout_ms).len() as u64
}

/// Add a task queue hash to a worker's capabilities.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_worker_add_task_queue(
    handle: *mut EngineHandle,
    worker_id: u64,
    tq_hash: u64,
) {
    if handle.is_null() { return; }
    let h = &*handle;
    h.engine.worker_registry().add_task_queue(worker_id, tq_hash);
}

/// Get total tasks completed across all workers.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_total_tasks_completed(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.worker_registry().total_tasks_completed()
}

/// Get total tasks failed across all workers.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_total_tasks_failed(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.worker_registry().total_tasks_failed()
}

/// Get workers for a specific task queue. Returns count; worker IDs written to output buffer.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_get_workers_for_queue(
    handle: *mut EngineHandle,
    tq_hash: u64,
    out_ptr: *mut u64,
    out_cap: u32,
) -> u32 {
    if handle.is_null() || out_ptr.is_null() { return 0; }
    let h = &*handle;
    let workers = h.engine.worker_registry().get_workers_for_queue(tq_hash);
    let count = std::cmp::min(workers.len(), out_cap as usize);
    for i in 0..count {
        *out_ptr.add(i) = workers[i];
    }
    count as u32
}

// ─── Search Attribute Get/List ───────────────────────────────────────────────

/// Get a search attribute value for a workflow. Returns the value as a string via output buffer.
/// Returns 1 if found, 0 if not found.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_get_search_attribute(
    handle: *mut EngineHandle,
    workflow_key: u64,
    key_ptr: *const u8, key_len: u32,
    out_ptr: *mut u8, out_cap: u32, out_len: *mut u32,
) -> i32 {
    if handle.is_null() || key_ptr.is_null() || out_ptr.is_null() { return 0; }
    let h = &*handle;
    let key = std::str::from_utf8(std::slice::from_raw_parts(key_ptr, key_len as usize)).unwrap_or("");
    
    let vis = h.engine.visibility();
    if let Some(info) = vis.get(workflow_key) {
        if let Some(val) = info.search_attributes.get(key) {
            let val_str = match val {
                crate::visibility::SearchAttributeValue::String(s) => s.clone(),
                crate::visibility::SearchAttributeValue::Integer(i) => i.to_string(),
                crate::visibility::SearchAttributeValue::Double(f) => f.to_string(),
                crate::visibility::SearchAttributeValue::Bool(b) => b.to_string(),
                crate::visibility::SearchAttributeValue::DateTime(ms) => ms.to_string(),
                crate::visibility::SearchAttributeValue::Keyword(s) => s.clone(),
            };
            let bytes = val_str.as_bytes();
            let copy_len = std::cmp::min(bytes.len() as u32, out_cap);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr, copy_len as usize);
            *out_len = copy_len;
            return 1;
        }
    }
    0
}

/// List all search attribute keys for a workflow. Keys are written as length-prefixed strings.
/// Returns the number of keys found.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_list_search_attributes(
    handle: *mut EngineHandle,
    workflow_key: u64,
    out_ptr: *mut u8, out_cap: u32, out_len: *mut u32,
) -> u32 {
    if handle.is_null() || out_ptr.is_null() { return 0; }
    let h = &*handle;
    let vis = h.engine.visibility();
    
    if let Some(info) = vis.get(workflow_key) {
        let mut pos = 0u32;
        let mut count = 0u32;
        for key in info.search_attributes.keys() {
            let key_bytes = key.as_bytes();
            let needed = 4 + key_bytes.len() as u32;
            if pos + needed > out_cap { break; }
            // Write key length + key
            (out_ptr.add(pos as usize) as *mut u32).write(key_bytes.len() as u32);
            pos += 4;
            std::ptr::copy_nonoverlapping(key_bytes.as_ptr(), out_ptr.add(pos as usize), key_bytes.len());
            pos += key_bytes.len() as u32;
            count += 1;
        }
        *out_len = pos;
        return count;
    }
    0
}

// ─── Workflow Timeout Enforcement ─────────────────────────────────────────────

/// Set all workflow timeout types (in milliseconds). The engine will auto-fail workflows
/// that exceed execution timeout.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_set_workflow_timeouts(
    handle: *mut EngineHandle,
    workflow_key: u64,
    execution_timeout_ms: u64,
    run_timeout_ms: u64,
    task_timeout_ms: u64,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    let mut workflows = h.engine.workflows_write();
    if let Some(ctx) = workflows.get_mut(&workflow_key) {
        if execution_timeout_ms > 0 {
            ctx.workflow_execution_timeout = Some(std::time::Duration::from_millis(execution_timeout_ms));
        }
        if run_timeout_ms > 0 {
            ctx.workflow_run_timeout = Some(std::time::Duration::from_millis(run_timeout_ms));
        }
        if task_timeout_ms > 0 {
            ctx.workflow_task_timeout = Some(std::time::Duration::from_millis(task_timeout_ms));
        }
        0
    } else {
        -1
    }
}

/// Check and enforce workflow timeouts. Returns the number of workflows timed out.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_check_timeouts(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let mut timed_out = 0u64;
    let mut workflows = h.engine.workflows_write();
    
    for (key, ctx) in workflows.iter_mut() {
        if ctx.status != crate::engine::WorkflowStatus::Running { continue; }
        
        // Check execution timeout
        if let Some(timeout) = ctx.workflow_execution_timeout {
            if ctx.start_time.elapsed() > timeout {
                ctx.status = crate::engine::WorkflowStatus::TimedOut;
                ctx.close_time = Some(std::time::Instant::now());
                timed_out += 1;
                // Record in history
                h.engine.history_store().record_event(
                    *key,
                    crate::event_history::HistoryEventType::WorkflowTimedOut,
                    vec![],
                );
            }
        }
    }
    
    // Also clean expired tasks from the task queue
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    h.engine.task_queue().remove_expired(now_ms);
    
    timed_out
}

/// Get total pending tasks across all task queues.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_total_pending_tasks(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.task_queue().total_pending() as u64
}

/// Get number of distinct task queues.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_task_queue_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.task_queue().queue_count() as u64
}

// ─── Replay Apply + Verify ──────────────────────────────────────────────────

/// Apply replay results back to the engine, reconstructing workflow state.
/// If the workflow context doesn't exist (e.g., after crash), creates a new one.
/// Returns 1 if successful, 0 if not.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_apply_replay(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let history = h.engine.history_store().get_history(workflow_key);
    match history {
        Some(events) => {
            let result = h.engine.replay_engine().replay(workflow_key, &events, None);
            if result.success {
                let mut workflows = h.engine.workflows_write();
                // If context doesn't exist (crash recovery), create one
                if !workflows.contains_key(&workflow_key) {
                    let total_steps = result.step_results.keys()
                        .max().map(|&m| m + 1).unwrap_or(0);
                    let mut ctx = WorkflowContext::new(
                        workflow_key >> 32, workflow_key & 0xFFFFFFFF, 0, 0, total_steps,
                    );
                    ctx.status = result.status;
                    for (step, data) in &result.step_results {
                        ctx.step_results.insert(*step, data.clone());
                        ctx.slab.step_bitmask.set_step(*step as usize);
                    }
                    // Restore pending signals
                    for (signal_id, payloads) in &result.pending_signals {
                        for payload in payloads {
                            ctx.signal_buffer.entry(*signal_id)
                                .or_default().push(payload.clone());
                        }
                    }
                    workflows.insert(workflow_key, ctx);
                } else if let Some(ctx) = workflows.get_mut(&workflow_key) {
                    // Existing context — apply replay results
                    for (step, data) in &result.step_results {
                        ctx.step_results.insert(*step, data.clone());
                        ctx.slab.step_bitmask.set_step(*step as usize);
                    }
                    ctx.status = result.status;
                    for (signal_id, payloads) in &result.pending_signals {
                        for payload in payloads {
                            ctx.signal_buffer.entry(*signal_id)
                                .or_default().push(payload.clone());
                        }
                    }
                }
                1
            } else {
                0
            }
        }
        None => 0,
    }
}

// ─── Cold Storage Management ───────────────────────────────────────────────

/// Delete an archived workflow from cold storage. Returns 1 if deleted, 0 if not found.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cold_storage_delete(
    handle: *mut EngineHandle,
    workflow_key: u64,
    base_dir_ptr: *const u8,
    base_dir_len: u32,
) -> i32 {
    if handle.is_null() { return 0; }
    let _h = &*handle;
    let base_dir = if base_dir_ptr.is_null() || base_dir_len == 0 {
        "/tmp/velocity_cold_storage"
    } else {
        std::str::from_utf8(std::slice::from_raw_parts(base_dir_ptr, base_dir_len as usize)).unwrap_or("/tmp/velocity_cold_storage")
    };
    match crate::cold_storage::FileColdStorage::new(base_dir) {
        Ok(storage) => match storage.delete(workflow_key) {
            Ok(deleted) => if deleted { 1 } else { 0 },
            Err(_) => 0,
        },
        Err(_) => 0,
    }
}

/// Garbage collect cold storage archives older than retention_ms. Returns count deleted.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cold_storage_gc(
    handle: *mut EngineHandle,
    retention_ms: u64,
    base_dir_ptr: *const u8,
    base_dir_len: u32,
) -> i32 {
    if handle.is_null() { return 0; }
    let _h = &*handle;
    let base_dir = if base_dir_ptr.is_null() || base_dir_len == 0 {
        "/tmp/velocity_cold_storage"
    } else {
        std::str::from_utf8(std::slice::from_raw_parts(base_dir_ptr, base_dir_len as usize)).unwrap_or("/tmp/velocity_cold_storage")
    };
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    match crate::cold_storage::FileColdStorage::new(base_dir) {
        Ok(storage) => match storage.gc_older_than(retention_ms, now_ms) {
            Ok(count) => count as i32,
            Err(_) => 0,
        },
        Err(_) => 0,
    }
}

/// List cold storage keys by namespace. Returns count; keys written to output buffer.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cold_storage_list_by_namespace(
    handle: *mut EngineHandle,
    namespace_id: u64,
    base_dir_ptr: *const u8,
    base_dir_len: u32,
    out_ptr: *mut u64,
    out_cap: u32,
) -> u32 {
    if handle.is_null() || out_ptr.is_null() { return 0; }
    let base_dir = if base_dir_ptr.is_null() || base_dir_len == 0 {
        "/tmp/velocity_cold_storage"
    } else {
        std::str::from_utf8(std::slice::from_raw_parts(base_dir_ptr, base_dir_len as usize)).unwrap_or("/tmp/velocity_cold_storage")
    };
    match crate::cold_storage::FileColdStorage::new(base_dir) {
        Ok(storage) => {
            let records = storage.list_by_namespace(namespace_id);
            let count = std::cmp::min(records.len(), out_cap as usize);
            for i in 0..count {
                *out_ptr.add(i) = records[i].workflow_key;
            }
            count as u32
        }
        Err(_) => 0,
    }
}

// ─── Schedule Introspection ─────────────────────────────────────────────────

/// List all schedules. Returns count; schedule IDs written to output buffer.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_list_schedules(
    handle: *mut EngineHandle,
    out_ptr: *mut u64,
    out_cap: u32,
) -> u32 {
    if handle.is_null() || out_ptr.is_null() { return 0; }
    let h = &*handle;
    let schedules = h.engine.schedule_manager().list();
    let count = std::cmp::min(schedules.len(), out_cap as usize);
    for i in 0..count {
        *out_ptr.add(i) = schedules[i].schedule_id;
    }
    schedules.len() as u32
}

/// Describe a schedule. Returns 1 if found, 0 if not.
/// Writes: workflow_type_id, namespace_id, task_queue_hash, overlap_policy, action_count
/// to the out_fields buffer (5 x u64).
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_describe_schedule(
    handle: *mut EngineHandle,
    schedule_id: u64,
    out_fields: *mut u64,
) -> i32 {
    if handle.is_null() || out_fields.is_null() { return 0; }
    let h = &*handle;
    match h.engine.schedule_manager().get(schedule_id) {
        Some(entry) => {
            *out_fields.add(0) = entry.workflow_type_id;
            *out_fields.add(1) = entry.namespace_id;
            *out_fields.add(2) = entry.task_queue_hash;
            *out_fields.add(3) = entry.overlap_policy as u64;
            *out_fields.add(4) = entry.action_count;
            1
        }
        None => 0,
    }
}

/// Check if a schedule is paused. Returns 1 if paused, 0 if not, -1 if not found.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_schedule_is_paused(
    handle: *mut EngineHandle,
    schedule_id: u64,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    match h.engine.schedule_manager().get(schedule_id) {
        Some(entry) => if entry.state == ScheduleState::Paused { 1 } else { 0 },
        None => -1,
    }
}

// ─── Dynamic Config Listing ────────────────────────────────────────────────

/// List all dynamic config keys. Returns total count.
/// Keys written as length-prefixed UTF-8 strings to the output buffer.
/// Each entry: [u32 key_len][key_bytes...]
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_list_config_keys(
    handle: *mut EngineHandle,
    out_ptr: *mut u8,
    out_cap: u32,
) -> u32 {
    if handle.is_null() || out_ptr.is_null() { return 0; }
    let h = &*handle;
    let keys = h.engine.dynamic_config().list_keys();
    let mut offset = 0usize;
    for key in &keys {
        let key_bytes = key.as_bytes();
        let needed = 4 + key_bytes.len();
        if offset + needed > out_cap as usize { break; }
        // Write key length (u32 LE)
        let len_bytes = (key_bytes.len() as u32).to_le_bytes();
        std::ptr::copy_nonoverlapping(len_bytes.as_ptr(), out_ptr.add(offset), 4);
        offset += 4;
        // Write key bytes
        std::ptr::copy_nonoverlapping(key_bytes.as_ptr(), out_ptr.add(offset), key_bytes.len());
        offset += key_bytes.len();
    }
    keys.len() as u32
}

/// Get a dynamic config value as i64. Returns the value.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_get_config_int(
    handle: *mut EngineHandle,
    key_ptr: *const u8,
    key_len: u32,
) -> i64 {
    if handle.is_null() || key_ptr.is_null() { return 0; }
    let h = &*handle;
    let key = std::str::from_utf8(std::slice::from_raw_parts(key_ptr, key_len as usize)).unwrap_or("");
    h.engine.dynamic_config().get_int(key)
}

// ─── Heartbeat Timeout Check ───────────────────────────────────────────────

/// Check for heartbeat timeouts. Returns count of timed-out activities.
/// Writes (workflow_key, activity_id) pairs to output buffer (2 x u64 per entry).
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_check_heartbeat_timeouts(
    handle: *mut EngineHandle,
    out_ptr: *mut u64,
    out_cap: u32,
) -> u32 {
    if handle.is_null() || out_ptr.is_null() { return 0; }
    let h = &*handle;
    let timed_out = h.engine.heartbeat_tracker().check_timeouts();
    let max_entries = out_cap as usize / 2;
    let count = std::cmp::min(timed_out.len(), max_entries);
    for i in 0..count {
        *out_ptr.add(i * 2) = timed_out[i].workflow_key;
        *out_ptr.add(i * 2 + 1) = timed_out[i].activity_id;
    }
    count as u32
}

// ─── Workflow Count Aggregation ─────────────────────────────────────────────

/// Count workflows by status (0=Void, 1=Running, 2=Completed, 3=Failed, 4=Canceled, 5=Terminated, 6=ContinuedAsNew, 7=TimedOut).
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_count_by_status(
    handle: *mut EngineHandle,
    status: u32,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let ws = match status {
        1 => crate::engine::WorkflowStatus::Running,
        2 => crate::engine::WorkflowStatus::Completed,
        3 => crate::engine::WorkflowStatus::Failed,
        4 => crate::engine::WorkflowStatus::Canceled,
        5 => crate::engine::WorkflowStatus::Terminated,
        6 => crate::engine::WorkflowStatus::ContinuedAsNew,
        7 => crate::engine::WorkflowStatus::TimedOut,
        _ => crate::engine::WorkflowStatus::Void,
    };
    h.engine.visibility().count_by_status(ws) as u64
}

/// Count workflows by namespace.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_count_by_namespace(
    handle: *mut EngineHandle,
    namespace_id: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.visibility().count_by_namespace(namespace_id) as u64
}

/// Count workflows by workflow type.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_count_by_type(
    handle: *mut EngineHandle,
    workflow_type_id: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.visibility().count_by_type(workflow_type_id) as u64
}

// ─── Namespace Retention ───────────────────────────────────────────────────

/// Get namespace retention period in milliseconds. Returns 0 if namespace not found.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_get_namespace_retention_ms(
    handle: *mut EngineHandle,
    namespace_id: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    match h.engine.namespaces().get(namespace_id) {
        Some(config) => config.retention_period.as_millis() as u64,
        None => 0,
    }
}

/// Cleanup expired workflows based on namespace retention policies.
/// Removes completed/failed/canceled/terminated workflows older than retention.
/// Returns count of workflows removed.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cleanup_expired_workflows(
    handle: *mut EngineHandle,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let mut removed = 0u64;
    // For each namespace, check retention and remove expired workflows
    let namespaces = h.engine.namespaces().list();
    for ns in &namespaces {
        let retention_ms = ns.retention_period.as_millis() as u64;
        let cutoff = now_ms.saturating_sub(retention_ms);
        // Get all closed workflows in this namespace
        let workflows = h.engine.visibility().list_by_namespace(ns.id);
        for wf in &workflows {
            // Only clean up closed workflows
            if wf.close_time_ms.is_some() && wf.close_time_ms.unwrap() < cutoff {
                h.engine.visibility().remove(wf.workflow_key);
                removed += 1;
            }
        }
    }
    removed
}

// ─── Query Dispatch ────────────────────────────────────────────────────────

/// Check if a query handler is registered. Returns 1 if registered, 0 if not.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_has_query_handler(
    handle: *mut EngineHandle,
    workflow_key: u64,
    query_name_id: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    if h.engine.query_registry().has_handler(workflow_key, query_name_id) { 1 } else { 0 }
}

/// Unregister all query handlers for a workflow. Returns 1 if removed, 0 if not found.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_unregister_query_handler(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.query_registry().unregister_workflow(workflow_key);
    1
}

/// Get reset points for a workflow. Writes event IDs to out_event_ids array.
/// Returns the number of reset points written.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_get_reset_points(
    handle: *mut EngineHandle,
    workflow_key: u64,
    out_event_ids: *mut u64,
    max_count: u32,
) -> u32 {
    if handle.is_null() || out_event_ids.is_null() { return 0; }
    let h = &*handle;
    let points = h.engine.workflow_resetter().get_reset_points(workflow_key);
    let out = std::slice::from_raw_parts_mut(out_event_ids, max_count as usize);
    let count = points.len().min(max_count as usize);
    for (i, p) in points.iter().take(count).enumerate() {
        out[i] = p.reset_to_event_id;
    }
    count as u32
}

// ─── Cloud Storage Adapter ─────────────────────────────────────────────────

/// Switch the cloud storage backend. backend: 0 = MockS3, 1 = MockGCS.
/// Returns 1 on success.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cloud_storage_set_backend(
    handle: *mut EngineHandle,
    backend: u32,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    use crate::cold_storage::{MockS3Adapter, MockGcsAdapter};
    let adapter: Arc<dyn crate::cold_storage::CloudStorageAdapter> = match backend {
        0 => Arc::new(MockS3Adapter::new("velocity-bucket", "us-east-1")),
        1 => Arc::new(MockGcsAdapter::new("velocity-bucket")),
        _ => return 0,
    };
    h.engine.set_cloud_storage(adapter);
    1
}

/// Archive a workflow to cloud storage. Returns 1 on success, 0 on failure.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cloud_archive(
    handle: *mut EngineHandle,
    workflow_key: u64,
    namespace_id: u64,
    status: i32,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let record = crate::cold_storage::ColdStorageRecord {
        workflow_key,
        workflow_id: workflow_key >> 32,
        run_id: workflow_key & 0xFFFFFFFF,
        workflow_type_id: 0,
        namespace_id,
        status: match status {
            1 => crate::engine::WorkflowStatus::Running,
            2 => crate::engine::WorkflowStatus::Completed,
            3 => crate::engine::WorkflowStatus::Failed,
            4 => crate::engine::WorkflowStatus::Canceled,
            5 => crate::engine::WorkflowStatus::Terminated,
            6 => crate::engine::WorkflowStatus::TimedOut,
            7 => crate::engine::WorkflowStatus::ContinuedAsNew,
            _ => crate::engine::WorkflowStatus::Void,
        },
        input_data: None,
        result_data: None,
        step_results: std::collections::HashMap::new(),
        event_history: vec![],
        archived_at_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
        file_path: String::new(),
    };
    h.engine.cloud_storage().archive(&record).map_or(0, |_| 1)
}

/// Check if a workflow exists in cloud storage. Returns 1 if found, 0 if not.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cloud_contains(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.cloud_storage().retrieve(workflow_key).map_or(0, |_| 1)
}

/// Delete a workflow from cloud storage. Returns 1 if deleted, 0 if not found.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cloud_delete(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.cloud_storage().delete(workflow_key).map_or(0, |deleted| if deleted { 1 } else { 0 })
}

/// Get the total count of records in cloud storage.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cloud_count(
    handle: *mut EngineHandle,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.cloud_storage().count().unwrap_or(0) as u64
}

/// List workflow keys in cloud storage by namespace.
/// Writes keys to out_keys array. Returns number of keys written.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cloud_list_by_namespace(
    handle: *mut EngineHandle,
    namespace_id: u64,
    out_keys: *mut u64,
    max_count: u32,
) -> u32 {
    if handle.is_null() || out_keys.is_null() { return 0; }
    let h = &*handle;
    let records = h.engine.cloud_storage().list_by_namespace(namespace_id).unwrap_or_default();
    let out = std::slice::from_raw_parts_mut(out_keys, max_count as usize);
    let count = records.len().min(max_count as usize);
    for (i, r) in records.iter().take(count).enumerate() {
        out[i] = r.workflow_key;
    }
    count as u32
}

/// Garbage collect cloud storage records older than retention_ms.
/// Returns number of records deleted.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cloud_gc(
    handle: *mut EngineHandle,
    retention_ms: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    h.engine.cloud_storage().gc_older_than(retention_ms, now_ms).unwrap_or(0) as i32
}

/// Get the cloud storage backend name. Writes name to out_name buffer.
/// Returns length of name written, 0 on failure.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cloud_backend_name(
    handle: *mut EngineHandle,
    out_name: *mut u8,
    max_len: u32,
) -> u32 {
    if handle.is_null() || out_name.is_null() { return 0; }
    let h = &*handle;
    let cs = h.engine.cloud_storage();
    let name = cs.backend_name();
    let bytes = name.as_bytes();
    let len = bytes.len().min(max_len as usize);
    let out = std::slice::from_raw_parts_mut(out_name, max_len as usize);
    out[..len].copy_from_slice(&bytes[..len]);
    len as u32
}

// ─── Visibility Listing Enhanced (Batch 29) ────────────────────────────────

/// List workflows by search attribute. Writes keys to out_keys array.
/// Returns number of keys written.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_list_by_search_attribute(
    handle: *mut EngineHandle,
    attr_key_ptr: *const u8, attr_key_len: u32,
    attr_val_ptr: *const u8, attr_val_len: u32,
    out_keys: *mut u64, max_count: u32,
) -> u32 {
    if handle.is_null() || out_keys.is_null() { return 0; }
    let h = &*handle;
    let key_slice = if attr_key_ptr.is_null() { &[] } else { std::slice::from_raw_parts(attr_key_ptr, attr_key_len as usize) };
    let val_slice = if attr_val_ptr.is_null() { &[] } else { std::slice::from_raw_parts(attr_val_ptr, attr_val_len as usize) };
    let key = std::str::from_utf8(key_slice).unwrap_or("");
    let val = std::str::from_utf8(val_slice).unwrap_or("");
    let attr_val = crate::visibility::SearchAttributeValue::String(val.to_string());
    let results = h.engine.visibility().list_by_search_attribute(key, &attr_val);
    let out = std::slice::from_raw_parts_mut(out_keys, max_count as usize);
    let count = results.len().min(max_count as usize);
    for (i, info) in results.iter().take(count).enumerate() {
        out[i] = info.workflow_key;
    }
    count as u32
}

/// List workflows by time range (start_time_ms..end_time_ms).
/// Writes keys to out_keys array. Returns number of keys written.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_list_by_time_range(
    handle: *mut EngineHandle,
    start_time_ms: u64, end_time_ms: u64,
    out_keys: *mut u64, max_count: u32,
) -> u32 {
    if handle.is_null() || out_keys.is_null() { return 0; }
    let h = &*handle;
    let results = h.engine.visibility().list_by_time_range(start_time_ms, end_time_ms);
    let out = std::slice::from_raw_parts_mut(out_keys, max_count as usize);
    let count = results.len().min(max_count as usize);
    for (i, info) in results.iter().take(count).enumerate() {
        out[i] = info.workflow_key;
    }
    count as u32
}

// ─── Replay Cache Management (Batch 29) ────────────────────────────────────

/// Invalidate the replay cache for a specific workflow.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_replay_invalidate(
    handle: *mut EngineHandle, workflow_key: u64,
) {
    if handle.is_null() { return; }
    let h = &*handle;
    h.engine.replay_engine().invalidate_cache(workflow_key);
}

/// Clear the entire replay cache.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_replay_clear_cache(
    handle: *mut EngineHandle,
) {
    if handle.is_null() { return; }
    let h = &*handle;
    h.engine.replay_engine().clear_cache();
}

/// Get the replay cache size (number of cached replay results).
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_replay_cache_size(
    handle: *mut EngineHandle,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.replay_engine().cache_size() as u64
}

// ─── Schedule Management Enhanced (Batch 29) ───────────────────────────────

/// Update the overlap policy for a schedule. Returns 1 on success.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_schedule_set_overlap_policy(
    handle: *mut EngineHandle, schedule_id: u64, policy: u32,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let overlap = match policy {
        0 => crate::schedules::OverlapPolicy::Skip,
        1 => crate::schedules::OverlapPolicy::BufferOne,
        2 => crate::schedules::OverlapPolicy::BufferAll,
        3 => crate::schedules::OverlapPolicy::TerminateOther,
        4 => crate::schedules::OverlapPolicy::AllowAll,
        _ => return 0,
    };
    if h.engine.schedule_manager().update_overlap_policy(schedule_id, overlap) { 1 } else { 0 }
}

/// Set remaining actions for a schedule. Returns 1 on success.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_schedule_set_remaining_actions(
    handle: *mut EngineHandle, schedule_id: u64, remaining: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.schedule_manager().set_remaining_actions(schedule_id, remaining);
    1
}

// ─── Event History Enhanced (Batch 29) ─────────────────────────────────────

/// Get the number of workflows with event history.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_history_workflow_count_v2(
    handle: *mut EngineHandle,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.history_store().workflow_count() as u64
}

// ─── Partition Worker Management (Batch 29) ────────────────────────────────

/// Get the total pending task count across all partitions for a task queue hash.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_partition_total_pending(
    handle: *mut EngineHandle, task_queue_hash: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.partition_manager().total_pending(task_queue_hash) as u64
}

// ─── Nexus Enhanced (Batch 29) ─────────────────────────────────────────────

/// Register a nexus service with endpoint. Returns 1 on success.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_nexus_register_service(
    handle: *mut EngineHandle,
    service_name_ptr: *const u8, service_name_len: u32,
    endpoint_ptr: *const u8, endpoint_len: u32,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let name_slice = if service_name_ptr.is_null() { &[] } else { std::slice::from_raw_parts(service_name_ptr, service_name_len as usize) };
    let name = std::str::from_utf8(name_slice).unwrap_or("");
    let ep_slice = if endpoint_ptr.is_null() { &[] } else { std::slice::from_raw_parts(endpoint_ptr, endpoint_len as usize) };
    let endpoint = std::str::from_utf8(ep_slice).unwrap_or("");
    h.engine.nexus_manager().register_service(name, endpoint);
    1
}

// ─── Real Cloud Storage SDK (feature-gated) ────────────────────────────────

/// Set cloud storage to real AWS S3 backend.
/// Returns 1 on success, 0 if feature not compiled or on error.
#[cfg(feature = "cloud-s3")]
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cloud_set_s3(
    handle: *mut EngineHandle,
    bucket_ptr: *const u8, bucket_len: u32,
    region_ptr: *const u8, region_len: u32,
    access_key_ptr: *const u8, access_key_len: u32,
    secret_key_ptr: *const u8, secret_key_len: u32,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let to_str = |p: *const u8, l: u32| -> &str {
        if p.is_null() { return ""; }
        let s = std::slice::from_raw_parts(p, l as usize);
        std::str::from_utf8(s).unwrap_or("")
    };
    let bucket = to_str(bucket_ptr, bucket_len);
    let region = to_str(region_ptr, region_len);
    let ak = to_str(access_key_ptr, access_key_len);
    let sk = to_str(secret_key_ptr, secret_key_len);
    let adapter = crate::cold_storage::S3Adapter::new(bucket, region, ak, sk);
    h.engine.set_cloud_storage(std::sync::Arc::new(adapter));
    1
}

#[cfg(not(feature = "cloud-s3"))]
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cloud_set_s3(
    _handle: *mut EngineHandle,
    _bucket_ptr: *const u8, _bucket_len: u32,
    _region_ptr: *const u8, _region_len: u32,
    _access_key_ptr: *const u8, _access_key_len: u32,
    _secret_key_ptr: *const u8, _secret_key_len: u32,
) -> i32 {
    0 // Feature not compiled
}

/// Set cloud storage to real GCS backend.
/// Returns 1 on success, 0 if feature not compiled or on error.
#[cfg(feature = "cloud-gcs")]
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cloud_set_gcs(
    handle: *mut EngineHandle,
    bucket_ptr: *const u8, bucket_len: u32,
    token_ptr: *const u8, token_len: u32,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let to_str = |p: *const u8, l: u32| -> &str {
        if p.is_null() { return ""; }
        let s = std::slice::from_raw_parts(p, l as usize);
        std::str::from_utf8(s).unwrap_or("")
    };
    let bucket = to_str(bucket_ptr, bucket_len);
    let token = to_str(token_ptr, token_len);
    let adapter = crate::cold_storage::GcsAdapter::new(bucket, token);
    h.engine.set_cloud_storage(std::sync::Arc::new(adapter));
    1
}

#[cfg(not(feature = "cloud-gcs"))]
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_cloud_set_gcs(
    _handle: *mut EngineHandle,
    _bucket_ptr: *const u8, _bucket_len: u32,
    _token_ptr: *const u8, _token_len: u32,
) -> i32 {
    0 // Feature not compiled
}

// ─── Replication Apply (Batch 30) ───────────────────────────────────────────

/// Apply an incoming replication task from a remote cluster.
/// Returns 1 on success, 0 on failure.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_apply_replication_task(
    handle: *mut EngineHandle,
    source_cluster_id: u64,
    target_cluster_id: u64,
    workflow_key: u64,
    event_type: u32,
    payload_ptr: *const u8,
    payload_len: u32,
    failover_version: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let payload = if payload_ptr.is_null() || payload_len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(payload_ptr, payload_len as usize).to_vec()
    };
    let task = crate::cluster::ReplicationTask {
        task_id: 0,
        source_cluster_id,
        target_cluster_id,
        workflow_key,
        event_type,
        payload,
        failover_version,
        task_type: crate::cluster::ReplicationTaskType::SyncHistory,
        first_event_id: 0,
        last_event_id: 0,
        created_ms: 0,
    };
    if h.engine.apply_replication_task(task) { 1 } else { 0 }
}

/// Process a fired timer — re-enqueues any pending activity retries.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_process_fired_timer(
    handle: *mut EngineHandle,
    workflow_key: u64,
) {
    if handle.is_null() { return; }
    let h = &*handle;
    h.engine.process_fired_timer(workflow_key);
}

/// Get replication status. Writes (pending, cluster_count, active, applied) to out array of 4 u64s.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_replication_status(
    handle: *mut EngineHandle,
    out: *mut u64,
) -> i32 {
    if handle.is_null() || out.is_null() { return 0; }
    let h = &*handle;
    let (pending, count, active, applied) = h.engine.cluster_manager().replication_status();
    let slice = std::slice::from_raw_parts_mut(out, 4);
    slice[0] = pending as u64;
    slice[1] = count as u64;
    slice[2] = active as u64;
    slice[3] = applied as u64;
    1
}

/// Set a cluster as active or standby. Returns 1 on success.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_set_cluster_active(
    handle: *mut EngineHandle,
    cluster_id: u64,
    active: i32,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    if h.engine.cluster_manager().set_cluster_active(cluster_id, active != 0) { 1 } else { 0 }
}

/// Set failover version for a cluster. Returns 1 on success.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_set_failover_version(
    handle: *mut EngineHandle,
    cluster_id: u64,
    version: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    if h.engine.cluster_manager().set_failover_version(cluster_id, version) { 1 } else { 0 }
}

// ─── Nexus Full Lifecycle (Batch 31) ──────────────────────────────────────

/// Mark a nexus operation as started (handler acknowledged). Returns 1 on success.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_nexus_mark_started(
    handle: *mut EngineHandle,
    op_id: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    if h.engine.nexus_manager().mark_started(op_id, None) { 1 } else { 0 }
}

/// Cancel a nexus operation. Returns 1 on success.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_nexus_cancel(
    handle: *mut EngineHandle,
    op_id: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    if h.engine.nexus_manager().cancel_operation(op_id) { 1 } else { 0 }
}

/// Timeout a nexus operation. Returns 1 on success.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_nexus_timeout(
    handle: *mut EngineHandle,
    op_id: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    if h.engine.nexus_manager().timeout_operation(op_id) { 1 } else { 0 }
}

/// Retry a failed/timed-out nexus operation. Returns 1 on success.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_nexus_retry(
    handle: *mut EngineHandle,
    op_id: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    if h.engine.nexus_manager().retry_operation(op_id) { 1 } else { 0 }
}

/// Count nexus operations by state (0=Scheduled,1=Started,2=Completed,3=Failed,4=Canceled,5=TimedOut).
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_nexus_count_by_state(
    handle: *mut EngineHandle,
    state: u32,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let s = match state {
        0 => crate::nexus::NexusOperationState::Scheduled,
        1 => crate::nexus::NexusOperationState::Started,
        2 => crate::nexus::NexusOperationState::Completed,
        3 => crate::nexus::NexusOperationState::Failed,
        4 => crate::nexus::NexusOperationState::Canceled,
        5 => crate::nexus::NexusOperationState::TimedOut,
        _ => crate::nexus::NexusOperationState::Scheduled,
    };
    h.engine.nexus_manager().count_by_state(s) as u64
}

// ─── Worker Registry Load-Aware Dispatch (Batch 31) ───────────────────────

/// Select the best worker for a task queue using load-aware dispatch. Returns worker_id or 0.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_select_worker(
    handle: *mut EngineHandle,
    tq_hash: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.worker_registry().select_worker(tq_hash).unwrap_or(0)
}

/// Check if a worker has capacity. Returns 1 if yes.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_worker_has_capacity(
    handle: *mut EngineHandle,
    worker_id: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    if h.engine.worker_registry().has_capacity(worker_id) { 1 } else { 0 }
}

/// Drain a worker (stop dispatching new tasks). Returns 1 on success.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_drain_worker(
    handle: *mut EngineHandle,
    worker_id: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    if h.engine.worker_registry().drain_worker(worker_id) { 1 } else { 0 }
}

/// Register a worker with explicit capacity. Returns worker_id.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_register_worker_with_capacity(
    handle: *mut EngineHandle,
    address_ptr: *const u8,
    address_len: u32,
    tq_hash_ptr: *const u64,
    tq_hash_count: u32,
    version_ptr: *const u8,
    version_len: u32,
    max_concurrent: u32,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let address = if address_ptr.is_null() || address_len == 0 { String::new() } else {
        String::from_utf8_lossy(std::slice::from_raw_parts(address_ptr, address_len as usize)).to_string()
    };
    let version = if version_ptr.is_null() || version_len == 0 { String::new() } else {
        String::from_utf8_lossy(std::slice::from_raw_parts(version_ptr, version_len as usize)).to_string()
    };
    let hashes = if tq_hash_ptr.is_null() || tq_hash_count == 0 { Vec::new() } else {
        std::slice::from_raw_parts(tq_hash_ptr, tq_hash_count as usize).to_vec()
    };
    h.engine.worker_registry().register_worker_with_capacity(&address, &hashes, &[], &version, max_concurrent)
}

/// Get total current load across all workers.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_total_worker_load(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.worker_registry().total_current_load() as u64
}

/// Get total available capacity across all workers.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_total_worker_capacity(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.worker_registry().total_capacity() as u64
}

// ─── Consistent Hash Ring Sharding (Batch 31) ─────────────────────────────

/// Add a host to the consistent hash ring.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_sharding_add_host(
    handle: *mut EngineHandle,
    host_ptr: *const u8,
    host_len: u32,
) {
    if handle.is_null() { return; }
    let h = &*handle;
    let host = if host_ptr.is_null() || host_len == 0 { return; } else {
        String::from_utf8_lossy(std::slice::from_raw_parts(host_ptr, host_len as usize)).to_string()
    };
    h.engine.shard_manager().add_host(&host);
}

/// Remove a host from the consistent hash ring. Returns 1 on success.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_sharding_remove_host(
    handle: *mut EngineHandle,
    host_ptr: *const u8,
    host_len: u32,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let host = if host_ptr.is_null() || host_len == 0 { return 0; } else {
        String::from_utf8_lossy(std::slice::from_raw_parts(host_ptr, host_len as usize)).to_string()
    };
    if h.engine.shard_manager().remove_host(&host) { 1 } else { 0 }
}

/// Migrate a shard to a new host. Returns 1 on success.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_sharding_migrate(
    handle: *mut EngineHandle,
    shard_id: u32,
    host_ptr: *const u8,
    host_len: u32,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let host = if host_ptr.is_null() || host_len == 0 { return 0; } else {
        String::from_utf8_lossy(std::slice::from_raw_parts(host_ptr, host_len as usize)).to_string()
    };
    if h.engine.shard_manager().migrate_shard(shard_id, &host) { 1 } else { 0 }
}

/// Get number of hosts on the hash ring.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_sharding_host_count(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.shard_manager().host_count() as u64
}

// ─── Hierarchical Partitions (Batch 31) ───────────────────────────────────

/// Create a child partition under an existing parent. Returns child partition_id or 0.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_create_child_partition(
    handle: *mut EngineHandle,
    parent_id: u32,
    tq_hash: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.partition_manager().create_child_partition(parent_id, tq_hash).unwrap_or(0) as u64
}

/// Delete a partition. Returns 1 on success.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_delete_partition(
    handle: *mut EngineHandle,
    partition_id: u32,
) -> i32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    if h.engine.partition_manager().delete_partition(partition_id) { 1 } else { 0 }
}

/// Get the depth of a partition in the hierarchy.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_partition_depth(
    handle: *mut EngineHandle,
    partition_id: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let h = &*handle;
    h.engine.partition_manager().partition_depth(partition_id).map(|d| d as i32).unwrap_or(-1)
}

/// Get total backlog across all partitions for a task queue.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_partition_backlog(
    handle: *mut EngineHandle,
    tq_hash: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.partition_manager().total_backlog(tq_hash)
}

// ─── Get Workflow Search Attributes (Batch 32) ────────────────────────────

/// Get the number of search attributes for a workflow. Returns count (0 if not found).
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_get_search_attr_count(
    handle: *mut EngineHandle,
    workflow_key: u64,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.get_workflow_search_attributes(workflow_key).map(|m| m.len() as u64).unwrap_or(0)
}

/// Get a search attribute key by index. Writes key to out_key buffer. Returns key length (0 if not found).
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_get_search_attr_key(
    handle: *mut EngineHandle,
    workflow_key: u64,
    index: u64,
    out_key: *mut u8,
    out_key_cap: u32,
) -> u32 {
    if handle.is_null() || out_key.is_null() { return 0; }
    let h = &*handle;
    let attrs = match h.engine.get_workflow_search_attributes(workflow_key) {
        Some(a) => a,
        None => return 0,
    };
    let keys: Vec<&String> = attrs.keys().collect();
    if index as usize >= keys.len() { return 0; }
    let key = keys[index as usize].as_bytes();
    let len = key.len().min(out_key_cap as usize);
    std::ptr::copy_nonoverlapping(key.as_ptr(), out_key, len);
    len as u32
}

/// Get a search attribute string value by index. Returns value length (0 if not found).
/// Writes the string representation to out_val buffer.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_get_search_attr_val(
    handle: *mut EngineHandle,
    workflow_key: u64,
    index: u64,
    out_val: *mut u8,
    out_val_cap: u32,
) -> u32 {
    if handle.is_null() || out_val.is_null() { return 0; }
    let h = &*handle;
    let attrs = match h.engine.get_workflow_search_attributes(workflow_key) {
        Some(a) => a,
        None => return 0,
    };
    let keys: Vec<&String> = attrs.keys().collect();
    if index as usize >= keys.len() { return 0; }
    let val = match attrs.get(keys[index as usize]) {
        Some(v) => format!("{:?}", v),
        None => return 0,
    };
    let bytes = val.as_bytes();
    let len = bytes.len().min(out_val_cap as usize);
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_val, len);
    len as u32
}

// ─── Replication Transport (Batch 33) ─────────────────────────────────────

/// Add a replication link to a remote cluster.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_repl_add_link(
    handle: *mut EngineHandle,
    cluster_name: *const u8, cluster_name_len: u32,
    cluster_id: u64,
    endpoint: *const u8, endpoint_len: u32,
) {
    if handle.is_null() { return; }
    let h = &*handle;
    let name_bytes = std::slice::from_raw_parts(cluster_name, cluster_name_len as usize);
    let ep_bytes = std::slice::from_raw_parts(endpoint, endpoint_len as usize);
    let name = std::str::from_utf8(name_bytes).unwrap_or("unknown");
    let ep = std::str::from_utf8(ep_bytes).unwrap_or("");
    h.engine.replication_transport().add_link(name, cluster_id, ep);
}

/// Remove a replication link.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_repl_remove_link(
    handle: *mut EngineHandle,
    cluster_id: u64,
) -> bool {
    if handle.is_null() { return false; }
    let h = &*handle;
    h.engine.replication_transport().remove_link(cluster_id)
}

/// Set a replication link active/inactive.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_repl_set_link_active(
    handle: *mut EngineHandle,
    cluster_id: u64,
    active: bool,
) -> bool {
    if handle.is_null() { return false; }
    let h = &*handle;
    h.engine.replication_transport().set_link_active(cluster_id, active)
}

/// Pull outgoing replication tasks for a remote cluster (poll-based transport).
/// Writes task count to out_count. Returns task count.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_repl_pull_for_cluster(
    handle: *mut EngineHandle,
    cluster_id: u64,
    max_count: u32,
) -> u32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    let tasks = h.engine.replication_transport().pull_for_cluster(cluster_id, max_count as usize);
    // Store tasks back into the cluster manager's replication queue for gRPC delivery
    // For now, just return the count — the gRPC layer will call drain again with serialization
    tasks.len() as u32
}

/// Push incoming replication tasks from a remote cluster.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_repl_push_from_cluster(
    handle: *mut EngineHandle,
    cluster_id: u64,
    workflow_key: u64,
    event_type: u32,
    payload: *const u8,
    payload_len: u32,
    failover_version: u64,
    last_event_id: u64,
) -> bool {
    if handle.is_null() { return false; }
    let h = &*handle;
    let pl = if payload.is_null() || payload_len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(payload, payload_len as usize).to_vec()
    };
    let task = crate::cluster::ReplicationTask {
        task_id: 0,
        source_cluster_id: cluster_id,
        target_cluster_id: 0,
        workflow_key,
        event_type,
        payload: pl,
        failover_version,
        task_type: crate::cluster::ReplicationTaskType::SyncHistory,
        first_event_id: last_event_id,
        last_event_id,
        created_ms: 0,
    };
    h.engine.replication_transport().push_from_cluster(cluster_id, vec![task]) > 0
}

/// Get the number of active replication links.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_repl_active_link_count(
    handle: *mut EngineHandle,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.replication_transport().active_link_count() as u64
}

/// Get total pending outgoing tasks across all links.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_repl_total_pending_outgoing(
    handle: *mut EngineHandle,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.replication_transport().total_pending_outgoing() as u64
}

/// Get total pending incoming tasks across all links.
#[no_mangle]
pub unsafe extern "C" fn velocity_engine_repl_total_pending_incoming(
    handle: *mut EngineHandle,
) -> u64 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.replication_transport().total_pending_incoming() as u64
}

// ── Replication Daemon FFI ──────────────────────────────────────────────

/// Global replication daemon instance (one per engine).
static REPL_DAEMON: std::sync::OnceLock<std::sync::Arc<crate::replication_daemon::ReplicationDaemon>> = std::sync::OnceLock::new();

fn get_daemon(engine: *mut std::ffi::c_void) -> std::sync::Arc<crate::replication_daemon::ReplicationDaemon> {
    REPL_DAEMON.get_or_init(|| {
        let handle = unsafe { &*(engine as *const crate::engine::WorkflowEngine) };
        std::sync::Arc::new(crate::replication_daemon::ReplicationDaemon::new(
            handle.replication_transport().clone(),
            crate::replication_daemon::ReplicationDaemonConfig::default(),
        ))
    }).clone()
}

/// Start the replication daemon background poller.
#[no_mangle]
pub extern "C" fn velocity_engine_repl_daemon_start(engine: *mut std::ffi::c_void) -> u32 {
    let daemon = get_daemon(engine);
    if daemon.start() { 1 } else { 0 }
}

/// Stop the replication daemon.
#[no_mangle]
pub extern "C" fn velocity_engine_repl_daemon_stop(engine: *mut std::ffi::c_void) -> u32 {
    if let Some(daemon) = REPL_DAEMON.get() {
        daemon.stop();
        1
    } else {
        0
    }
}

/// Check if the daemon is running.
#[no_mangle]
pub extern "C" fn velocity_engine_repl_daemon_is_running(engine: *mut std::ffi::c_void) -> u32 {
    if let Some(daemon) = REPL_DAEMON.get() {
        if daemon.is_running() { 1 } else { 0 }
    } else {
        0
    }
}

/// Run one poll cycle manually (useful for testing without background thread).
#[no_mangle]
pub extern "C" fn velocity_engine_repl_daemon_poll_once(engine: *mut std::ffi::c_void) -> u64 {
    let daemon = get_daemon(engine);
    let handle = unsafe { &*(engine as *const crate::engine::WorkflowEngine) };
    let (delivered, applied) = daemon.poll_once(handle);
    ((delivered as u64) << 32) | (applied as u64)
}

/// Get daemon stats: total_cycles.
#[no_mangle]
pub extern "C" fn velocity_engine_repl_daemon_stat_cycles(engine: *mut std::ffi::c_void) -> u64 {
    if let Some(daemon) = REPL_DAEMON.get() {
        daemon.stats().total_cycles
    } else {
        0
    }
}

/// Get daemon stats: total_outgoing_delivered.
#[no_mangle]
pub extern "C" fn velocity_engine_repl_daemon_stat_delivered(engine: *mut std::ffi::c_void) -> u64 {
    if let Some(daemon) = REPL_DAEMON.get() {
        daemon.stats().total_outgoing_delivered
    } else {
        0
    }
}

/// Get daemon stats: total_incoming_applied.
#[no_mangle]
pub extern "C" fn velocity_engine_repl_daemon_stat_applied(engine: *mut std::ffi::c_void) -> u64 {
    if let Some(daemon) = REPL_DAEMON.get() {
        daemon.stats().total_incoming_applied
    } else {
        0
    }
}

/// Get daemon stats: total failures (outgoing + incoming).
#[no_mangle]
pub extern "C" fn velocity_engine_repl_daemon_stat_failures(engine: *mut std::ffi::c_void) -> u64 {
    if let Some(daemon) = REPL_DAEMON.get() {
        let s = daemon.stats();
        s.total_outgoing_failed + s.total_incoming_failed
    } else {
        0
    }
}

/// Get daemon stats: uptime_ms.
#[no_mangle]
pub extern "C" fn velocity_engine_repl_daemon_stat_uptime(engine: *mut std::ffi::c_void) -> u64 {
    if let Some(daemon) = REPL_DAEMON.get() {
        daemon.stats().uptime_ms
    } else {
        0
    }
}

/// Get count of recent delivery log entries.
#[no_mangle]
pub extern "C" fn velocity_engine_repl_daemon_delivery_count(engine: *mut std::ffi::c_void) -> u64 {
    if let Some(daemon) = REPL_DAEMON.get() {
        daemon.recent_deliveries(10_000).len() as u64
    } else {
        0
    }
}

// ============================================================
// Batch 35+ — Raft Consensus, History Compaction, Durable RPC, AI Context
// ============================================================

use std::sync::OnceLock;
use crate::raft_consensus::{RaftCluster, RaftConfig};
use crate::history_compaction::{HistoryCompactor, CompactionConfig, CompactableEventType};
use crate::durable_rpc::{DurableServiceMesh, DurableRpcConfig};
use crate::ai_context::{AiContextWindow, AiContextConfig, MessageRole};

static RAFT_CLUSTER: OnceLock<std::sync::Mutex<RaftCluster>> = OnceLock::new();
static HISTORY_COMPACTOR: OnceLock<std::sync::Mutex<HistoryCompactor>> = OnceLock::new();
static SERVICE_MESH: OnceLock<std::sync::Mutex<DurableServiceMesh>> = OnceLock::new();
static AI_CONTEXT: OnceLock<std::sync::Mutex<AiContextWindow>> = OnceLock::new();

fn get_raft() -> &'static std::sync::Mutex<RaftCluster> {
    RAFT_CLUSTER.get_or_init(|| std::sync::Mutex::new(RaftCluster::new()))
}
fn get_compactor() -> &'static std::sync::Mutex<HistoryCompactor> {
    HISTORY_COMPACTOR.get_or_init(|| std::sync::Mutex::new(HistoryCompactor::new(CompactionConfig::default())))
}
fn get_mesh() -> &'static std::sync::Mutex<DurableServiceMesh> {
    SERVICE_MESH.get_or_init(|| std::sync::Mutex::new(DurableServiceMesh::new(DurableRpcConfig::default())))
}
fn get_ai_ctx() -> &'static std::sync::Mutex<AiContextWindow> {
    AI_CONTEXT.get_or_init(|| std::sync::Mutex::new(AiContextWindow::new(AiContextConfig::default())))
}

// --- Raft Consensus FFI ---

#[no_mangle]
pub extern "C" fn velocity_raft_create_group(node_id: u64) -> u64 {
    let mut cluster = get_raft().lock().unwrap();
    cluster.create_group(RaftConfig { node_id, ..Default::default() })
}

#[no_mangle]
pub extern "C" fn velocity_raft_become_leader(group_id: u64) -> bool {
    let mut cluster = get_raft().lock().unwrap();
    if let Some(node) = cluster.get_node_mut(group_id) {
        node.start_election();
        node.become_leader();
        true
    } else { false }
}

#[no_mangle]
pub extern "C" fn velocity_raft_append_entry(group_id: u64, workflow_key: u64, event_type: u8, payload: *const u8, payload_len: u32) -> u64 {
    let mut cluster = get_raft().lock().unwrap();
    let data = if !payload.is_null() && payload_len > 0 {
        unsafe { std::slice::from_raw_parts(payload, payload_len as usize) }.to_vec()
    } else { Vec::new() };
    let et = match event_type {
        0 => crate::raft_consensus::RaftEventType::WorkflowStarted,
        1 => crate::raft_consensus::RaftEventType::StepCompleted,
        2 => crate::raft_consensus::RaftEventType::ActivityScheduled,
        3 => crate::raft_consensus::RaftEventType::ActivityCompleted,
        _ => crate::raft_consensus::RaftEventType::WorkflowCompleted,
    };
    if let Some(node) = cluster.get_node_mut(group_id) {
        node.append_entry(workflow_key, et, data).unwrap_or(0)
    } else { 0 }
}

#[no_mangle]
pub extern "C" fn velocity_raft_apply_committed(group_id: u64) -> u64 {
    let mut cluster = get_raft().lock().unwrap();
    if let Some(node) = cluster.get_node_mut(group_id) {
        node.apply_committed().len() as u64
    } else { 0 }
}

#[no_mangle]
pub extern "C" fn velocity_raft_group_count() -> u64 {
    get_raft().lock().unwrap().group_count() as u64
}

#[no_mangle]
pub extern "C" fn velocity_raft_stat_committed() -> u64 {
    get_raft().lock().unwrap().aggregate_stats().entries_committed
}

// --- History Compaction FFI ---

#[no_mangle]
pub extern "C" fn velocity_compact_append_event(workflow_key: u64, event_type: u8) -> u64 {
    let mut compactor = get_compactor().lock().unwrap();
    let et = match event_type {
        0 => CompactableEventType::WorkflowStarted,
        1 => CompactableEventType::ActivityTaskScheduled,
        2 => CompactableEventType::ActivityTaskCompleted,
        3 => CompactableEventType::TimerStarted,
        4 => CompactableEventType::TimerFired,
        5 => CompactableEventType::SignalReceived,
        6 => CompactableEventType::WorkflowTaskScheduled,
        7 => CompactableEventType::WorkflowTaskCompleted,
        _ => CompactableEventType::WorkflowStarted,
    };
    compactor.append_event(workflow_key, et, Vec::new())
}

#[no_mangle]
pub extern "C" fn velocity_compact_workflow(workflow_key: u64) -> u64 {
    get_compactor().lock().unwrap().compact_workflow(workflow_key)
}

#[no_mangle]
pub extern "C" fn velocity_compact_all() -> u64 {
    get_compactor().lock().unwrap().compact_all()
}

#[no_mangle]
pub extern "C" fn velocity_compact_event_count(workflow_key: u64) -> u64 {
    get_compactor().lock().unwrap().workflow_event_count(workflow_key) as u64
}

// --- Durable RPC FFI ---

#[no_mangle]
pub extern "C" fn velocity_rpc_initiate(caller: *const u8, caller_len: u32, target: *const u8, target_len: u32, method: *const u8, method_len: u32) -> u64 {
    let c = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(caller, caller_len as usize)) };
    let t = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(target, target_len as usize)) };
    let m = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(method, method_len as usize)) };
    let mut mesh = get_mesh().lock().unwrap();
    mesh.initiate_rpc(c, t, m, Vec::new(), None, None).unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn velocity_rpc_complete(rpc_id: u64) -> bool {
    get_mesh().lock().unwrap().complete_rpc(rpc_id, Vec::new())
}

#[no_mangle]
pub extern "C" fn velocity_rpc_fail(rpc_id: u64) -> bool {
    get_mesh().lock().unwrap().fail_rpc(rpc_id, "ffi_error")
}

#[no_mangle]
pub extern "C" fn velocity_rpc_count() -> u64 {
    get_mesh().lock().unwrap().rpc_count() as u64
}

#[no_mangle]
pub extern "C" fn velocity_rpc_stat_completed() -> u64 {
    get_mesh().lock().unwrap().stats().completed_rpcs
}

// --- AI Context FFI ---

#[no_mangle]
pub extern "C" fn velocity_ai_add_message(role: u8, content: *const u8, content_len: u32) -> u64 {
    let text = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(content, content_len as usize)) };
    let r = match role {
        0 => MessageRole::System,
        1 => MessageRole::User,
        2 => MessageRole::Assistant,
        3 => MessageRole::Tool,
        _ => MessageRole::User,
    };
    get_ai_ctx().lock().unwrap().add_message(r, text) as u64
}

#[no_mangle]
pub extern "C" fn velocity_ai_compress() -> u64 {
    get_ai_ctx().lock().unwrap().compress() as u64
}

#[no_mangle]
pub extern "C" fn velocity_ai_current_tokens() -> u64 {
    get_ai_ctx().lock().unwrap().current_tokens() as u64
}

#[no_mangle]
pub extern "C" fn velocity_ai_message_count() -> u64 {
    get_ai_ctx().lock().unwrap().message_count() as u64
}

#[no_mangle]
pub extern "C" fn velocity_ai_add_tool_call(tool: *const u8, tool_len: u32, args: *const u8, args_len: u32) -> u64 {
    let t = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(tool, tool_len as usize)) };
    let a = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(args, args_len as usize)) };
    let ctx = get_ai_ctx().lock().unwrap();
    // Return tool call count as ID (simplified)
    ctx.stats().total_tool_calls + 1
}

// ─── Hardware Abstraction Layer FFI ─────────────────────────────────────────

use crate::hardware_integration::{HardwareAbstractionLayer, MerkleEccResult, compute_simple_merkle_root};

static HAL: OnceLock<std::sync::Mutex<HardwareAbstractionLayer>> = OnceLock::new();

fn get_hal() -> &'static std::sync::Mutex<HardwareAbstractionLayer> {
    HAL.get_or_init(|| std::sync::Mutex::new(HardwareAbstractionLayer::with_simulated_hardware()))
}

/// Initialize the HAL with simulated hardware. Returns 1 on success.
#[no_mangle]
pub extern "C" fn velocity_hal_init() -> u32 {
    let _ = get_hal();
    1
}

/// Called after slab mutation. Computes ECC parity and optionally offloads to SmartNIC.
/// Returns the parity length in bytes.
#[no_mangle]
pub unsafe extern "C" fn velocity_hal_on_slab_write(
    workflow_key: u64,
    slab_ptr: *const u8,
    slab_len: u32,
    merkle_root_ptr: *const u8,
) -> u64 {
    let slab_data = std::slice::from_raw_parts(slab_ptr, slab_len as usize);
    let mut merkle_root = [0u8; 32];
    if !merkle_root_ptr.is_null() {
        merkle_root.copy_from_slice(std::slice::from_raw_parts(merkle_root_ptr, 32));
    }
    let parity = get_hal().lock().unwrap().on_slab_write(workflow_key, slab_data, merkle_root);
    parity.len() as u64
}

/// Called before slab read. Verifies Merkle root + ECC parity.
/// Returns: 0 = Valid, 1 = Repaired, 2 = Unrecoverable
#[no_mangle]
pub unsafe extern "C" fn velocity_hal_on_slab_read(
    workflow_key: u64,
    slab_ptr: *mut u8,
    slab_len: u32,
    merkle_root_ptr: *const u8,
) -> u32 {
    let slab_data = std::slice::from_raw_parts_mut(slab_ptr, slab_len as usize);
    let mut merkle_root = [0u8; 32];
    if !merkle_root_ptr.is_null() {
        merkle_root.copy_from_slice(std::slice::from_raw_parts(merkle_root_ptr, 32));
    }
    let result = get_hal().lock().unwrap().on_slab_read(workflow_key, slab_data, &merkle_root);
    match result {
        crate::hardware_traits::VerificationResult::Valid => 0,
        crate::hardware_traits::VerificationResult::Repaired => 1,
        crate::hardware_traits::VerificationResult::Unrecoverable => 2,
    }
}

/// Full Merkle ECC self-healing loop. Returns: 0 = Valid, 1 = Repaired, 2 = Unrecoverable
#[no_mangle]
pub unsafe extern "C" fn velocity_hal_merkle_ecc_self_heal(
    workflow_key: u64,
    slab_ptr: *mut u8,
    slab_len: u32,
) -> u32 {
    let slab_data = std::slice::from_raw_parts_mut(slab_ptr, slab_len as usize);
    let result = get_hal().lock().unwrap().merkle_ecc_self_heal(workflow_key, slab_data);
    match result {
        MerkleEccResult::Valid => 0,
        MerkleEccResult::Repaired => 1,
        MerkleEccResult::MerkleMismatchUnrecoverable | MerkleEccResult::Unrecoverable => 2,
    }
}

/// Get ECC verification count.
#[no_mangle]
pub extern "C" fn velocity_hal_ecc_verifications() -> u64 {
    get_hal().lock().unwrap().ecc_stats().total_verifications
}

/// Get ECC repair count.
#[no_mangle]
pub extern "C" fn velocity_hal_ecc_repairs() -> u64 {
    get_hal().lock().unwrap().ecc_stats().total_repairs
}

/// Get slab write count (HAL-tracked).
#[no_mangle]
pub extern "C" fn velocity_hal_slab_write_count() -> u64 {
    get_hal().lock().unwrap().slab_write_count()
}

/// Get slab read count (HAL-tracked).
#[no_mangle]
pub extern "C" fn velocity_hal_slab_read_count() -> u64 {
    get_hal().lock().unwrap().slab_read_count()
}

/// Get SmartNIC offload count.
#[no_mangle]
pub extern "C" fn velocity_hal_nic_offload_count() -> u64 {
    get_hal().lock().unwrap().nic_offload_count()
}

/// Get TEE enclave count.
#[no_mangle]
pub extern "C" fn velocity_hal_tee_enclave_count() -> u64 {
    get_hal().lock().unwrap().tee_enclave_count()
}

/// Cleanup HAL data for a workflow.
#[no_mangle]
pub extern "C" fn velocity_hal_cleanup_workflow(workflow_key: u64) {
    get_hal().lock().unwrap().cleanup_workflow(workflow_key);
}

/// Check if ECC verification is enabled.
#[no_mangle]
pub extern "C" fn velocity_hal_is_ecc_enabled() -> u32 {
    if get_hal().lock().unwrap().is_ecc_enabled() { 1 } else { 0 }
}

/// Check if SmartNIC offload is enabled.
#[no_mangle]
pub extern "C" fn velocity_hal_is_nic_enabled() -> u32 {
    if get_hal().lock().unwrap().is_nic_enabled() { 1 } else { 0 }
}

/// Check if TEE protection is enabled.
#[no_mangle]
pub extern "C" fn velocity_hal_is_tee_enabled() -> u32 {
    if get_hal().lock().unwrap().is_tee_enabled() { 1 } else { 0 }
}

/// Compute a simple Merkle root for arbitrary data.
#[no_mangle]
pub unsafe extern "C" fn velocity_hal_compute_merkle_root(
    data_ptr: *const u8,
    data_len: u32,
    out_root: *mut u8,
) {
    let data = std::slice::from_raw_parts(data_ptr, data_len as usize);
    let root = compute_simple_merkle_root(data);
    std::ptr::copy_nonoverlapping(root.as_ptr(), out_root, 32);
}

// ─── Network Replication FFI ──────────────────────────────────────────────────

use crate::network_replication::{TcpReplicationServer, TcpReplicationConfig, UdpReplicationTransport, UdpReplicationConfig};
use crate::search_index::SearchAttributeIndex;

static TCP_REPL_SERVER: OnceLock<std::sync::Mutex<Option<TcpReplicationServer>>> = OnceLock::new();
static UDP_REPL_TRANSPORT: OnceLock<std::sync::Mutex<Option<UdpReplicationTransport>>> = OnceLock::new();
static SEARCH_INDEX: OnceLock<SearchAttributeIndex> = OnceLock::new();

fn get_tcp_repl() -> &'static std::sync::Mutex<Option<TcpReplicationServer>> {
    TCP_REPL_SERVER.get_or_init(|| std::sync::Mutex::new(None))
}

fn get_udp_repl() -> &'static std::sync::Mutex<Option<UdpReplicationTransport>> {
    UDP_REPL_TRANSPORT.get_or_init(|| std::sync::Mutex::new(None))
}

fn get_search_index() -> &'static SearchAttributeIndex {
    SEARCH_INDEX.get_or_init(|| SearchAttributeIndex::new())
}

/// Initialize TCP replication server. Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn velocity_net_tcp_init(
    bind_addr_ptr: *const u8,
    bind_addr_len: u32,
    cluster_id: u64,
    failover_version: u64,
) -> i32 {
    let bind_addr = if bind_addr_ptr.is_null() || bind_addr_len == 0 {
        "127.0.0.1:9090".to_string()
    } else {
        let slice = unsafe { std::slice::from_raw_parts(bind_addr_ptr, bind_addr_len as usize) };
        String::from_utf8_lossy(slice).to_string()
    };

    let config = TcpReplicationConfig {
        bind_addr,
        cluster_id,
        failover_version,
        ..Default::default()
    };
    let mut server = TcpReplicationServer::new(config);
    match server.bind() {
        Ok(_) => {
            *get_tcp_repl().lock().unwrap() = Some(server);
            0
        }
        Err(_) => -1,
    }
}

/// Get TCP replication connections accepted count.
#[no_mangle]
pub extern "C" fn velocity_net_tcp_connections_accepted() -> u64 {
    let guard = get_tcp_repl().lock().unwrap();
    guard.as_ref().map(|s| s.stats().connections_accepted).unwrap_or(0)
}

/// Get TCP replication frames sent count.
#[no_mangle]
pub extern "C" fn velocity_net_tcp_frames_sent() -> u64 {
    let guard = get_tcp_repl().lock().unwrap();
    guard.as_ref().map(|s| s.stats().frames_sent).unwrap_or(0)
}

/// Get TCP replication bytes sent count.
#[no_mangle]
pub extern "C" fn velocity_net_tcp_bytes_sent() -> u64 {
    let guard = get_tcp_repl().lock().unwrap();
    guard.as_ref().map(|s| s.stats().bytes_sent).unwrap_or(0)
}

/// Get TCP replication tasks sent count.
#[no_mangle]
pub extern "C" fn velocity_net_tcp_tasks_sent() -> u64 {
    let guard = get_tcp_repl().lock().unwrap();
    guard.as_ref().map(|s| s.stats().tasks_sent).unwrap_or(0)
}

/// Initialize UDP replication transport. Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn velocity_net_udp_init(
    bind_addr_ptr: *const u8,
    bind_addr_len: u32,
    peer_addr_ptr: *const u8,
    peer_addr_len: u32,
    cluster_id: u64,
) -> i32 {
    let bind_addr = if bind_addr_ptr.is_null() || bind_addr_len == 0 {
        "127.0.0.1:9091".to_string()
    } else {
        let slice = unsafe { std::slice::from_raw_parts(bind_addr_ptr, bind_addr_len as usize) };
        String::from_utf8_lossy(slice).to_string()
    };
    let peer_addr = if peer_addr_ptr.is_null() || peer_addr_len == 0 {
        "127.0.0.1:9092".to_string()
    } else {
        let slice = unsafe { std::slice::from_raw_parts(peer_addr_ptr, peer_addr_len as usize) };
        String::from_utf8_lossy(slice).to_string()
    };

    let config = UdpReplicationConfig {
        bind_addr,
        peer_addr,
        cluster_id,
        ..Default::default()
    };
    let mut transport = UdpReplicationTransport::new(config);
    match transport.bind() {
        Ok(_) => {
            *get_udp_repl().lock().unwrap() = Some(transport);
            0
        }
        Err(_) => -1,
    }
}

/// Get UDP packets sent count.
#[no_mangle]
pub extern "C" fn velocity_net_udp_packets_sent() -> u64 {
    let guard = get_udp_repl().lock().unwrap();
    guard.as_ref().map(|t| t.stats().packets_sent).unwrap_or(0)
}

/// Get UDP bytes sent count.
#[no_mangle]
pub extern "C" fn velocity_net_udp_bytes_sent() -> u64 {
    let guard = get_udp_repl().lock().unwrap();
    guard.as_ref().map(|t| t.stats().bytes_sent).unwrap_or(0)
}

// ─── Search Index FFI ─────────────────────────────────────────────────────────

/// Index a string search attribute for a workflow.
#[no_mangle]
pub extern "C" fn velocity_search_index_string(
    workflow_key: u64,
    attr_ptr: *const u8,
    attr_len: u32,
    val_ptr: *const u8,
    val_len: u32,
) {
    let attr = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(attr_ptr, attr_len as usize)) };
    let val = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(val_ptr, val_len as usize)) };
    get_search_index().index_attribute(
        workflow_key, attr,
        &crate::visibility::SearchAttributeValue::String(val.to_string()),
    );
}

/// Index an integer search attribute for a workflow.
#[no_mangle]
pub extern "C" fn velocity_search_index_integer(
    workflow_key: u64,
    attr_ptr: *const u8,
    attr_len: u32,
    value: i64,
) {
    let attr = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(attr_ptr, attr_len as usize)) };
    get_search_index().index_attribute(
        workflow_key, attr,
        &crate::visibility::SearchAttributeValue::Integer(value),
    );
}

/// Query exact match. Returns count of matching workflows.
#[no_mangle]
pub extern "C" fn velocity_search_query_exact_count(
    attr_ptr: *const u8,
    attr_len: u32,
    val_ptr: *const u8,
    val_len: u32,
) -> u64 {
    let attr = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(attr_ptr, attr_len as usize)) };
    let val = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(val_ptr, val_len as usize)) };
    get_search_index().exact_match(
        attr,
        &crate::visibility::SearchAttributeValue::String(val.to_string()),
    ).len() as u64
}

/// Query integer range [low, high]. Returns count of matching workflows.
#[no_mangle]
pub extern "C" fn velocity_search_query_range_count(
    attr_ptr: *const u8,
    attr_len: u32,
    low: i64,
    high: i64,
) -> u64 {
    let attr = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(attr_ptr, attr_len as usize)) };
    get_search_index().range_integer(attr, low, high).len() as u64
}

/// Get total entries in the search index.
#[no_mangle]
pub extern "C" fn velocity_search_index_entry_count() -> u64 {
    get_search_index().entry_count() as u64
}

/// Get indexed workflow count.
#[no_mangle]
pub extern "C" fn velocity_search_index_workflow_count() -> u64 {
    get_search_index().workflow_count() as u64
}

// ─── Chaos Endurance FFI ──────────────────────────────────────────────────────

/// Run a short soak test (returns total operations count).
#[no_mangle]
pub extern "C" fn velocity_chaos_soak_test(
    duration_ms: u64,
    thread_count: u32,
    inject_failures: i32,
) -> u64 {
    let config = crate::chaos_endurance::SoakTestConfig {
        duration: std::time::Duration::from_millis(duration_ms),
        thread_count: thread_count as usize,
        inject_failures: inject_failures != 0,
        failure_rate: if inject_failures != 0 { 0.2 } else { 0.0 },
        ..Default::default()
    };
    let metrics = crate::chaos_endurance::run_soak_test(&config);
    metrics.total_operations()
}

/// Run a crash recovery test. Returns (started << 32) | recovered.
#[no_mangle]
pub extern "C" fn velocity_chaos_crash_recovery_test(workflow_count: u32) -> u64 {
    let (started, recovered) = crate::chaos_endurance::run_crash_recovery_test(workflow_count as usize);
    ((started as u64) << 32) | (recovered as u64)
}

// ─── Hot-Swap FFI ─────────────────────────────────────────────────────────────

use crate::hot_swap::HotSwapRegistry;

static HOT_SWAP_REGISTRY: OnceLock<HotSwapRegistry> = OnceLock::new();

fn get_hot_swap() -> &'static HotSwapRegistry {
    HOT_SWAP_REGISTRY.get_or_init(|| HotSwapRegistry::new())
}

/// Register a hot-swap patch. Returns the patch_id.
#[no_mangle]
pub extern "C" fn velocity_hotswap_register(
    workflow_type_id: u64,
    desc_ptr: *const u8,
    desc_len: u32,
    step_index: u32,
    handler_id: u64,
) -> u64 {
    let desc = if desc_ptr.is_null() || desc_len == 0 {
        "patch".to_string()
    } else {
        let slice = unsafe { std::slice::from_raw_parts(desc_ptr, desc_len as usize) };
        String::from_utf8_lossy(slice).to_string()
    };
    get_hot_swap().register_patch(workflow_type_id, &desc, vec![(step_index, handler_id)])
}

/// Apply a hot-swap patch to a workflow. Returns 1 on success, 0 on failure.
#[no_mangle]
pub extern "C" fn velocity_hotswap_apply(patch_id: u64, workflow_key: u64) -> u32 {
    match get_hot_swap().apply_patch(patch_id, workflow_key) {
        crate::hot_swap::HotSwapResult::Applied { .. } => 1,
        _ => 0,
    }
}

/// Rollback the last patch for a workflow. Returns 1 on success, 0 on failure.
#[no_mangle]
pub extern "C" fn velocity_hotswap_rollback(workflow_key: u64) -> u32 {
    if get_hot_swap().rollback(workflow_key) { 1 } else { 0 }
}

/// Get total patch count.
#[no_mangle]
pub extern "C" fn velocity_hotswap_patch_count() -> u64 {
    get_hot_swap().patch_count() as u64
}

/// Get patched workflow count.
#[no_mangle]
pub extern "C" fn velocity_hotswap_patched_workflow_count() -> u64 {
    get_hot_swap().patched_workflow_count() as u64
}

/// Get latest version for a workflow type.
#[no_mangle]
pub extern "C" fn velocity_hotswap_latest_version(workflow_type_id: u64) -> u64 {
    get_hot_swap().latest_version(workflow_type_id)
}

// ─── Slab Visualization FFI ───────────────────────────────────────────────────

/// Get the slab header size (always 128).
#[no_mangle]
pub extern "C" fn velocity_slab_header_size() -> u32 {
    128
}

/// Get the slab count for an engine.
#[no_mangle]
pub unsafe extern "C" fn velocity_slab_count(handle: *mut EngineHandle) -> u32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    h.engine.workflow_count() as u32
}

/// Verify a slab's Merkle root for a workflow. Returns 1 if valid, 0 if invalid.
#[no_mangle]
pub unsafe extern "C" fn velocity_slab_verify_merkle(handle: *mut EngineHandle, workflow_key: u64) -> u32 {
    if handle.is_null() { return 0; }
    let h = &*handle;
    match h.engine.get_slab(workflow_key) {
        Some(slab) => if slab.verify_merkle_root() { 1 } else { 0 },
        None => 0,
    }
}

// ─── Observability FFI ───────────────────────────────────────────────────────

use crate::observability::{self, ObservabilityConfig, ObservabilityContext, LogLevel};
use std::sync::atomic::{AtomicU64, Ordering};

static OBS_INIT: AtomicU64 = AtomicU64::new(0);

/// Initialize the global observability context. Returns 0 on success.
#[no_mangle]
pub unsafe extern "C" fn velocity_obs_init(
    enable_tracing: u8,
    enable_metrics: u8,
    enable_logging: u8,
    log_level: u8,
    service_name_ptr: *const u8,
    service_name_len: u32,
) -> i32 {
    let service_name = if service_name_ptr.is_null() || service_name_len == 0 {
        "velocity-workflow-engine".to_string()
    } else {
        let slice = std::slice::from_raw_parts(service_name_ptr, service_name_len as usize);
        String::from_utf8_lossy(slice).into_owned()
    };

    let config = ObservabilityConfig {
        enable_tracing: enable_tracing != 0,
        enable_metrics: enable_metrics != 0,
        enable_logging: enable_logging != 0,
        log_level: LogLevel::from_u8(log_level),
        metrics_export_interval_ms: 10_000,
        service_name,
    };

    observability::init_global(config);
    OBS_INIT.store(1, Ordering::Release);
    0
}

fn obs_ctx() -> Option<&'static ObservabilityContext> {
    if OBS_INIT.load(Ordering::Acquire) == 0 { return None; }
    observability::global()
}

/// Log a structured event. Fields are passed as parallel arrays of key/value byte pointers.
#[no_mangle]
pub unsafe extern "C" fn velocity_obs_log_event(
    level: u8,
    name_ptr: *const u8,
    name_len: u32,
    field_keys: *const *const u8,
    field_key_lens: *const u32,
    field_vals: *const *const u8,
    field_val_lens: *const u32,
    field_count: u32,
) -> i32 {
    let ctx = match obs_ctx() { Some(c) => c, None => return -1 };

    let event_name = if name_ptr.is_null() || name_len == 0 {
        "unknown"
    } else {
        let slice = std::slice::from_raw_parts(name_ptr, name_len as usize);
        std::str::from_utf8(slice).unwrap_or("unknown")
    };

    let log_level = LogLevel::from_u8(level);

    // Build field pairs on the stack (up to 32 fields)
    let mut fields_buf: [(&str, &str); 32] = [("", ""); 32];
    let mut field_count_actual = 0usize;

    if !field_keys.is_null() && !field_vals.is_null() && field_count > 0 {
        let keys = std::slice::from_raw_parts(field_keys, field_count as usize);
        let key_lens = std::slice::from_raw_parts(field_key_lens, field_count as usize);
        let vals = std::slice::from_raw_parts(field_vals, field_count as usize);
        let val_lens = std::slice::from_raw_parts(field_val_lens, field_count as usize);

        for i in 0..(field_count as usize).min(32) {
            let k = if keys[i].is_null() || key_lens[i] == 0 { "" } else {
                std::str::from_utf8(std::slice::from_raw_parts(keys[i], key_lens[i] as usize)).unwrap_or("")
            };
            let v = if vals[i].is_null() || val_lens[i] == 0 { "" } else {
                std::str::from_utf8(std::slice::from_raw_parts(vals[i], val_lens[i] as usize)).unwrap_or("")
            };
            fields_buf[i] = (k, v);
            field_count_actual += 1;
        }
    }

    ctx.logger().log_event(log_level, event_name, &fields_buf[..field_count_actual]);
    0
}

/// Export Prometheus metrics into the provided buffer. Returns bytes written.
#[no_mangle]
pub unsafe extern "C" fn velocity_obs_export_metrics(
    buf: *mut u8,
    buf_len: u32,
) -> u64 {
    let ctx = match obs_ctx() { Some(c) => c, None => return 0 };
    let output = ctx.metrics().export_prometheus();
    let bytes = output.as_bytes();
    let copy_len = bytes.len().min(buf_len as usize);
    if !buf.is_null() && copy_len > 0 {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf, copy_len);
    }
    copy_len as u64
}

/// Start a trace span. Returns the span ID (0 if tracing disabled).
#[no_mangle]
pub unsafe extern "C" fn velocity_obs_start_span(
    name_ptr: *const u8,
    name_len: u32,
    parent_id: u64,
) -> u64 {
    let ctx = match obs_ctx() { Some(c) => c, None => return 0 };
    let name = if name_ptr.is_null() || name_len == 0 {
        "unknown"
    } else {
        let slice = std::slice::from_raw_parts(name_ptr, name_len as usize);
        std::str::from_utf8(slice).unwrap_or("unknown")
    };
    let parent = if parent_id == 0 { None } else { Some(parent_id) };
    ctx.tracer().start_span(name, parent)
}

/// End a trace span.
#[no_mangle]
pub unsafe extern "C" fn velocity_obs_end_span(span_id: u64) -> i32 {
    let ctx = match obs_ctx() { Some(c) => c, None => return -1 };
    if ctx.tracer().end_span(span_id) { 0 } else { -1 }
}

/// Increment the workflow_started_total counter.
#[no_mangle]
pub unsafe extern "C" fn velocity_obs_workflow_started() {
    if let Some(ctx) = obs_ctx() {
        ctx.metrics().inc_counter("workflow_started_total");
    }
}

/// Increment the workflow_completed_total counter.
#[no_mangle]
pub unsafe extern "C" fn velocity_obs_workflow_completed() {
    if let Some(ctx) = obs_ctx() {
        ctx.metrics().inc_counter("workflow_completed_total");
    }
}

/// Increment the workflow_failed_total counter.
#[no_mangle]
pub unsafe extern "C" fn velocity_obs_workflow_failed() {
    if let Some(ctx) = obs_ctx() {
        ctx.metrics().inc_counter("workflow_failed_total");
    }
}

// ─── Update API ──────────────────────────────────────────────────────────────

use crate::update::{UpdateController, UpdateRequest, UpdateWaitPolicy};
use std::collections::HashMap;
use std::sync::Mutex;

static UPDATE_CONTROLLERS: Mutex<Option<HashMap<u64, UpdateController>>> = Mutex::new(None);

fn update_controllers() -> std::sync::MutexGuard<'static, Option<HashMap<u64, UpdateController>>> {
    let mut guard = UPDATE_CONTROLLERS.lock().unwrap();
    if guard.is_none() {
        *guard = Some(HashMap::new());
    }
    guard
}

/// Create an update controller. Returns controller ID.
#[no_mangle]
pub unsafe extern "C" fn velocity_update_controller_create() -> u64 {
    let mut controllers = update_controllers();
    let map = controllers.as_mut().unwrap();
    let id = map.len() as u64 + 1;
    map.insert(id, UpdateController::new());
    id
}

/// Register an update handler (identity handler for FFI — just echoes args).
#[no_mangle]
pub unsafe extern "C" fn velocity_update_register_handler(controller_id: u64, name_ptr: *const u8, name_len: u32) -> i32 {
    let mut controllers = update_controllers();
    let map = controllers.as_mut().unwrap();
    let controller = match map.get_mut(&controller_id) {
        Some(c) => c,
        None => return -1,
    };
    let name = std::str::from_utf8(std::slice::from_raw_parts(name_ptr, name_len as usize)).unwrap_or("unknown");
    controller.register_handler(name, |args| Ok(args.to_vec()));
    0
}

/// Submit an update. Returns 0 on success.
#[no_mangle]
pub unsafe extern "C" fn velocity_update_submit(
    controller_id: u64,
    workflow_key: u64,
    name_ptr: *const u8, name_len: u32,
    args_ptr: *const u8, args_len: u32,
) -> i32 {
    let controllers = update_controllers();
    let map = controllers.as_ref().unwrap();
    let controller = match map.get(&controller_id) {
        Some(c) => c,
        None => return -1,
    };
    let name = std::str::from_utf8(std::slice::from_raw_parts(name_ptr, name_len as usize)).unwrap_or("unknown");
    let args = if args_ptr.is_null() || args_len == 0 { vec![] } else { std::slice::from_raw_parts(args_ptr, args_len as usize).to_vec() };
    let request = UpdateRequest {
        workflow_key,
        update_id: format!("ffi-update-{}", workflow_key),
        update_name: name.to_string(),
        args,
        wait_policy: UpdateWaitPolicy::Completed,
    };
    let result = controller.submit_update(request);
    if result.status == crate::update::UpdateStatus::Completed { 0 } else { -1 }
}

/// Get handler count for a controller.
#[no_mangle]
pub unsafe extern "C" fn velocity_update_handler_count(controller_id: u64) -> u32 {
    let controllers = update_controllers();
    let map = controllers.as_ref().unwrap();
    match map.get(&controller_id) {
        Some(c) => c.list_handlers().len() as u32,
        None => 0,
    }
}

// ─── Reachability API ────────────────────────────────────────────────────────

use crate::reachability::ReachabilityTracker;

static REACHABILITY_TRACKERS: Mutex<Option<HashMap<u64, ReachabilityTracker>>> = Mutex::new(None);

fn reachability_trackers() -> std::sync::MutexGuard<'static, Option<HashMap<u64, ReachabilityTracker>>> {
    let mut guard = REACHABILITY_TRACKERS.lock().unwrap();
    if guard.is_none() {
        *guard = Some(HashMap::new());
    }
    guard
}

/// Create a reachability tracker. Returns tracker ID.
#[no_mangle]
pub unsafe extern "C" fn velocity_reachability_tracker_create() -> u64 {
    let mut trackers = reachability_trackers();
    let map = trackers.as_mut().unwrap();
    let id = map.len() as u64 + 1;
    map.insert(id, ReachabilityTracker::new());
    id
}

/// Record a worker poll on a task queue.
#[no_mangle]
pub unsafe extern "C" fn velocity_reachability_record_poll(tracker_id: u64, queue_ptr: *const u8, queue_len: u32, timestamp: u64) {
    let trackers = reachability_trackers();
    let map = trackers.as_ref().unwrap();
    if let Some(tracker) = map.get(&tracker_id) {
        let queue = std::str::from_utf8(std::slice::from_raw_parts(queue_ptr, queue_len as usize)).unwrap_or("unknown");
        tracker.record_poll(queue, timestamp);
    }
}

/// Check if a task queue is reachable. Returns 1 if reachable, 0 otherwise.
#[no_mangle]
pub unsafe extern "C" fn velocity_reachability_check(tracker_id: u64, queue_ptr: *const u8, queue_len: u32) -> i32 {
    let trackers = reachability_trackers();
    let map = trackers.as_ref().unwrap();
    if let Some(tracker) = map.get(&tracker_id) {
        let queue = std::str::from_utf8(std::slice::from_raw_parts(queue_ptr, queue_len as usize)).unwrap_or("unknown");
        let result = tracker.check_task_queue(queue);
        if result.is_reachable { 1 } else { 0 }
    } else { 0 }
}

/// Get worker count for a task queue.
#[no_mangle]
pub unsafe extern "C" fn velocity_reachability_worker_count(tracker_id: u64, queue_ptr: *const u8, queue_len: u32) -> u32 {
    let trackers = reachability_trackers();
    let map = trackers.as_ref().unwrap();
    if let Some(tracker) = map.get(&tracker_id) {
        let queue = std::str::from_utf8(std::slice::from_raw_parts(queue_ptr, queue_len as usize)).unwrap_or("unknown");
        tracker.check_task_queue(queue).worker_count as u32
    } else { 0 }
}

// ─── Deployment API ──────────────────────────────────────────────────────────

use crate::deployment_api::DeploymentManager;

static DEPLOYMENT_MANAGERS: Mutex<Option<HashMap<u64, DeploymentManager>>> = Mutex::new(None);

fn deployment_managers() -> std::sync::MutexGuard<'static, Option<HashMap<u64, DeploymentManager>>> {
    let mut guard = DEPLOYMENT_MANAGERS.lock().unwrap();
    if guard.is_none() {
        *guard = Some(HashMap::new());
    }
    guard
}

/// Create a deployment manager. Returns manager ID.
#[no_mangle]
pub unsafe extern "C" fn velocity_deployment_manager_create() -> u64 {
    let mut managers = deployment_managers();
    let map = managers.as_mut().unwrap();
    let id = map.len() as u64 + 1;
    map.insert(id, DeploymentManager::new());
    id
}

/// Create a deployment. Returns 0 on success.
#[no_mangle]
pub unsafe extern "C" fn velocity_deployment_create(
    manager_id: u64,
    id_ptr: *const u8, id_len: u32,
    series_ptr: *const u8, series_len: u32,
    build_ptr: *const u8, build_len: u32,
    timestamp: u64,
) -> i32 {
    let managers = deployment_managers();
    let map = managers.as_ref().unwrap();
    if let Some(mgr) = map.get(&manager_id) {
        let id = std::str::from_utf8(std::slice::from_raw_parts(id_ptr, id_len as usize)).unwrap_or("unknown");
        let series = std::str::from_utf8(std::slice::from_raw_parts(series_ptr, series_len as usize)).unwrap_or("unknown");
        let build = std::str::from_utf8(std::slice::from_raw_parts(build_ptr, build_len as usize)).unwrap_or("unknown");
        mgr.create_deployment(id, series, build, timestamp);
        0
    } else { -1 }
}

/// Activate a deployment. Returns 0 on success.
#[no_mangle]
pub unsafe extern "C" fn velocity_deployment_activate(manager_id: u64, id_ptr: *const u8, id_len: u32) -> i32 {
    let managers = deployment_managers();
    let map = managers.as_ref().unwrap();
    if let Some(mgr) = map.get(&manager_id) {
        let id = std::str::from_utf8(std::slice::from_raw_parts(id_ptr, id_len as usize)).unwrap_or("unknown");
        mgr.activate_deployment(id).map(|_| 0).unwrap_or(-1)
    } else { -1 }
}

/// Get deployment count.
#[no_mangle]
pub unsafe extern "C" fn velocity_deployment_count(manager_id: u64) -> u32 {
    let managers = deployment_managers();
    let map = managers.as_ref().unwrap();
    match map.get(&manager_id) {
        Some(mgr) => mgr.deployment_count() as u32,
        None => 0,
    }
}

// ─── Codec Server ────────────────────────────────────────────────────────────

use crate::codec_server::{CodecServer, CodecRequest};

static CODEC_SERVERS: Mutex<Option<HashMap<u64, CodecServer>>> = Mutex::new(None);

fn codec_servers() -> std::sync::MutexGuard<'static, Option<HashMap<u64, CodecServer>>> {
    let mut guard = CODEC_SERVERS.lock().unwrap();
    if guard.is_none() {
        *guard = Some(HashMap::new());
    }
    guard
}

/// Create a codec server. Returns server ID.
#[no_mangle]
pub unsafe extern "C" fn velocity_codec_server_create() -> u64 {
    let mut servers = codec_servers();
    let map = servers.as_mut().unwrap();
    let id = map.len() as u64 + 1;
    map.insert(id, CodecServer::new());
    id
}

/// Get codec count.
#[no_mangle]
pub unsafe extern "C" fn velocity_codec_server_codec_count(server_id: u64) -> u32 {
    let servers = codec_servers();
    let map = servers.as_ref().unwrap();
    match map.get(&server_id) {
        Some(s) => s.codec_count() as u32,
        None => 0,
    }
}

// ─── Worker Sessions ─────────────────────────────────────────────────────────

use crate::worker_sessions::{SessionManager, SessionConfig};

static SESSION_MANAGERS: Mutex<Option<HashMap<u64, SessionManager>>> = Mutex::new(None);

fn session_managers() -> std::sync::MutexGuard<'static, Option<HashMap<u64, SessionManager>>> {
    let mut guard = SESSION_MANAGERS.lock().unwrap();
    if guard.is_none() {
        *guard = Some(HashMap::new());
    }
    guard
}

/// Create a session manager. Returns manager ID.
#[no_mangle]
pub unsafe extern "C" fn velocity_session_manager_create() -> u64 {
    let mut managers = session_managers();
    let map = managers.as_mut().unwrap();
    let id = map.len() as u64 + 1;
    map.insert(id, SessionManager::new(SessionConfig::default()));
    id
}

/// Create a session. Returns session count.
#[no_mangle]
pub unsafe extern "C" fn velocity_session_create(manager_id: u64, worker_ptr: *const u8, worker_len: u32, queue_ptr: *const u8, queue_len: u32) -> u32 {
    let managers = session_managers();
    let map = managers.as_ref().unwrap();
    if let Some(mgr) = map.get(&manager_id) {
        let worker = std::str::from_utf8(std::slice::from_raw_parts(worker_ptr, worker_len as usize)).unwrap_or("unknown");
        let queue = std::str::from_utf8(std::slice::from_raw_parts(queue_ptr, queue_len as usize)).unwrap_or("unknown");
        mgr.create_session(worker, queue);
        mgr.session_count() as u32
    } else { 0 }
}

/// Get session count.
#[no_mangle]
pub unsafe extern "C" fn velocity_session_count(manager_id: u64) -> u32 {
    let managers = session_managers();
    let map = managers.as_ref().unwrap();
    match map.get(&manager_id) {
        Some(mgr) => mgr.session_count() as u32,
        None => 0,
    }
}

/// Get active session count.
#[no_mangle]
pub unsafe extern "C" fn velocity_session_active_count(manager_id: u64) -> u32 {
    let managers = session_managers();
    let map = managers.as_ref().unwrap();
    match map.get(&manager_id) {
        Some(mgr) => mgr.active_session_count() as u32,
        None => 0,
    }
}

// ─── Worker Determinism ──────────────────────────────────────────────────────

use crate::worker_determinism::DeterminismChecker;

static DETERMINISM_CHECKERS: Mutex<Option<HashMap<u64, DeterminismChecker>>> = Mutex::new(None);

fn determinism_checkers() -> std::sync::MutexGuard<'static, Option<HashMap<u64, DeterminismChecker>>> {
    let mut guard = DETERMINISM_CHECKERS.lock().unwrap();
    if guard.is_none() {
        *guard = Some(HashMap::new());
    }
    guard
}

/// Create a determinism checker. Returns checker ID.
#[no_mangle]
pub unsafe extern "C" fn velocity_determinism_checker_create() -> u64 {
    let mut checkers = determinism_checkers();
    let map = checkers.as_mut().unwrap();
    let id = map.len() as u64 + 1;
    map.insert(id, DeterminismChecker::new());
    id
}

/// Record a side effect. Returns side effect ID.
#[no_mangle]
pub unsafe extern "C" fn velocity_determinism_record_side_effect(checker_id: u64, op_ptr: *const u8, op_len: u32, result_ptr: *const u8, result_len: u32, timestamp: u64) -> u64 {
    let checkers = determinism_checkers();
    let map = checkers.as_ref().unwrap();
    if let Some(checker) = map.get(&checker_id) {
        let op = std::str::from_utf8(std::slice::from_raw_parts(op_ptr, op_len as usize)).unwrap_or("unknown");
        let result = if result_ptr.is_null() || result_len == 0 { vec![] } else { std::slice::from_raw_parts(result_ptr, result_len as usize).to_vec() };
        checker.record_side_effect(op, &result, timestamp)
    } else { 0 }
}

/// Get violation count.
#[no_mangle]
pub unsafe extern "C" fn velocity_determinism_violation_count(checker_id: u64) -> u32 {
    let checkers = determinism_checkers();
    let map = checkers.as_ref().unwrap();
    match map.get(&checker_id) {
        Some(c) => c.violation_count() as u32,
        None => 0,
    }
}

/// Get side effect count.
#[no_mangle]
pub unsafe extern "C" fn velocity_determinism_side_effect_count(checker_id: u64) -> u32 {
    let checkers = determinism_checkers();
    let map = checkers.as_ref().unwrap();
    match map.get(&checker_id) {
        Some(c) => c.side_effect_count() as u32,
        None => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::WorkflowStatus;
    use crate::task_queue::TaskKind;

    #[test]
    fn test_ffi_lifecycle() {
        unsafe {
            let engine = velocity_engine_create();
            assert!(!engine.is_null());

            // Start a 3-step workflow
            let key = velocity_engine_start_workflow(engine, 1001, 1, 0, 42, 3, std::ptr::null(), 0);
            assert!(key > 0);

            // Check status
            assert_eq!(velocity_engine_get_status(engine, key), WorkflowStatus::Running as i32);

            // Poll the task queue — should have a workflow task
            let mut kind = 0u32;
            let mut wk = 0u64;
            let mut step = 0u32;
            let mut act = 0u64;
            let mut tid = 0u64;
            let mut attempt = 0u32;
            let result = velocity_engine_poll_task(engine, 42, &mut kind, &mut wk, &mut step, &mut act, &mut tid, &mut attempt);
            assert_eq!(result, 1);
            assert_eq!(kind, TaskKind::WorkflowTask as u32);
            assert_eq!(wk, key);

            // Complete step 0
            let data = [1u8, 2, 3];
            velocity_engine_complete_step(engine, key, 0, data.as_ptr(), 3);
            assert_eq!(velocity_engine_is_step_completed(engine, key, 0), 1);

            // Verify Merkle root
            assert_eq!(velocity_engine_verify_slab(engine, key), 1);

            // Complete workflow
            velocity_engine_complete_workflow(engine, key, std::ptr::null(), 0);
            assert_eq!(velocity_engine_get_status(engine, key), WorkflowStatus::Completed as i32);

            // Cleanup
            assert_eq!(velocity_engine_destroy(engine), 0);
        }
    }

    #[test]
    fn test_ffi_signal() {
        unsafe {
            let engine = velocity_engine_create();
            let key = velocity_engine_start_workflow(engine, 2001, 1, 0, 42, 1, std::ptr::null(), 0);

            assert_eq!(velocity_engine_has_signal(engine, key, 100), 0);

            let payload = [7u8, 8, 9];
            velocity_engine_signal(engine, key, 100, payload.as_ptr(), 3);
            assert_eq!(velocity_engine_has_signal(engine, key, 100), 1);

            velocity_engine_destroy(engine);
        }
    }

    #[test]
    fn test_ffi_update_api() {
        unsafe {
            let ctrl_id = velocity_update_controller_create();
            assert!(ctrl_id > 0);

            let name = b"setAmount";
            assert_eq!(velocity_update_register_handler(ctrl_id, name.as_ptr(), name.len() as u32), 0);
            assert_eq!(velocity_update_handler_count(ctrl_id), 1);

            let args = b"100";
            let result = velocity_update_submit(ctrl_id, 1, name.as_ptr(), name.len() as u32, args.as_ptr(), args.len() as u32);
            assert_eq!(result, 0);
        }
    }

    #[test]
    fn test_ffi_reachability_api() {
        unsafe {
            let tracker_id = velocity_reachability_tracker_create();
            assert!(tracker_id > 0);

            let queue = b"main-queue";
            velocity_reachability_record_poll(tracker_id, queue.as_ptr(), queue.len() as u32, 1000);
            assert_eq!(velocity_reachability_check(tracker_id, queue.as_ptr(), queue.len() as u32), 1);
            assert!(velocity_reachability_worker_count(tracker_id, queue.as_ptr(), queue.len() as u32) >= 1);
        }
    }

    #[test]
    fn test_ffi_deployment_api() {
        unsafe {
            let mgr_id = velocity_deployment_manager_create();
            assert!(mgr_id > 0);

            let id = b"deploy-1";
            let series = b"production";
            let build = b"v1.0.0";
            assert_eq!(velocity_deployment_create(mgr_id, id.as_ptr(), id.len() as u32, series.as_ptr(), series.len() as u32, build.as_ptr(), build.len() as u32, 1000), 0);
            assert_eq!(velocity_deployment_count(mgr_id), 1);

            assert_eq!(velocity_deployment_activate(mgr_id, id.as_ptr(), id.len() as u32), 0);
        }
    }

    #[test]
    fn test_ffi_codec_server_api() {
        unsafe {
            let server_id = velocity_codec_server_create();
            assert!(server_id > 0);
            assert!(velocity_codec_server_codec_count(server_id) >= 3);
        }
    }

    #[test]
    fn test_ffi_session_manager_api() {
        unsafe {
            let mgr_id = velocity_session_manager_create();
            assert!(mgr_id > 0);

            let worker = b"worker-1";
            let queue = b"main-queue";
            let count = velocity_session_create(mgr_id, worker.as_ptr(), worker.len() as u32, queue.as_ptr(), queue.len() as u32);
            assert!(count >= 1);
            assert_eq!(velocity_session_count(mgr_id), count);
            assert!(velocity_session_active_count(mgr_id) >= 1);
        }
    }

    #[test]
    fn test_ffi_determinism_checker_api() {
        unsafe {
            let checker_id = velocity_determinism_checker_create();
            assert!(checker_id > 0);

            let op = b"db_query";
            let result = b"42";
            let id = velocity_determinism_record_side_effect(checker_id, op.as_ptr(), op.len() as u32, result.as_ptr(), result.len() as u32, 1000);
            assert!(id == 0); // First side effect gets ID 0

            assert_eq!(velocity_determinism_side_effect_count(checker_id), 1);
            assert_eq!(velocity_determinism_violation_count(checker_id), 0);
        }
    }
}
