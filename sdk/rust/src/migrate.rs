//! Velocity Rust Migration Tool
//!
//! Scans a Rust codebase for Temporal, Restate, or DBOS workflow patterns
//! and converts them to Velocity Rust SDK workflows.
//!
//! Usage:
//!   cargo run --bin velocity-migrate -- --src ./my_project --from temporal
//!   cargo run --bin velocity-migrate -- --detect ./my_project

use std::fs;
use std::path::{Path, PathBuf};

/// A migration pattern with source regex and target replacement.
struct MigrationPattern {
    name: &'static str,
    source_pattern: &'static str,  // simple string match (regex crate optional)
    target_template: &'static str,
    source_framework: &'static str,
}

// ─── Temporal → Velocity Patterns ──────────────────────────────────────────

fn temporal_patterns() -> Vec<MigrationPattern> {
    vec![
        // Dependency/import replacements
        MigrationPattern {
            name: "temporal-dep-tokio",
            source_pattern: "temporal-client",
            target_template: "velocity-sdk",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-use-workflow",
            source_pattern: "use temporal_client::",
            target_template: "use velocity_sdk::",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-use-worker",
            source_pattern: "use temporal_worker::",
            target_template: "use velocity_sdk::worker::",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-use-activity",
            source_pattern: "use temporal_sdk::activity",
            target_template: "use velocity_sdk::activity",
            source_framework: "temporal",
        },

        // Function signature replacements
        MigrationPattern {
            name: "temporal-workflow-fn",
            source_pattern: "async fn workflow_fn",
            target_template: "async fn workflow_fn",
            source_framework: "temporal",
        },

        // Method call replacements
        MigrationPattern {
            name: "temporal-execute-activity",
            source_pattern: "ctx.execute_activity(",
            target_template: "ctx.execute_activity(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-sleep",
            source_pattern: "tokio::time::sleep(",
            target_template: "ctx.sleep(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-signal",
            source_pattern: "ctx.make_signal_channel(",
            target_template: "ctx.signal_channel(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-side-effect",
            source_pattern: "ctx.side_effect(",
            target_template: "ctx.side_effect(",
            source_framework: "temporal",
        },

        // Worker/Client
        MigrationPattern {
            name: "temporal-client-new",
            source_pattern: "Client::new(",
            target_template: "VelocityClient::new(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-worker-new",
            source_pattern: "Worker::new(",
            target_template: "VelocityWorker::new(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-register-workflow",
            source_pattern: "worker.register_workflow(",
            target_template: "worker.register_workflow(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-register-activity",
            source_pattern: "worker.register_activity(",
            target_template: "worker.register_activity(",
            source_framework: "temporal",
        },
        // Search attributes
        MigrationPattern {
            name: "temporal-search-attributes",
            source_pattern: "workflow::search_attributes(",
            target_template: "ctx.search_attributes(",
            source_framework: "temporal",
        },
        // Memo
        MigrationPattern {
            name: "temporal-memo",
            source_pattern: "workflow::memo(",
            target_template: "ctx.memo(",
            source_framework: "temporal",
        },
        // Update handler
        MigrationPattern {
            name: "temporal-update-handler",
            source_pattern: "#[temporal::update]",
            target_template: "#[velocity::update]",
            source_framework: "temporal",
        },
        // Continue-as-new
        MigrationPattern {
            name: "temporal-continue-as-new",
            source_pattern: "workflow::continue_as_new(",
            target_template: "ctx.continue_as_new(",
            source_framework: "temporal",
        },
        // ─── Child Workflow Patterns ─────────────────────────────────────────────
        MigrationPattern {
            name: "temporal-execute-child-workflow",
            source_pattern: "workflow::execute_child_workflow(",
            target_template: "ctx.execute_child_workflow(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-child-workflow-options",
            source_pattern: "ChildWorkflowOptions::new(",
            target_template: "ChildWorkflowOptions::new(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-child-workflow-future",
            source_pattern: "ChildWorkflowFuture<",
            target_template: "ChildWorkflowFuture<",
            source_framework: "temporal",
        },
        // ─── Activity Options Patterns ───────────────────────────────────────────
        MigrationPattern {
            name: "temporal-activity-options",
            source_pattern: "ActivityOptions::new(",
            target_template: "ActivityOptions::new(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-execute-local-activity",
            source_pattern: "workflow::execute_local_activity(",
            target_template: "ctx.execute_local_activity(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-local-activity-options",
            source_pattern: "LocalActivityOptions::new(",
            target_template: "LocalActivityOptions::new(",
            source_framework: "temporal",
        },
        // ─── Coroutine & Concurrency Patterns ────────────────────────────────────
        MigrationPattern {
            name: "temporal-tokio-spawn",
            source_pattern: "tokio::spawn(",
            target_template: "ctx.spawn(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-workflow-await",
            source_pattern: "workflow::await(",
            target_template: "ctx.await(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-workflow-await-with-timeout",
            source_pattern: "workflow::await_with_timeout(",
            target_template: "ctx.await_with_timeout(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-future",
            source_pattern: "Future<",
            target_template: "Future<",
            source_framework: "temporal",
        },
        // ─── Relay/Nexus Operation Patterns ──────────────────────────────────────
        MigrationPattern {
            name: "temporal-new-nexus-client",
            source_pattern: "workflow::new_nexus_client(",
            target_template: "ctx.new_relay_client(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-nexus-execute-operation",
            source_pattern: "nexus_client.execute_operation(",
            target_template: "relay_client.execute(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-nexus-operation-options",
            source_pattern: "NexusOperationOptions::new(",
            target_template: "RelayOperationOptions::new(",
            source_framework: "temporal",
        },
        // ─── Activity Context Patterns ───────────────────────────────────────────
        MigrationPattern {
            name: "temporal-activity-get-info",
            source_pattern: "activity::info(",
            target_template: "ctx.info(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-activity-record-heartbeat",
            source_pattern: "activity::heartbeat(",
            target_template: "ctx.heartbeat(",
            source_framework: "temporal",
        },
        // ─── Workflow Context Patterns ───────────────────────────────────────────
        MigrationPattern {
            name: "temporal-workflow-get-info",
            source_pattern: "workflow::info(",
            target_template: "ctx.workflow_info(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-workflow-get-logger",
            source_pattern: "workflow::logger(",
            target_template: "ctx.logger(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-workflow-with-cancel",
            source_pattern: "workflow::with_cancel(",
            target_template: "ctx.with_cancel(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-signal-external-workflow",
            source_pattern: "workflow::signal_external_workflow(",
            target_template: "ctx.signal_external_workflow(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-workflow-get-version",
            source_pattern: "workflow::get_version(",
            target_template: "ctx.get_version(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-upsert-search-attributes",
            source_pattern: "workflow::upsert_search_attributes(",
            target_template: "ctx.upsert_search_attributes(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-upsert-memo",
            source_pattern: "workflow::upsert_memo(",
            target_template: "ctx.upsert_memo(",
            source_framework: "temporal",
        },
        // ─── Error Handling Patterns ─────────────────────────────────────────────
        MigrationPattern {
            name: "temporal-new-application-error",
            source_pattern: "ApplicationError::new(",
            target_template: "VelocityApplicationError::new(",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-canceled-error",
            source_pattern: "CanceledError",
            target_template: "VelocityCanceledError",
            source_framework: "temporal",
        },
        MigrationPattern {
            name: "temporal-import-nexus-package",
            source_pattern: "use temporal_nexus::",
            target_template: "use velocity_sdk::relay::",
            source_framework: "temporal",
        },
    ]
}

// ─── Restate → Velocity Patterns ───────────────────────────────────────────

fn restate_patterns() -> Vec<MigrationPattern> {
    vec![
        MigrationPattern {
            name: "restate-dep",
            source_pattern: "restate-sdk",
            target_template: "velocity-sdk",
            source_framework: "restate",
        },
        MigrationPattern {
            name: "restate-use",
            source_pattern: "use restate_sdk::",
            target_template: "use velocity_sdk::",
            source_framework: "restate",
        },
        MigrationPattern {
            name: "restate-ctx-run",
            source_pattern: "ctx.run(",
            target_template: "ctx.execute_activity(",
            source_framework: "restate",
        },
        MigrationPattern {
            name: "restate-ctx-sleep",
            source_pattern: "ctx.sleep(",
            target_template: "ctx.sleep(",
            source_framework: "restate",
        },
        MigrationPattern {
            name: "restate-ctx-get",
            source_pattern: "ctx.get::<",
            target_template: "ctx.get_state::<",
            source_framework: "restate",
        },
        MigrationPattern {
            name: "restate-ctx-set",
            source_pattern: "ctx.set(",
            target_template: "ctx.set_state(",
            source_framework: "restate",
        },
        MigrationPattern {
            name: "restate-service-handler",
            source_pattern: "#[restate::handler]",
            target_template: "#[velocity::workflow]",
            source_framework: "restate",
        },
        // Idempotency key
        MigrationPattern {
            name: "restate-idempotency-key",
            source_pattern: "ctx.idempotency_key()",
            target_template: "ctx.idempotency_key()",
            source_framework: "restate",
        },
        // Service client
        MigrationPattern {
            name: "restate-service-client",
            source_pattern: "restate::Client::new(",
            target_template: "velocity_sdk::Client::new(",
            source_framework: "restate",
        },
    ]
}

// ─── DBOS → Velocity Patterns ──────────────────────────────────────────────

fn dbos_patterns() -> Vec<MigrationPattern> {
    vec![
        MigrationPattern {
            name: "dbos-dep",
            source_pattern: "dbos-sdk",
            target_template: "velocity-sdk",
            source_framework: "dbos",
        },
        MigrationPattern {
            name: "dbos-use",
            source_pattern: "use dbos::",
            target_template: "use velocity_sdk::",
            source_framework: "dbos",
        },
        MigrationPattern {
            name: "dbos-workflow-attr",
            source_pattern: "#[dbos::workflow]",
            target_template: "#[velocity::workflow]",
            source_framework: "dbos",
        },
        MigrationPattern {
            name: "dbos-transaction-attr",
            source_pattern: "#[dbos::transaction]",
            target_template: "#[velocity::activity]",
            source_framework: "dbos",
        },
        MigrationPattern {
            name: "dbos-sleep",
            source_pattern: "dbos::sleep(",
            target_template: "ctx.sleep(",
            source_framework: "dbos",
        },
        MigrationPattern {
            name: "dbos-recv",
            source_pattern: "dbos::recv(",
            target_template: "ctx.recv(",
            source_framework: "dbos",
        },
        MigrationPattern {
            name: "dbos-set-event",
            source_pattern: "dbos::set_event(",
            target_template: "ctx.set_event(",
            source_framework: "dbos",
        },
        MigrationPattern {
            name: "dbos-get-event",
            source_pattern: "dbos::get_event(",
            target_template: "ctx.get_event(",
            source_framework: "dbos",
        },
        // Queue operations
        MigrationPattern {
            name: "dbos-queue-enqueue",
            source_pattern: "dbos::enqueue(",
            target_template: "ctx.enqueue(",
            source_framework: "dbos",
        },
        MigrationPattern {
            name: "dbos-queue-dequeue",
            source_pattern: "dbos::dequeue(",
            target_template: "ctx.dequeue(",
            source_framework: "dbos",
        },
        // HTTP handler
        MigrationPattern {
            name: "dbos-http-handler",
            source_pattern: "#[dbos::http_handler]",
            target_template: "#[velocity::http_handler]",
            source_framework: "dbos",
        },
    ]
}

// ─── Framework Detection ─────────────────────────────────────────────────────

pub struct DetectionResult {
    pub framework: String,
    pub confidence: f64,
    pub evidence: Vec<String>,
}

pub fn detect_framework(content: &str) -> DetectionResult {
    let mut scores: Vec<(&str, i32, Vec<String>)> = vec![
        ("temporal", 0, vec![]),
        ("restate", 0, vec![]),
        ("dbos", 0, vec![]),
    ];

    // Temporal checks
    if content.contains("temporal-client") || content.contains("temporal_client") {
        scores[0].1 += 3;
        scores[0].2.push("Temporal SDK dependency".into());
    }
    if content.contains("temporal_sdk") {
        scores[0].1 += 2;
        scores[0].2.push("temporal_sdk import".into());
    }
    if content.contains("ctx.execute_activity") {
        scores[0].1 += 1;
        scores[0].2.push("execute_activity call".into());
    }
    if content.contains("workflow::search_attributes") || content.contains("workflow::continue_as_new") {
        scores[0].1 += 1;
        scores[0].2.push("Temporal advanced workflow API".into());
    }
    if content.contains("#[temporal::update]") {
        scores[0].1 += 1;
        scores[0].2.push("Temporal update handler".into());
    }

    // Restate checks
    if content.contains("restate-sdk") || content.contains("restate_sdk") {
        scores[1].1 += 3;
        scores[1].2.push("Restate SDK dependency".into());
    }
    if content.contains("#[restate::") {
        scores[1].1 += 2;
        scores[1].2.push("Restate attribute".into());
    }
    if content.contains("ctx.run(") {
        scores[1].1 += 1;
        scores[1].2.push("ctx.run() call".into());
    }
    if content.contains("ctx.idempotency_key") || content.contains("restate::Client") {
        scores[1].1 += 1;
        scores[1].2.push("Restate idempotency/client".into());
    }

    // DBOS checks
    if content.contains("dbos-sdk") || content.contains("use dbos::") {
        scores[2].1 += 3;
        scores[2].2.push("DBOS SDK dependency".into());
    }
    if content.contains("#[dbos::") {
        scores[2].1 += 2;
        scores[2].2.push("DBOS attribute".into());
    }
    if content.contains("dbos::sleep") {
        scores[2].1 += 1;
        scores[2].2.push("dbos::sleep call".into());
    }
    if content.contains("dbos::enqueue") || content.contains("#[dbos::http_handler]") {
        scores[2].1 += 1;
        scores[2].2.push("DBOS queue/HTTP handler".into());
    }

    // Find best match
    let (best_fw, best_score, best_evidence) = scores
        .iter()
        .max_by_key(|(_, score, _)| *score)
        .map(|(fw, score, ev)| (*fw, *score, ev.clone()))
        .unwrap_or(("temporal", 0, vec![]));

    let total: i32 = scores.iter().map(|(_, s, _)| s).sum();
    let confidence = if total > 0 {
        best_score as f64 / total as f64
    } else {
        0.0
    };

    DetectionResult {
        framework: best_fw.to_string(),
        confidence,
        evidence: best_evidence,
    }
}

// ─── File Migration ──────────────────────────────────────────────────────────

pub struct FileResult {
    pub source_path: String,
    pub success: bool,
    pub error: Option<String>,
    pub detected_framework: String,
    pub transformations: usize,
}

pub fn migrate_file(content: &str, source_framework: &str) -> (String, FileResult) {
    let mut result = FileResult {
        source_path: String::new(),
        success: true,
        error: None,
        detected_framework: String::new(),
        transformations: 0,
    };

    let framework = if source_framework == "auto" {
        let detection = detect_framework(content);
        result.detected_framework = detection.framework.clone();
        if detection.confidence < 0.3 {
            result.success = false;
            result.error = Some(format!(
                "Low confidence: {} ({:.2})",
                detection.framework, detection.confidence
            ));
            return (content.to_string(), result);
        }
        detection.framework
    } else {
        result.detected_framework = source_framework.to_string();
        source_framework.to_string()
    };

    let patterns = match framework.as_str() {
        "temporal" => temporal_patterns(),
        "restate" => restate_patterns(),
        "dbos" => dbos_patterns(),
        _ => {
            result.success = false;
            result.error = Some(format!("Unknown framework: {}", framework));
            return (content.to_string(), result);
        }
    };

    let mut migrated = content.to_string();
    let mut count = 0;
    for p in &patterns {
        if migrated.contains(p.source_pattern) {
            migrated = migrated.replace(p.source_pattern, p.target_template);
            count += 1;
        }
    }
    result.transformations = count;

    (migrated, result)
}

// ─── Project Scanner ─────────────────────────────────────────────────────────

const SKIP_DIRS: &[&str] = &["target", ".git", "node_modules"];

pub fn scan_rust_files(root_dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    scan_dir(root_dir, &mut files);
    files
}

fn scan_dir(dir: &Path, files: &mut Vec<PathBuf>) {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let name = entry.file_name().to_string_lossy().to_string();
            if SKIP_DIRS.contains(&name.as_str()) {
                continue;
            }
            scan_dir(&path, files);
        } else if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == "rs" {
                    files.push(path);
                }
            }
        }
    }
}

pub fn has_workflow_content(content: &str) -> bool {
    let indicators = [
        "temporal", "restate", "dbos",
        "execute_activity", "ctx.run(",
        "#[restate::", "#[dbos::",
    ];
    indicators.iter().any(|i| content.contains(i))
}

// ─── CLI Entry Point ─────────────────────────────────────────────────────────

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut src: Option<String> = None;
    let mut from = "auto".to_string();
    let mut output: Option<String> = None;
    let mut dry_run = false;
    let mut detect = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--src" => { i += 1; src = args.get(i).cloned(); }
            "--from" => { i += 1; from = args.get(i).cloned().unwrap_or("auto".into()); }
            "--output" | "-o" => { i += 1; output = args.get(i).cloned(); }
            "--dry-run" => dry_run = true,
            "--detect" => detect = true,
            "--help" | "-h" => {
                println!("Velocity Rust Migration Tool\n");
                println!("Usage:");
                println!("  --src <file|dir>     Source file or directory");
                println!("  --from <framework>   Source: temporal, restate, dbos, auto");
                println!("  --output <path>      Output file or directory");
                println!("  --dry-run            Detect without writing");
                println!("  --detect             Detect framework in directory");
                return;
            }
            _ => {}
        }
        i += 1;
    }

    let src = src.unwrap_or_else(|| {
        eprintln!("Error: --src is required");
        std::process::exit(1);
    });

    // Mode: detect
    if detect {
        let src_path = Path::new(&src);
        if !src_path.is_dir() {
            eprintln!("Error: --detect requires a directory");
            std::process::exit(1);
        }
        let files = scan_rust_files(src_path);
        println!("Scanning {} Rust files in {}...", files.len(), src);
        for f in &files {
            if let Ok(content) = fs::read_to_string(f) {
                let d = detect_framework(&content);
                if d.confidence > 0.3 {
                    let rel = f.strip_prefix(src_path).unwrap_or(f);
                    println!(
                        "  {}: {} ({:.0}%) [{}]",
                        rel.display(),
                        d.framework,
                        d.confidence * 100.0,
                        d.evidence.join(", ")
                    );
                }
            }
        }
        return;
    }

    let src_path = Path::new(&src);

    // Mode: single file
    if src_path.is_file() {
        let content = fs::read_to_string(src_path).unwrap_or_else(|e| {
            eprintln!("Error reading file: {}", e);
            std::process::exit(1);
        });

        let (migrated, result) = migrate_file(&content, &from);
        if !result.success {
            eprintln!("Migration failed: {}", result.error.unwrap_or_default());
            std::process::exit(1);
        }

        if let Some(out) = output {
            fs::write(&out, &migrated).unwrap();
            println!("Written to: {}", out);
        } else {
            print!("{}", migrated);
        }
        println!("\nDetected: {}", result.detected_framework);
        println!("Transformations: {}", result.transformations);
        return;
    }

    // Mode: directory
    let output_dir = output.unwrap_or_else(|| {
        format!("{}/../velocity-migrated", src)
    });

    println!("Scanning: {}", src);
    println!("Output: {}", if dry_run { "(dry run)" } else { &output_dir });
    println!("Source framework: {}\n", from);

    let files = scan_rust_files(src_path);
    let mut migrated_count = 0;
    let mut failed_count = 0;
    let mut skipped_count = 0;

    for f in &files {
        let content = match fs::read_to_string(f) {
            Ok(c) => c,
            Err(_) => { failed_count += 1; continue; }
        };

        if !has_workflow_content(&content) {
            skipped_count += 1;
            continue;
        }

        let (migrated, result) = migrate_file(&content, &from);
        let rel = f.strip_prefix(src_path).unwrap_or(f);

        if result.success && !dry_run {
            let out_path = Path::new(&output_dir).join(rel);
            if let Some(parent) = out_path.parent() {
                fs::create_dir_all(parent).ok();
            }
            fs::write(&out_path, &migrated).ok();
            migrated_count += 1;
        } else if result.success {
            migrated_count += 1;
        } else {
            failed_count += 1;
        }

        let status = if result.success { "OK" } else { "FAIL" };
        println!(
            "  [{}] {} ({}, {} changes)",
            status,
            rel.display(),
            result.detected_framework,
            result.transformations
        );
    }

    println!("\nResults: {} files, {} migrated, {} failed, {} skipped",
        files.len(), migrated_count, failed_count, skipped_count);
}
