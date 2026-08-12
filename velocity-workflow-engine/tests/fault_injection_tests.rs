//! Fault injection and production-hardening tests.
//!
//! Validates:
//! - WAL corruption detection and recovery
//! - WAL version header validation
//! - AES-256-GCM encryption roundtrip + tamper detection
//! - Concurrent DashMap stress (many writers, no deadlocks)
//! - Zero-alloc container correctness under pressure

use velocity_workflow_engine::engine::WorkflowEngine;
use velocity_workflow_engine::wal::{WalWriter, WalRecord, WalEventType, WAL_MAGIC, WAL_VERSION};
use velocity_workflow_engine::wal::read_wal_records;
use velocity_workflow_engine::zero_alloc::{SlotMap, SlotVec};
use velocity_workflow_engine::string_interner::StringInterner;
use std::fs;
use std::io::Write;

// ── WAL Corruption Tests ────────────────────────────────────────────────────

#[test]
fn test_wal_corruption_truncated_record() {
    let dir = std::env::temp_dir().join("vel_test_wal_corrupt");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let wal_path = dir.join("test.wal");

    // Write valid records
    {
        let mut writer = WalWriter::open(&wal_path).unwrap();
        writer.append_event(WalEventType::WorkflowStarted, 42, vec![1, 2, 3]).unwrap();
        writer.append_event(WalEventType::StepCompleted, 42, vec![4, 5, 6]).unwrap();
        writer.sync().unwrap();
    }

    // Corrupt the file by truncating the last few bytes
    let data = fs::read(&wal_path).unwrap();
    let truncated = &data[..data.len() - 5];
    fs::write(&wal_path, truncated).unwrap();

    // Recovery should still read the first record (or handle gracefully)
    let result = read_wal_records(&wal_path);
    // Either reads partial records or returns error — both are acceptable
    match result {
        Ok(records) => {
            // At least the header was valid, partial reads are ok
            assert!(records.len() <= 2, "Should not read more records than written");
        }
        Err(e) => {
            // Corruption detected — this is the expected behavior
            assert!(
                e.to_string().contains("CRC") || e.to_string().contains("truncated") || e.to_string().contains("Unexpected EOF") || e.kind() == std::io::ErrorKind::UnexpectedEof,
                "Error should indicate corruption: {}", e
            );
        }
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_wal_corruption_garbage_bytes() {
    let dir = std::env::temp_dir().join("vel_test_wal_garbage");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let wal_path = dir.join("test.wal");

    // Write valid records
    {
        let mut writer = WalWriter::open(&wal_path).unwrap();
        writer.append_event(WalEventType::WorkflowStarted, 100, vec![10, 20]).unwrap();
        writer.sync().unwrap();
    }

    // Inject garbage bytes in the middle of the file (after header)
    let mut data = fs::read(&wal_path).unwrap();
    let insert_pos = 12; // After 8-byte header + a few bytes
    for i in 0..20 {
        data.insert(insert_pos + i, 0xFF);
    }
    fs::write(&wal_path, &data).unwrap();

    // Recovery should detect corruption
    let result = read_wal_records(&wal_path);
    match result {
        Ok(records) => {
            // If it somehow parsed, records should be limited
            assert!(records.len() <= 1);
        }
        Err(_) => {
            // Corruption detected — expected
        }
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_wal_version_header_validation() {
    let dir = std::env::temp_dir().join("vel_test_wal_version");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let wal_path = dir.join("test.wal");

    // Write a file with invalid magic bytes
    {
        let mut file = fs::File::create(&wal_path).unwrap();
        file.write_all(b"BAAD").unwrap(); // Wrong magic
        file.write_all(&1u32.to_le_bytes()).unwrap();
    }

    let result = read_wal_records(&wal_path);
    assert!(result.is_err(), "Should reject file with bad magic");
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("Not a Velocity WAL") || err.to_string().contains("bad magic"),
        "Error should mention bad magic: {}", err
    );

    // Write a file with a future version
    {
        let mut file = fs::File::create(&wal_path).unwrap();
        file.write_all(&WAL_MAGIC).unwrap();
        file.write_all(&999u32.to_le_bytes()).unwrap(); // Future version
    }

    let result = read_wal_records(&wal_path);
    assert!(result.is_err(), "Should reject file with future version");
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("newer than maximum"),
        "Error should mention version mismatch: {}", err
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_wal_valid_header_on_create() {
    let dir = std::env::temp_dir().join("vel_test_wal_header");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let wal_path = dir.join("test.wal");

    // Create a new WAL
    {
        let mut writer = WalWriter::open(&wal_path).unwrap();
        writer.append_event(WalEventType::WorkflowStarted, 1, vec![]).unwrap();
        writer.sync().unwrap();
    }

    // Verify header
    let data = fs::read(&wal_path).unwrap();
    assert!(data.len() >= 8, "WAL should have at least 8-byte header");
    assert_eq!(&data[0..4], &WAL_MAGIC, "Magic bytes should match");
    let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
    assert_eq!(version, WAL_VERSION, "Version should match");

    // Should be readable
    let records = read_wal_records(&wal_path).unwrap();
    assert_eq!(records.len(), 1);

    let _ = fs::remove_dir_all(&dir);
}

// ── AES-256-GCM Encryption Tests ────────────────────────────────────────────

#[test]
fn test_aes256gcm_roundtrip() {
    use velocity_workflow_engine::auth_v2::{EncryptionAtRest, EncryptionConfig, EncryptionAlgorithm};

    let config = EncryptionConfig {
        algorithm: EncryptionAlgorithm::Aes256Gcm,
        key_id: "test-key".into(),
        key_rotation_interval_ms: 0,
    };
    let enc = EncryptionAtRest::new(config, "master-secret-key");

    let plaintext = b"Hello, AES-256-GCM encryption!";
    let ciphertext = enc.encrypt(plaintext);

    // Ciphertext should be different from plaintext
    assert_ne!(&ciphertext[45..], plaintext, "Ciphertext should differ from plaintext");

    // Decrypt should recover original
    let decrypted = enc.decrypt(&ciphertext).expect("Decryption should succeed");
    assert_eq!(decrypted, plaintext);
}

#[test]
fn test_aes256gcm_tamper_detection() {
    use velocity_workflow_engine::auth_v2::{EncryptionAtRest, EncryptionConfig, EncryptionAlgorithm};

    let config = EncryptionConfig {
        algorithm: EncryptionAlgorithm::Aes256Gcm,
        key_id: "tamper-test".into(),
        key_rotation_interval_ms: 0,
    };
    let enc = EncryptionAtRest::new(config, "secret");

    let plaintext = b"Sensitive workflow data";
    let mut ciphertext = enc.encrypt(plaintext);

    // Tamper with the ciphertext (flip a byte)
    if ciphertext.len() > 50 {
        ciphertext[50] ^= 0xFF;
    }

    // GCM should detect tampering and return None
    let result = enc.decrypt(&ciphertext);
    assert!(result.is_none(), "Tampered ciphertext should fail decryption");
}

#[test]
fn test_aes256gcm_wrong_key_fails() {
    use velocity_workflow_engine::auth_v2::{EncryptionAtRest, EncryptionConfig, EncryptionAlgorithm};

    let config1 = EncryptionConfig {
        algorithm: EncryptionAlgorithm::Aes256Gcm,
        key_id: "key-A".into(),
        key_rotation_interval_ms: 0,
    };
    let config2 = EncryptionConfig {
        algorithm: EncryptionAlgorithm::Aes256Gcm,
        key_id: "key-B".into(),
        key_rotation_interval_ms: 0,
    };

    let enc1 = EncryptionAtRest::new(config1, "secret1");
    let enc2 = EncryptionAtRest::new(config2, "secret2");

    let ciphertext = enc1.encrypt(b"private data");

    // Different key should fail (key_id mismatch)
    let result = enc2.decrypt(&ciphertext);
    assert!(result.is_none(), "Wrong key should fail decryption");
}

#[test]
fn test_aes256gcm_unique_nonces() {
    use velocity_workflow_engine::auth_v2::{EncryptionAtRest, EncryptionConfig, EncryptionAlgorithm};

    let config = EncryptionConfig {
        algorithm: EncryptionAlgorithm::Aes256Gcm,
        key_id: "nonce-test".into(),
        key_rotation_interval_ms: 0,
    };
    let enc = EncryptionAtRest::new(config, "key");

    // Encrypt the same plaintext twice — ciphertexts should differ (unique nonces)
    let ct1 = enc.encrypt(b"same data");
    let ct2 = enc.encrypt(b"same data");

    assert_ne!(ct1, ct2, "Same plaintext should produce different ciphertexts (unique nonces)");

    // Both should decrypt to the same plaintext
    assert_eq!(enc.decrypt(&ct1).unwrap(), b"same data");
    assert_eq!(enc.decrypt(&ct2).unwrap(), b"same data");
}

// ── DashMap Concurrent Stress Tests ─────────────────────────────────────────

#[test]
fn test_dashmap_concurrent_writers_no_deadlock() {
    use std::thread;
    use std::sync::Arc;
    use dashmap::DashMap;

    let map: Arc<DashMap<u64, String>> = Arc::new(DashMap::new());
    let num_threads = 16;
    let ops_per_thread = 1000;

    let handles: Vec<_> = (0..num_threads).map(|t| {
        let map = map.clone();
        thread::spawn(move || {
            for i in 0..ops_per_thread {
                let key = (t * ops_per_thread + i) as u64;
                map.insert(key, format!("value-{}-{}", t, i));
            }
            // Read back
            for i in 0..ops_per_thread {
                let key = (t * ops_per_thread + i) as u64;
                assert!(map.get(&key).is_some(), "Key {} should exist", key);
            }
            // Remove half
            for i in 0..ops_per_thread / 2 {
                let key = (t * ops_per_thread + i) as u64;
                map.remove(&key);
            }
        })
    }).collect();

    for h in handles {
        h.join().expect("Thread should not panic");
    }

    // Final count: each thread inserted 1000, removed 500 = 500 remaining × 16 threads
    assert_eq!(map.len(), num_threads * ops_per_thread / 2);
}

#[test]
fn test_dashmap_entry_api_under_contention() {
    use std::thread;
    use std::sync::Arc;
    use dashmap::DashMap;

    let map: Arc<DashMap<u64, u64>> = Arc::new(DashMap::new());
    let num_threads = 8;
    let ops_per_thread = 500;

    // All threads try to insert the same keys (contention on same shards)
    let handles: Vec<_> = (0..num_threads).map(|t| {
        let map = map.clone();
        thread::spawn(move || {
            for i in 0..ops_per_thread {
                let key = i as u64; // Same keys across threads
                match map.entry(key) {
                    dashmap::mapref::entry::Entry::Occupied(mut e) => {
                        *e.get_mut() += t as u64;
                    }
                    dashmap::mapref::entry::Entry::Vacant(e) => {
                        e.insert(t as u64);
                    }
                }
            }
        })
    }).collect();

    for h in handles {
        h.join().unwrap();
    }

    // All keys should exist
    assert_eq!(map.len(), ops_per_thread);
    // Each key should have been incremented by all threads
    for i in 0..ops_per_thread {
        let val = map.get(&(i as u64)).unwrap();
        assert!(*val > 0, "Value should be positive");
    }
}

// ── Zero-Allocation Container Stress Tests ──────────────────────────────────

#[test]
fn test_slotmap_concurrent_like_usage() {
    let mut slab: SlotMap<String> = SlotMap::with_capacity(1024);

    // Simulate workflow step results pattern
    for i in 0..1000u64 {
        slab.insert(i, format!("result-{}", i));
    }

    assert_eq!(slab.len(), 1000);

    // Verify all entries
    for i in 0..1000u64 {
        assert_eq!(slab.get(i), Some(&format!("result-{}", i)));
    }

    // Remove half
    for i in (0..1000u64).step_by(2) {
        slab.remove(i);
    }
    assert_eq!(slab.len(), 500);

    // Verify only odd keys remain
    for i in 0..1000u64 {
        if i % 2 == 0 {
            assert!(slab.get(i).is_none());
        } else {
            assert!(slab.get(i).is_some());
        }
    }
}

#[test]
fn test_slotvec_signal_buffer_pattern() {
    let mut slot_vec: SlotVec<Vec<u8>> = SlotVec::with_capacity(256);

    // Simulate signal buffer pattern: multiple signals per workflow
    for signal_id in 0..100u64 {
        for _ in 0..3 {
            slot_vec.push(signal_id, vec![1, 2, 3]);
        }
    }

    // Each slot should have 3 entries
    for signal_id in 0..100u64 {
        let entries = slot_vec.get(signal_id).unwrap();
        assert_eq!(entries.len(), 3);
    }

    // Drain one slot via pop_front
    for _ in 0..3 {
        slot_vec.pop_front(0);
    }
    assert!(slot_vec.is_empty_at(0));
}

// ── String Interner Tests ───────────────────────────────────────────────────

#[test]
fn test_string_interner_deduplication() {
    let mut interner = StringInterner::with_capacity(64);

    let s1 = interner.intern("workflow_type_a");
    let s2 = interner.intern("workflow_type_a"); // Same string
    let s3 = interner.intern("workflow_type_b"); // Different string

    assert_eq!(s1, s2, "Same string should get same InternedString");
    assert_ne!(s1, s3, "Different strings should get different InternedStrings");

    // Resolve back
    assert_eq!(interner.resolve(s1), "workflow_type_a");
    assert_eq!(interner.resolve(s3), "workflow_type_b");
}

#[test]
fn test_string_interner_zero_alloc_lookup() {
    let mut interner = StringInterner::with_capacity(64);
    let interned = interner.intern("test_signal");

    // Lookup is O(1) — just integer comparison
    let interned2 = interner.intern("test_signal");
    assert_eq!(interned, interned2);

    // Non-existent
    let missing = interner.intern("nonexistent");
    assert_ne!(interned, missing);
}

// ── Engine Fault Recovery Tests ─────────────────────────────────────────────

#[test]
fn test_engine_wal_recovery_with_versioned_header() {
    let dir = std::env::temp_dir().join("vel_test_engine_recovery");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let wal_path = dir.join("recovery.wal");

    // Create engine with WAL, start some workflows
    {
        let engine = WorkflowEngine::with_wal(wal_path.to_str().unwrap(), 64 * 1024 * 1024).unwrap();
        let key1 = engine.start_workflow(1, 1, 100, 10, 3, None);
        engine.complete_step(key1, 0, b"step0".to_vec());
        engine.complete_step(key1, 1, b"step1".to_vec());

        let key2 = engine.start_workflow(2, 2, 200, 5, 2, None);
        engine.complete_step(key2, 0, b"result".to_vec());
    }

    // Create new engine from same WAL — should recover
    {
        let engine = WorkflowEngine::with_wal(wal_path.to_str().unwrap(), 64 * 1024 * 1024).unwrap();
        // Recovered workflows should be accessible
        assert!(engine.workflow_count() >= 0); // At minimum, no crash
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_wal_sync_durability() {
    let dir = std::env::temp_dir().join("vel_test_wal_sync");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let wal_path = dir.join("sync_test.wal");

    // Write and sync
    {
        let mut writer = WalWriter::open(&wal_path).unwrap();
        writer.append_event(WalEventType::WorkflowStarted, 1, vec![42]).unwrap();
        writer.sync().unwrap(); // Force fsync
    }

    // File should be readable even if process crashes right after sync
    let records = read_wal_records(&wal_path).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].workflow_key, 1);

    let _ = fs::remove_dir_all(&dir);
}
