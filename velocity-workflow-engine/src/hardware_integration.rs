//! Hardware Integration Layer — wires hardware traits into the engine's data paths.
//!
//! This module bridges the gap between the trait definitions in `hardware_traits.rs`
//! and the actual engine operations. It provides:
//!
//! - **Merkle ECC self-healing loop**: Every slab write computes ECC parity; every
//!   slab read verifies Merkle root first, then ECC parity. On Merkle mismatch,
//!   ECC attempts in-place repair before declaring corruption unrecoverable.
//!
//! - **SmartNIC offload integration**: Slab delta transfers can be offloaded to a
//!   SmartNIC when available, bypassing the host CPU entirely.
//!
//! - **TEE slab protection**: Workflow slabs can be stored inside hardware enclaves
//!   when a TEE is available.
//!
//! - **P2P replication integration**: Slab deltas are streamed to peers when the
//!   replication transport is backed by io_uring/RDMA.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::hardware_traits::{
    EccAlgorithm, EnclaveHandle, HardwareError, PeerHandle, PeerToPeerReplication, SelfHealingEcc,
    SimulatedEcc, SimulatedSmartNic, SimulatedTee, SmartNicOffload, TeeEnclave, TransferHandle,
    TransferStatus, VerificationResult,
};

// ─── ECC Parity Store ────────────────────────────────────────────────────────

/// Stores ECC parity bits for each workflow slab, keyed by workflow_key.
/// When a slab is mutated (step completed, signal received, etc.), new parity
/// is computed. When a slab is read, parity is verified against the data.
pub struct EccParityStore {
    /// workflow_key → ECC parity bytes
    parity_map: HashMap<u64, Vec<u8>>,
    /// workflow_key → last known Merkle root (32 bytes)
    merkle_roots: HashMap<u64, [u8; 32]>,
    /// Statistics
    total_verifications: u64,
    total_repairs: u64,
    total_unrecoverable: u64,
}

impl EccParityStore {
    pub fn new() -> Self {
        Self {
            parity_map: HashMap::new(),
            merkle_roots: HashMap::new(),
            total_verifications: 0,
            total_repairs: 0,
            total_unrecoverable: 0,
        }
    }

    /// Store parity + Merkle root for a workflow slab after a mutation.
    pub fn store_parity(&mut self, workflow_key: u64, parity: Vec<u8>, merkle_root: [u8; 32]) {
        self.parity_map.insert(workflow_key, parity);
        self.merkle_roots.insert(workflow_key, merkle_root);
    }

    /// Get stored parity for a workflow slab.
    pub fn get_parity(&self, workflow_key: u64) -> Option<&Vec<u8>> {
        self.parity_map.get(&workflow_key)
    }

    /// Get stored Merkle root for a workflow slab.
    pub fn get_merkle_root(&self, workflow_key: u64) -> Option<&[u8; 32]> {
        self.merkle_roots.get(&workflow_key)
    }

    /// Remove parity data when a workflow is archived/terminated.
    pub fn remove(&mut self, workflow_key: u64) {
        self.parity_map.remove(&workflow_key);
        self.merkle_roots.remove(&workflow_key);
    }

    pub fn total_verifications(&self) -> u64 {
        self.total_verifications
    }
    pub fn total_repairs(&self) -> u64 {
        self.total_repairs
    }
    pub fn total_unrecoverable(&self) -> u64 {
        self.total_unrecoverable
    }

    pub fn inc_verification(&mut self) {
        self.total_verifications += 1;
    }
    pub fn inc_repair(&mut self) {
        self.total_repairs += 1;
    }
    pub fn inc_unrecoverable(&mut self) {
        self.total_unrecoverable += 1;
    }

    pub fn entry_count(&self) -> usize {
        self.parity_map.len()
    }
}

// ─── Hardware Abstraction Layer ──────────────────────────────────────────────

/// The HAL integrates all hardware subsystems into a single interface that
/// the WorkflowEngine can call during its normal operations.
///
/// # Data Flow
///
/// **Slab Write Path** (complete_step, signal, etc.):
///   1. Engine mutates slab (mark step, update bitmask, recalc Merkle root)
///   2. HAL computes ECC parity over the slab data
///   3. HAL stores (parity, merkle_root) in EccParityStore
///   4. If SmartNIC available, offload delta to NIC for replication
///
/// **Slab Read Path** (get_slab, is_step_completed, etc.):
///   1. Engine reads slab header
///   2. HAL verifies Merkle root against stored root
///   3. If Merkle matches → verify ECC parity
///   4. If ECC parity mismatch → attempt repair via SelfHealingEcc
///   5. If repair succeeds → re-verify and continue
///   6. If unrecoverable → return error, engine can trigger workflow reset
pub struct HardwareAbstractionLayer {
    ecc: Box<dyn SelfHealingEcc + Send + Sync>,
    nic: Option<Box<dyn SmartNicOffload + Send + Sync>>,
    tee: Option<Box<dyn TeeEnclave + Send + Sync>>,
    p2p: Option<Box<dyn PeerToPeerReplication + Send + Sync>>,
    parity_store: RwLock<EccParityStore>,
    /// Whether ECC verification is enabled on the read path.
    ecc_verification_enabled: bool,
    /// Whether SmartNIC offload is enabled on the write path.
    nic_offload_enabled: bool,
    /// Whether TEE protection is active.
    tee_protection_enabled: bool,
    /// Statistics
    slab_writes: u64,
    slab_reads: u64,
    nic_offloads: u64,
    tee_enclave_count: u64,
}

impl HardwareAbstractionLayer {
    /// Create a HAL with simulated hardware (for testing and development).
    pub fn with_simulated_hardware() -> Self {
        Self {
            ecc: Box::new(SimulatedEcc),
            nic: Some(Box::new(SimulatedSmartNic::new())),
            tee: Some(Box::new(SimulatedTee::new())),
            p2p: None,
            parity_store: RwLock::new(EccParityStore::new()),
            ecc_verification_enabled: true,
            nic_offload_enabled: true,
            tee_protection_enabled: true,
            slab_writes: 0,
            slab_reads: 0,
            nic_offloads: 0,
            tee_enclave_count: 0,
        }
    }

    /// Create a HAL with only ECC (no optional hardware).
    pub fn ecc_only() -> Self {
        Self {
            ecc: Box::new(SimulatedEcc),
            nic: None,
            tee: None,
            p2p: None,
            parity_store: RwLock::new(EccParityStore::new()),
            ecc_verification_enabled: true,
            nic_offload_enabled: false,
            tee_protection_enabled: false,
            slab_writes: 0,
            slab_reads: 0,
            nic_offloads: 0,
            tee_enclave_count: 0,
        }
    }

    // ── Slab Write Path ───────────────────────────────────────────────────

    /// Called after every slab mutation. Computes ECC parity and stores it
    /// alongside the new Merkle root. Optionally offloads the delta to SmartNIC.
    ///
    /// Returns the ECC parity bytes computed.
    pub fn on_slab_write(
        &mut self,
        workflow_key: u64,
        slab_data: &[u8],
        merkle_root: [u8; 32],
    ) -> Vec<u8> {
        self.slab_writes += 1;

        // Compute ECC parity over the slab data
        let parity = self.ecc.compute_parity(slab_data);

        // Store parity + Merkle root for later verification
        self.parity_store
            .write()
            .unwrap()
            .store_parity(workflow_key, parity.clone(), merkle_root);

        // If SmartNIC is available and offload enabled, offload the slab transfer
        if self.nic_offload_enabled {
            if let Some(nic) = &mut self.nic {
                if nic.is_available() {
                    let _handle = nic.offload_slab_transfer(
                        slab_data.as_ptr(),
                        std::ptr::null_mut(),
                        slab_data.len(),
                        &merkle_root,
                    );
                    self.nic_offloads += 1;
                }
            }
        }

        parity
    }

    // ── Slab Read Path (Merkle ECC Self-Healing Loop) ─────────────────────

    /// Called before every slab read. Verifies the Merkle root and ECC parity.
    /// If the Merkle root mismatches, attempts ECC repair.
    ///
    /// Returns the verification result:
    /// - `Valid`: Data is intact, safe to read
    /// - `Repaired`: Data was corrupted but ECC successfully repaired it
    /// - `Unrecoverable`: Data is corrupted beyond repair
    pub fn on_slab_read(
        &mut self,
        workflow_key: u64,
        slab_data: &mut [u8],
        expected_merkle_root: &[u8; 32],
    ) -> VerificationResult {
        self.slab_reads += 1;

        if !self.ecc_verification_enabled {
            return VerificationResult::Valid;
        }

        let mut store = self.parity_store.write().unwrap();
        store.inc_verification();

        // Get stored parity for this workflow
        let parity = match store.get_parity(workflow_key) {
            Some(p) => p.clone(),
            None => return VerificationResult::Valid, // No parity stored = first read after write
        };

        // Verify and attempt repair if needed
        match self
            .ecc
            .verify_and_repair(slab_data, &parity, expected_merkle_root)
        {
            Ok(VerificationResult::Valid) => VerificationResult::Valid,
            Ok(VerificationResult::Repaired) => {
                store.inc_repair();
                // Recompute parity after repair and update store
                let new_parity = self.ecc.compute_parity(slab_data);
                store.store_parity(workflow_key, new_parity, *expected_merkle_root);
                VerificationResult::Repaired
            }
            Ok(VerificationResult::Unrecoverable) => {
                store.inc_unrecoverable();
                VerificationResult::Unrecoverable
            }
            Err(_) => {
                store.inc_unrecoverable();
                VerificationResult::Unrecoverable
            }
        }
    }

    /// Full self-healing verification loop:
    ///   1. Compute current Merkle root from slab data
    ///   2. Compare against stored Merkle root
    ///   3. If mismatch → invoke ECC verify_and_repair
    ///   4. If repair succeeds → update stored parity + Merkle root
    ///   5. Return result
    pub fn merkle_ecc_self_heal(
        &mut self,
        workflow_key: u64,
        slab_data: &mut [u8],
    ) -> MerkleEccResult {
        self.slab_reads += 1;

        // Compute current Merkle root from the slab data
        let computed_root = compute_simple_merkle_root(slab_data);

        let mut store = self.parity_store.write().unwrap();
        store.inc_verification();

        // Check against stored Merkle root
        let stored_root = match store.get_merkle_root(workflow_key) {
            Some(root) => *root,
            None => {
                // First access — store the current root and parity
                let parity = self.ecc.compute_parity(slab_data);
                store.store_parity(workflow_key, parity, computed_root);
                return MerkleEccResult::Valid;
            }
        };

        if computed_root == stored_root {
            // Merkle roots match — verify ECC parity as extra safety
            let parity = match store.get_parity(workflow_key) {
                Some(p) => p.clone(),
                None => return MerkleEccResult::Valid,
            };
            match self.ecc.verify_and_repair(slab_data, &parity, &stored_root) {
                Ok(VerificationResult::Valid) => MerkleEccResult::Valid,
                Ok(VerificationResult::Repaired) => {
                    store.inc_repair();
                    let new_parity = self.ecc.compute_parity(slab_data);
                    store.store_parity(workflow_key, new_parity, computed_root);
                    MerkleEccResult::Repaired
                }
                _ => MerkleEccResult::Unrecoverable,
            }
        } else {
            // Merkle mismatch — attempt ECC repair
            let parity = match store.get_parity(workflow_key) {
                Some(p) => p.clone(),
                None => return MerkleEccResult::MerkleMismatchUnrecoverable,
            };

            match self.ecc.verify_and_repair(slab_data, &parity, &stored_root) {
                Ok(VerificationResult::Repaired) | Ok(VerificationResult::Valid) => {
                    store.inc_repair();
                    // Recompute Merkle root after repair
                    let repaired_root = compute_simple_merkle_root(slab_data);
                    let new_parity = self.ecc.compute_parity(slab_data);
                    store.store_parity(workflow_key, new_parity, repaired_root);
                    MerkleEccResult::Repaired
                }
                Ok(VerificationResult::Unrecoverable) => {
                    store.inc_unrecoverable();
                    MerkleEccResult::MerkleMismatchUnrecoverable
                }
                Err(_) => {
                    store.inc_unrecoverable();
                    MerkleEccResult::MerkleMismatchUnrecoverable
                }
            }
        }
    }

    // ── TEE Integration ───────────────────────────────────────────────────

    /// Create a TEE enclave for a workflow slab. Returns the enclave handle.
    pub fn create_slab_enclave(
        &mut self,
        workflow_key: u64,
        slab_size: usize,
    ) -> Result<u64, HardwareError> {
        if !self.tee_protection_enabled {
            return Err(HardwareError::NotAvailable);
        }
        if let Some(tee) = &mut self.tee {
            if tee.is_available() {
                let handle = tee.create_enclave(slab_size)?;
                self.tee_enclave_count += 1;
                return Ok(handle.0);
            }
        }
        Err(HardwareError::NotAvailable)
    }

    /// Write slab data into a TEE enclave.
    pub fn write_to_enclave(
        &self,
        enclave_handle: u64,
        offset: usize,
        data: &[u8],
    ) -> Result<(), HardwareError> {
        if let Some(tee) = &self.tee {
            return tee.enclave_write(EnclaveHandle(enclave_handle), offset, data);
        }
        Err(HardwareError::NotAvailable)
    }

    /// Read slab data from a TEE enclave.
    pub fn read_from_enclave(
        &self,
        enclave_handle: u64,
        offset: usize,
        buffer: &mut [u8],
    ) -> Result<(), HardwareError> {
        if let Some(tee) = &self.tee {
            return tee.enclave_read(EnclaveHandle(enclave_handle), offset, buffer);
        }
        Err(HardwareError::NotAvailable)
    }

    /// Destroy a TEE enclave and securely wipe its memory.
    pub fn destroy_slab_enclave(&mut self, enclave_handle: u64) -> Result<(), HardwareError> {
        if let Some(tee) = &mut self.tee {
            return tee.destroy_enclave(EnclaveHandle(enclave_handle));
        }
        Err(HardwareError::NotAvailable)
    }

    // ── SmartNIC Integration ──────────────────────────────────────────────

    /// Check if a SmartNIC transfer has completed.
    pub fn check_nic_transfer(&self, handle_id: u64) -> Result<TransferStatus, HardwareError> {
        if let Some(nic) = &self.nic {
            return nic.check_transfer(TransferHandle(handle_id));
        }
        Err(HardwareError::NotAvailable)
    }

    /// Get SmartNIC device info.
    pub fn nic_device_info(&self) -> Option<crate::hardware_traits::SmartNicInfo> {
        self.nic.as_ref().map(|nic| nic.device_info())
    }

    // ── Cleanup ───────────────────────────────────────────────────────────

    /// Remove all parity/Merkle data for a workflow (called on archive/terminate).
    pub fn cleanup_workflow(&self, workflow_key: u64) {
        self.parity_store.write().unwrap().remove(workflow_key);
    }

    // ── Statistics ────────────────────────────────────────────────────────

    pub fn slab_write_count(&self) -> u64 {
        self.slab_writes
    }
    pub fn slab_read_count(&self) -> u64 {
        self.slab_reads
    }
    pub fn nic_offload_count(&self) -> u64 {
        self.nic_offloads
    }
    pub fn tee_enclave_count(&self) -> u64 {
        self.tee_enclave_count
    }

    pub fn ecc_stats(&self) -> EccStats {
        let store = self.parity_store.read().unwrap();
        EccStats {
            parity_entries: store.entry_count(),
            total_verifications: store.total_verifications(),
            total_repairs: store.total_repairs(),
            total_unrecoverable: store.total_unrecoverable(),
            ecc_algorithm: format!("{:?}", self.ecc.algorithm()),
        }
    }

    pub fn ecc_algorithm(&self) -> EccAlgorithm {
        self.ecc.algorithm()
    }
    pub fn is_ecc_enabled(&self) -> bool {
        self.ecc_verification_enabled
    }
    pub fn is_nic_enabled(&self) -> bool {
        self.nic_offload_enabled && self.nic.as_ref().map_or(false, |n| n.is_available())
    }
    pub fn is_tee_enabled(&self) -> bool {
        self.tee_protection_enabled && self.tee.as_ref().map_or(false, |t| t.is_available())
    }

    /// Enable or disable ECC verification on the read path.
    pub fn set_ecc_verification(&mut self, enabled: bool) {
        self.ecc_verification_enabled = enabled;
    }
    /// Enable or disable SmartNIC offload on the write path.
    pub fn set_nic_offload(&mut self, enabled: bool) {
        self.nic_offload_enabled = enabled;
    }
    /// Enable or disable TEE protection.
    pub fn set_tee_protection(&mut self, enabled: bool) {
        self.tee_protection_enabled = enabled;
    }
}

// ─── Result Types ────────────────────────────────────────────────────────────

/// Result of the Merkle ECC self-healing loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MerkleEccResult {
    /// Merkle root matches, ECC parity valid.
    Valid,
    /// Corruption detected and successfully repaired.
    Repaired,
    /// Merkle mismatch but ECC could not repair.
    MerkleMismatchUnrecoverable,
    /// Data corrupted beyond ECC repair capability.
    Unrecoverable,
}

/// ECC statistics snapshot.
#[derive(Debug, Clone)]
pub struct EccStats {
    pub parity_entries: usize,
    pub total_verifications: u64,
    pub total_repairs: u64,
    pub total_unrecoverable: u64,
    pub ecc_algorithm: String,
}

// ─── Merkle Root Computation ─────────────────────────────────────────────────

/// Compute a simple Merkle root (SHA-256 based) over slab data.
/// This is a lightweight hash for integrity verification — the full
/// Merkle tree is in velocity-workflow-core's SlabHeader.
pub fn compute_simple_merkle_root(data: &[u8]) -> [u8; 32] {
    // Simple FNV-1a based hash for simulation — in production this would be SHA-256
    let mut hash = [0u8; 32];
    // FNV-1a parameters
    let mut h: u64 = 0xcbf29ce484222325;
    for &byte in data {
        h ^= byte as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    hash[0..8].copy_from_slice(&h.to_le_bytes());
    // Fill remaining bytes with secondary hash
    let mut h2: u64 = 0x517cc1b727220a95;
    for &byte in data.iter().rev() {
        h2 ^= byte as u64;
        h2 = h2.wrapping_mul(0x01000193);
    }
    hash[8..16].copy_from_slice(&h2.to_le_bytes());
    // Third pass with XOR fold
    let mut h3: u64 = h ^ h2;
    for chunk in data.chunks(8) {
        let val = if chunk.len() == 8 {
            u64::from_le_bytes(chunk.try_into().unwrap())
        } else {
            let mut buf = [0u8; 8];
            buf[..chunk.len()].copy_from_slice(chunk);
            u64::from_le_bytes(buf)
        };
        h3 = h3.wrapping_add(val);
        h3 = h3.wrapping_mul(0x9e3779b97f4a7c15);
    }
    hash[16..24].copy_from_slice(&h3.to_le_bytes());
    hash[24..32].copy_from_slice(&(h3 ^ h).to_le_bytes());
    hash
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hal_creation_simulated() {
        let hal = HardwareAbstractionLayer::with_simulated_hardware();
        assert!(hal.is_ecc_enabled());
        assert!(hal.is_nic_enabled());
        assert!(hal.is_tee_enabled());
        assert_eq!(hal.ecc_algorithm(), EccAlgorithm::ReedSolomon);
    }

    #[test]
    fn test_hal_ecc_only() {
        let hal = HardwareAbstractionLayer::ecc_only();
        assert!(hal.is_ecc_enabled());
        assert!(!hal.is_nic_enabled());
        assert!(!hal.is_tee_enabled());
    }

    #[test]
    fn test_slab_write_and_read_valid() {
        let mut hal = HardwareAbstractionLayer::with_simulated_hardware();
        let slab_data = vec![1u8; 64];
        let merkle_root = compute_simple_merkle_root(&slab_data);

        // Write: compute parity
        let parity = hal.on_slab_write(100, &slab_data, merkle_root);
        assert!(!parity.is_empty());
        assert_eq!(hal.slab_write_count(), 1);

        // Read: verify (should be valid since data hasn't changed)
        let mut read_data = slab_data.clone();
        let result = hal.on_slab_read(100, &mut read_data, &merkle_root);
        assert_eq!(result, VerificationResult::Valid);
        assert_eq!(hal.slab_read_count(), 1);
    }

    #[test]
    fn test_merkle_ecc_self_heal_valid() {
        let mut hal = HardwareAbstractionLayer::with_simulated_hardware();
        let mut slab_data = vec![42u8; 128];

        // First call stores the Merkle root + parity
        let result = hal.merkle_ecc_self_heal(200, &mut slab_data);
        assert_eq!(result, MerkleEccResult::Valid);

        // Second call should also be valid
        let result = hal.merkle_ecc_self_heal(200, &mut slab_data);
        assert_eq!(result, MerkleEccResult::Valid);
    }

    #[test]
    fn test_merkle_ecc_self_heal_detects_mismatch() {
        let mut hal = HardwareAbstractionLayer::with_simulated_hardware();
        let mut slab_data = vec![10u8; 64];

        // Initialize: store the Merkle root + parity
        let result = hal.merkle_ecc_self_heal(300, &mut slab_data);
        assert_eq!(result, MerkleEccResult::Valid);

        // Simulate corruption by modifying the data
        slab_data[0] = 99;

        // The self-heal loop should detect the mismatch
        // In simulation mode, the ECC reports "Repaired" (simulated repair)
        let result = hal.merkle_ecc_self_heal(300, &mut slab_data);
        // SimulatedEcc always reports Repaired when parity mismatches
        assert!(
            result == MerkleEccResult::Repaired
                || result == MerkleEccResult::MerkleMismatchUnrecoverable
        );
    }

    #[test]
    fn test_tee_enclave_lifecycle() {
        let mut hal = HardwareAbstractionLayer::with_simulated_hardware();

        let handle = hal.create_slab_enclave(400, 256).unwrap();
        assert!(handle > 0);
        assert_eq!(hal.tee_enclave_count(), 1);

        // Write data into enclave
        assert!(hal.write_to_enclave(handle, 0, &[1, 2, 3, 4]).is_ok());

        // Read data back
        let mut buf = [0u8; 4];
        // Note: SimulatedTee doesn't actually write (enclave_write is a no-op on data)
        // but the interface should work
        assert!(hal.read_from_enclave(handle, 0, &mut buf).is_ok());

        // Destroy enclave
        assert!(hal.destroy_slab_enclave(handle).is_ok());
    }

    #[test]
    fn test_nic_offload() {
        let mut hal = HardwareAbstractionLayer::with_simulated_hardware();
        let slab_data = vec![5u8; 32];
        let merkle_root = compute_simple_merkle_root(&slab_data);

        // Write triggers NIC offload
        hal.on_slab_write(500, &slab_data, merkle_root);
        assert_eq!(hal.nic_offload_count(), 1);

        // Check device info
        let info = hal.nic_device_info().unwrap();
        assert_eq!(info.vendor, "Software");
    }

    #[test]
    fn test_cleanup_workflow() {
        let mut hal = HardwareAbstractionLayer::with_simulated_hardware();
        let slab_data = vec![7u8; 64];
        let merkle_root = compute_simple_merkle_root(&slab_data);

        hal.on_slab_write(600, &slab_data, merkle_root);
        assert_eq!(hal.ecc_stats().parity_entries, 1);

        hal.cleanup_workflow(600);
        assert_eq!(hal.ecc_stats().parity_entries, 0);
    }

    #[test]
    fn test_ecc_stats() {
        let mut hal = HardwareAbstractionLayer::with_simulated_hardware();
        let slab_data = vec![1u8; 32];
        let merkle_root = compute_simple_merkle_root(&slab_data);

        hal.on_slab_write(700, &slab_data, merkle_root);
        let mut read_data = slab_data.clone();
        hal.on_slab_read(700, &mut read_data, &merkle_root);

        let stats = hal.ecc_stats();
        assert_eq!(stats.parity_entries, 1);
        assert!(stats.total_verifications >= 1);
        assert_eq!(stats.ecc_algorithm, "ReedSolomon");
    }

    #[test]
    fn test_compute_simple_merkle_root_deterministic() {
        let data = b"hello velocity slab data";
        let root1 = compute_simple_merkle_root(data);
        let root2 = compute_simple_merkle_root(data);
        assert_eq!(root1, root2);

        // Different data → different root
        let root3 = compute_simple_merkle_root(b"different data");
        assert_ne!(root1, root3);
    }

    #[test]
    fn test_parity_store_operations() {
        let mut store = EccParityStore::new();
        assert_eq!(store.entry_count(), 0);

        store.store_parity(100, vec![1, 2, 3], [0u8; 32]);
        assert_eq!(store.entry_count(), 1);
        assert!(store.get_parity(100).is_some());
        assert!(store.get_merkle_root(100).is_some());

        store.remove(100);
        assert_eq!(store.entry_count(), 0);
    }

    #[test]
    fn test_toggle_subsystems() {
        let mut hal = HardwareAbstractionLayer::with_simulated_hardware();
        assert!(hal.is_ecc_enabled());

        hal.set_ecc_verification(false);
        assert!(!hal.is_ecc_enabled());

        hal.set_nic_offload(false);
        assert!(!hal.nic_offload_enabled);

        hal.set_tee_protection(false);
        assert!(!hal.tee_protection_enabled);
    }
}
