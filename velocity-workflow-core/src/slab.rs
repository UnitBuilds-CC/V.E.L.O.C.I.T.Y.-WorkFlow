//! Repr(C) binary slab layout with chained SHA-256 Merkle-root state verification.
//!
//! Each step's Merkle root hashes over the previous root, creating a tamper-evident
//! chain analogous to a blockchain. Crash recovery verifies integrity by walking
//! the chain from genesis to current state.

use crate::bitmask::Bitmask256;
use sha2::{Digest, Sha256};

pub const MAGIC_VLCT: u32 = 0x564C4354; // "VLCT" in ASCII
pub const CURRENT_SCHEMA_VERSION: u32 = 1;
pub const SLAB_HEADER_SIZE: usize = 128;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct SlabHeader {
    pub magic: u32,                 // 4 Bytes: "VLCT"
    pub schema_version: u32,        // 4 Bytes: Version ID
    pub workflow_id: u64,           // 8 Bytes: Unique workflow instance ID
    pub run_id: u64,                // 8 Bytes: Unique run ID
    pub current_step: u32,          // 4 Bytes: Current step index
    pub total_steps: u32,           // 4 Bytes: Total planned steps
    pub merkle_root: [u8; 32],      // 32 Bytes: Cryptographic SHA-256 state proof (chained)
    pub step_bitmask: Bitmask256,   // 32 Bytes: O(1) step completion flags
    pub prev_merkle_root: [u8; 32], // 32 Bytes: Previous step's Merkle root (chain link)
}

impl SlabHeader {
    pub fn new(workflow_id: u64, run_id: u64, total_steps: u32) -> Self {
        let mut header = Self {
            magic: MAGIC_VLCT,
            schema_version: CURRENT_SCHEMA_VERSION,
            workflow_id,
            run_id,
            current_step: 0,
            total_steps,
            merkle_root: [0u8; 32],
            step_bitmask: Bitmask256::new(),
            prev_merkle_root: [0u8; 32], // Genesis step: no previous root
        };
        header.recalculate_merkle_root();
        header
    }

    /// Create a slab header chained to a previous step's Merkle root.
    pub fn new_chained(workflow_id: u64, run_id: u64, total_steps: u32, prev_root: [u8; 32]) -> Self {
        let mut header = Self {
            magic: MAGIC_VLCT,
            schema_version: CURRENT_SCHEMA_VERSION,
            workflow_id,
            run_id,
            current_step: 0,
            total_steps,
            merkle_root: [0u8; 32],
            step_bitmask: Bitmask256::new(),
            prev_merkle_root: prev_root,
        };
        header.recalculate_merkle_root();
        header
    }

    pub fn is_valid(&self) -> bool {
        self.magic == MAGIC_VLCT
    }

    pub fn recalculate_merkle_root(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.magic.to_le_bytes());
        hasher.update(self.schema_version.to_le_bytes());
        hasher.update(self.workflow_id.to_le_bytes());
        hasher.update(self.run_id.to_le_bytes());
        hasher.update(self.current_step.to_le_bytes());
        hasher.update(self.total_steps.to_le_bytes());
        // Chain link: hash the previous step's Merkle root into this one
        hasher.update(self.prev_merkle_root);
        for word in &self.step_bitmask.bits {
            hasher.update(word.to_le_bytes());
        }
        let result = hasher.finalize();
        self.merkle_root.copy_from_slice(&result);
    }

    pub fn verify_merkle_root(&self) -> bool {
        let mut hasher = Sha256::new();
        hasher.update(self.magic.to_le_bytes());
        hasher.update(self.schema_version.to_le_bytes());
        hasher.update(self.workflow_id.to_le_bytes());
        hasher.update(self.run_id.to_le_bytes());
        hasher.update(self.current_step.to_le_bytes());
        hasher.update(self.total_steps.to_le_bytes());
        // Chain link: verify includes previous step's Merkle root
        hasher.update(self.prev_merkle_root);
        for word in &self.step_bitmask.bits {
            hasher.update(word.to_le_bytes());
        }
        let result = hasher.finalize();
        self.merkle_root == result.as_slice()
    }

    pub fn mark_step_completed(&mut self, step_index: usize) -> bool {
        if self.step_bitmask.set_step(step_index) {
            // Save current root as previous before recalculating (chain link)
            self.prev_merkle_root = self.merkle_root;
            self.current_step = (step_index + 1) as u32;
            self.recalculate_merkle_root();
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slab_header_merkle_verification() {
        let mut header = SlabHeader::new(1001, 5001, 10);
        assert!(header.is_valid());
        assert!(header.verify_merkle_root());
        // Genesis step: prev_merkle_root is all zeros
        assert_eq!(header.prev_merkle_root, [0u8; 32]);

        let genesis_root = header.merkle_root;
        header.mark_step_completed(0);
        assert!(header.verify_merkle_root());
        assert_eq!(header.current_step, 1);
        assert!(header.step_bitmask.is_step_set(0));
        // Chain link: prev_merkle_root should be the genesis root
        assert_eq!(header.prev_merkle_root, genesis_root);
        // Current root should differ from genesis (chain advanced)
        assert_ne!(header.merkle_root, genesis_root);

        // Verify chaining: step 2's prev should be step 1's root
        let step1_root = header.merkle_root;
        header.mark_step_completed(1);
        assert!(header.verify_merkle_root());
        assert_eq!(header.prev_merkle_root, step1_root);

        // Corrupt header — tampering is cryptographically detectable
        header.current_step = 99;
        assert!(!header.verify_merkle_root());
    }

    #[test]
    fn test_slab_header_chain_integrity() {
        // Verify that the Merkle chain forms an unbroken sequence
        let mut header = SlabHeader::new(42, 100, 5);
        let mut roots = vec![header.merkle_root];

        for step in 0..5 {
            header.mark_step_completed(step);
            assert!(header.verify_merkle_root(), "Chain broken at step {}", step);
            assert_eq!(header.prev_merkle_root, roots[step]);
            roots.push(header.merkle_root);
        }

        // All roots in the chain should be unique
        for i in 0..roots.len() {
            for j in (i+1)..roots.len() {
                assert_ne!(roots[i], roots[j], "Duplicate roots at {} and {}", i, j);
            }
        }
    }

    #[test]
    fn test_slab_header_chained_constructor() {
        let prev_root = [0xABu8; 32];
        let header = SlabHeader::new_chained(1001, 5001, 10, prev_root);
        assert!(header.is_valid());
        assert!(header.verify_merkle_root());
        assert_eq!(header.prev_merkle_root, prev_root);
    }
}
