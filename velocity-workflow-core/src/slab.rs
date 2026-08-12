//! Memory-mapped repr(C) binary slab layout with Merkle-root SHA-256 state verification.

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
    pub merkle_root: [u8; 32],      // 32 Bytes: Cryptographic SHA-256 state proof
    pub step_bitmask: Bitmask256,   // 32 Bytes: O(1) step completion flags
    pub reserved_padding: [u8; 32], // 32 Bytes: Reserved slot padding for schema migrations
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
            reserved_padding: [0u8; 32],
        };
        header.recalculate_merkle_root();
        header
    }

    pub fn is_valid(&self) -> bool {
        self.magic == MAGIC_VLCT
    }

    pub fn recalculate_merkle_root(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(&self.magic.to_le_bytes());
        hasher.update(&self.schema_version.to_le_bytes());
        hasher.update(&self.workflow_id.to_le_bytes());
        hasher.update(&self.run_id.to_le_bytes());
        hasher.update(&self.current_step.to_le_bytes());
        hasher.update(&self.total_steps.to_le_bytes());
        for word in &self.step_bitmask.bits {
            hasher.update(&word.to_le_bytes());
        }
        let result = hasher.finalize();
        self.merkle_root.copy_from_slice(&result);
    }

    pub fn verify_merkle_root(&self) -> bool {
        let mut hasher = Sha256::new();
        hasher.update(&self.magic.to_le_bytes());
        hasher.update(&self.schema_version.to_le_bytes());
        hasher.update(&self.workflow_id.to_le_bytes());
        hasher.update(&self.run_id.to_le_bytes());
        hasher.update(&self.current_step.to_le_bytes());
        hasher.update(&self.total_steps.to_le_bytes());
        for word in &self.step_bitmask.bits {
            hasher.update(&word.to_le_bytes());
        }
        let result = hasher.finalize();
        self.merkle_root == result.as_slice()
    }

    pub fn mark_step_completed(&mut self, step_index: usize) -> bool {
        if self.step_bitmask.set_step(step_index) {
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

        header.mark_step_completed(0);
        assert!(header.verify_merkle_root());
        assert_eq!(header.current_step, 1);
        assert!(header.step_bitmask.is_step_set(0));

        // Corrupt header
        header.current_step = 99;
        assert!(!header.verify_merkle_root());
    }
}
