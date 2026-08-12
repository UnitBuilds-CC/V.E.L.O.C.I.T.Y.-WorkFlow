//! Neural Document Architecture (NDA) binary schema integration for V.E.L.O.C.I.T.Y.-WorkFlow.

use sha2::{Digest, Sha256};

pub const NDA_MAGIC: u32 = 0x3141444E; // "NDA1" (0x3141444E)

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct NdaHeader {
    pub magic: u32,               // 4 Bytes: "NDA1"
    pub flags: u32,               // 4 Bytes: Config / bitmask flags
    pub merkle_root: [u8; 32],    // 32 Bytes: Cryptographic SHA-256 Merkle root
    pub triple_count: u32,        // 4 Bytes: Semantic triples count
    pub command_count: u16,       // 2 Bytes: Canvas command count
    pub string_pool_offset: u16,  // 2 Bytes: Offset to string pool
}

impl NdaHeader {
    pub fn new(triple_count: u32, command_count: u16, string_pool_offset: u16) -> Self {
        let mut header = Self {
            magic: NDA_MAGIC,
            flags: 0,
            merkle_root: [0u8; 32],
            triple_count,
            command_count,
            string_pool_offset,
        };
        header.recalculate_merkle();
        header
    }

    pub fn recalculate_merkle(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(&self.magic.to_le_bytes());
        hasher.update(&self.flags.to_le_bytes());
        hasher.update(&self.triple_count.to_le_bytes());
        hasher.update(&self.command_count.to_le_bytes());
        hasher.update(&self.string_pool_offset.to_le_bytes());
        let res = hasher.finalize();
        self.merkle_root.copy_from_slice(&res);
    }

    pub fn verify_merkle(&self) -> bool {
        let mut hasher = Sha256::new();
        hasher.update(&self.magic.to_le_bytes());
        hasher.update(&self.flags.to_le_bytes());
        hasher.update(&self.triple_count.to_le_bytes());
        hasher.update(&self.command_count.to_le_bytes());
        hasher.update(&self.string_pool_offset.to_le_bytes());
        let res = hasher.finalize();
        self.merkle_root == res.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nda_header_verification() {
        let mut header = NdaHeader::new(10, 5, 256);
        assert_eq!(header.magic, NDA_MAGIC);
        assert!(header.verify_merkle());

        header.triple_count = 99;
        assert!(!header.verify_merkle());
    }
}
