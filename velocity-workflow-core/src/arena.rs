//! Unmanaged lock-free Tier-2 bump allocation arena for dynamic payload overflow.

pub const DEFAULT_ARENA_PAGE_SIZE: usize = 65536; // 64KB page

#[repr(C)]
pub struct BumpArenaPage {
    pub capacity: usize,
    pub offset: usize,
    pub data: [u8; DEFAULT_ARENA_PAGE_SIZE],
}

impl BumpArenaPage {
    pub fn new() -> Self {
        Self {
            capacity: DEFAULT_ARENA_PAGE_SIZE,
            offset: 0,
            data: [0u8; DEFAULT_ARENA_PAGE_SIZE],
        }
    }

    pub fn alloc_slice(&mut self, bytes: &[u8]) -> Option<usize> {
        let len = bytes.len();
        if self.offset + len > self.capacity {
            return None; // Page full
        }
        let start = self.offset;
        self.data[start..start + len].copy_from_slice(bytes);
        self.offset += len;
        Some(start)
    }

    pub fn get_slice(&self, start: usize, len: usize) -> Option<&[u8]> {
        if start + len > self.offset {
            None
        } else {
            Some(&self.data[start..start + len])
        }
    }

    pub fn reset(&mut self) {
        self.offset = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bump_arena_alloc_and_reset() {
        let mut arena = BumpArenaPage::new();
        let payload = b"Hello V.E.L.O.C.I.T.Y. Tier-2 Bump Arena";

        let offset = arena.alloc_slice(payload).expect("Allocation failed");
        assert_eq!(offset, 0);

        let retrieved = arena.get_slice(offset, payload.len()).unwrap();
        assert_eq!(retrieved, payload);

        arena.reset();
        assert_eq!(arena.offset, 0);
    }
}
