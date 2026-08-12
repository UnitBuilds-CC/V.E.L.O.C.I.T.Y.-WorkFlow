//! Bitmask status vector tracking for zero-allocation workflow step states.

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bitmask256 {
    pub bits: [u64; 4],
}

impl Default for Bitmask256 {
    fn default() -> Self {
        Self::new()
    }
}

impl Bitmask256 {
    pub const fn new() -> Self {
        Self { bits: [0; 4] }
    }

    #[inline(always)]
    pub fn set_step(&mut self, step_index: usize) -> bool {
        if step_index >= 256 {
            return false;
        }
        let word = step_index / 64;
        let bit = step_index % 64;
        self.bits[word] |= 1u64 << bit;
        true
    }

    #[inline(always)]
    pub fn is_step_set(&self, step_index: usize) -> bool {
        if step_index >= 256 {
            return false;
        }
        let word = step_index / 64;
        let bit = step_index % 64;
        (self.bits[word] & (1u64 << bit)) != 0
    }

    #[inline(always)]
    pub fn clear_step(&mut self, step_index: usize) -> bool {
        if step_index >= 256 {
            return false;
        }
        let word = step_index / 64;
        let bit = step_index % 64;
        self.bits[word] &= !(1u64 << bit);
        true
    }

    #[inline(always)]
    pub fn count_completed(&self) -> u32 {
        self.bits[0].count_ones()
            + self.bits[1].count_ones()
            + self.bits[2].count_ones()
            + self.bits[3].count_ones()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmask_operations() {
        let mut mask = Bitmask256::new();
        assert!(!mask.is_step_set(0));
        assert!(!mask.is_step_set(42));

        mask.set_step(42);
        assert!(mask.is_step_set(42));
        assert_eq!(mask.count_completed(), 1);

        mask.set_step(255);
        assert!(mask.is_step_set(255));
        assert_eq!(mask.count_completed(), 2);

        mask.clear_step(42);
        assert!(!mask.is_step_set(42));
        assert_eq!(mask.count_completed(), 1);
    }
}
