//! Zero-allocation CRDT (Conflict-Free Replicated Data Types) for multi-region workflow convergence.

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PNCounter {
    pub increments: u64,
    pub decrements: u64,
}

impl Default for PNCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl PNCounter {
    pub const fn new() -> Self {
        Self {
            increments: 0,
            decrements: 0,
        }
    }

    #[inline(always)]
    pub fn value(&self) -> i64 {
        (self.increments as i64) - (self.decrements as i64)
    }

    #[inline(always)]
    pub fn inc(&mut self, delta: u64) {
        self.increments = self.increments.saturating_add(delta);
    }

    #[inline(always)]
    pub fn dec(&mut self, delta: u64) {
        self.decrements = self.decrements.saturating_add(delta);
    }

    #[inline(always)]
    pub fn merge(&mut self, other: &PNCounter) {
        if other.increments > self.increments {
            self.increments = other.increments;
        }
        if other.decrements > self.decrements {
            self.decrements = other.decrements;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pn_counter_convergence() {
        let mut node1 = PNCounter::new();
        let mut node2 = PNCounter::new();

        node1.inc(10);
        node1.dec(3);

        node2.inc(15);
        node2.dec(1);

        node1.merge(&node2);
        assert_eq!(node1.increments, 15);
        assert_eq!(node1.decrements, 3);
        assert_eq!(node1.value(), 12);
    }
}
