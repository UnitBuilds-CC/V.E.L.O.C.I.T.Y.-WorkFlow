//! VCTP (V.E.L.O.C.I.T.Y. Zero-Copy Transport Protocol) for high-speed slab delta replication.

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct VctpPacketHeader {
    pub magic: u32,           // 4 Bytes: "VCTP" (0x50544356)
    pub sequence_number: u64, // 8 Bytes: Monotonic packet sequence ID
    pub workflow_id: u64,     // 8 Bytes: Associated workflow ID
    pub slab_offset: u32,     // 4 Bytes: Byte offset in target slab
    pub payload_length: u32,  // 4 Bytes: Length of bitmask or slab delta payload
}

pub const VCTP_MAGIC: u32 = 0x50544356; // "VCTP"

impl VctpPacketHeader {
    pub fn new(
        sequence_number: u64,
        workflow_id: u64,
        slab_offset: u32,
        payload_length: u32,
    ) -> Self {
        Self {
            magic: VCTP_MAGIC,
            sequence_number,
            workflow_id,
            slab_offset,
            payload_length,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.magic == VCTP_MAGIC
    }
}

/// AIMD Congestion Control Pacing Calculator
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct AimdController {
    pub pacing_rate_mbps: u32,
    pub loss_threshold_percent: u32,
}

impl Default for AimdController {
    fn default() -> Self {
        Self::new()
    }
}

impl AimdController {
    pub fn new() -> Self {
        Self {
            pacing_rate_mbps: 100,     // Default 100 Mbps
            loss_threshold_percent: 2, // 2% loss threshold
        }
    }

    pub fn update(&mut self, loss_percent: u32) {
        if loss_percent > self.loss_threshold_percent {
            // Multiplicative Decrease (scale back 15%)
            self.pacing_rate_mbps = (self.pacing_rate_mbps as f32 * 0.85) as u32;
            if self.pacing_rate_mbps < 10 {
                self.pacing_rate_mbps = 10;
            }
        } else {
            // Additive Increase (+10 Mbps per window)
            self.pacing_rate_mbps = self.pacing_rate_mbps.saturating_add(10);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vctp_packet_header() {
        let header = VctpPacketHeader::new(1, 999, 64, 32);
        assert!(header.is_valid());
        assert_eq!(header.sequence_number, 1);
    }

    #[test]
    fn test_aimd_congestion_control() {
        let mut controller = AimdController::new();
        assert_eq!(controller.pacing_rate_mbps, 100);

        // No loss -> Additive Increase
        controller.update(0);
        assert_eq!(controller.pacing_rate_mbps, 110);

        // High loss (>2%) -> Multiplicative Decrease
        controller.update(5);
        assert_eq!(controller.pacing_rate_mbps, 93);
    }
}
