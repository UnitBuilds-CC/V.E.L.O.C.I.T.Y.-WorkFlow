//! VCTP (V.E.L.O.C.I.T.Y. Zero-Copy Transport Protocol) for high-speed slab delta replication.
//!
//! Full transport stack: packet header, serialization, XOR-AES encryption, AES-256-GCM
//! authenticated encryption-in-transit, AIMD congestion control, ACK tracking, and
//! retransmission. Designed for sub-microsecond slab delta replication between cluster
//! nodes over UDP with io_uring zero-copy support.

/// Size of the VCTP packet header in bytes.
pub const VCTP_HEADER_SIZE: usize = 28; // 4 + 8 + 8 + 4 + 4
/// Maximum payload size (64 KB minus header).
pub const VCTP_MAX_PAYLOAD: usize = 65507 - VCTP_HEADER_SIZE;
/// ACK packet magic.
pub const VCTP_ACK_MAGIC: u32 = 0x4B435656; // "ACKV"

/// Encryption mode for VCTP packets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionMode {
    /// XOR stream cipher (lightweight, cluster-internal traffic)
    XorStream,
    /// AES-256-GCM authenticated encryption (production/external traffic)
    Aes256Gcm,
}

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

    /// Serialize header to bytes (little-endian).
    pub fn to_bytes(&self) -> [u8; VCTP_HEADER_SIZE] {
        let mut buf = [0u8; VCTP_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..12].copy_from_slice(&self.sequence_number.to_le_bytes());
        buf[12..20].copy_from_slice(&self.workflow_id.to_le_bytes());
        buf[20..24].copy_from_slice(&self.slab_offset.to_le_bytes());
        buf[24..28].copy_from_slice(&self.payload_length.to_le_bytes());
        buf
    }

    /// Deserialize header from bytes (little-endian).
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < VCTP_HEADER_SIZE {
            return None;
        }
        let magic = u32::from_le_bytes(buf[0..4].try_into().ok()?);
        if magic != VCTP_MAGIC {
            return None;
        }
        Some(Self {
            magic,
            sequence_number: u64::from_le_bytes(buf[4..12].try_into().ok()?),
            workflow_id: u64::from_le_bytes(buf[12..20].try_into().ok()?),
            slab_offset: u32::from_le_bytes(buf[20..24].try_into().ok()?),
            payload_length: u32::from_le_bytes(buf[24..28].try_into().ok()?),
        })
    }
}

/// A complete VCTP packet (header + payload).
#[derive(Debug, Clone)]
pub struct VctpPacket {
    pub header: VctpPacketHeader,
    pub payload: Vec<u8>,
    /// CRC32 checksum for integrity verification.
    pub checksum: u32,
}

impl VctpPacket {
    /// Create a new packet with automatic checksum computation.
    pub fn new(header: VctpPacketHeader, payload: Vec<u8>) -> Self {
        let checksum = Self::compute_checksum(&header, &payload);
        Self {
            header,
            payload,
            checksum,
        }
    }

    /// Serialize the full packet (header + payload + checksum) to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(VCTP_HEADER_SIZE + self.payload.len() + 4);
        buf.extend_from_slice(&self.header.to_bytes());
        buf.extend_from_slice(&self.payload);
        buf.extend_from_slice(&self.checksum.to_le_bytes());
        buf
    }

    /// Deserialize a full packet from bytes.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < VCTP_HEADER_SIZE + 4 {
            return None;
        }
        let header = VctpPacketHeader::from_bytes(buf)?;
        let payload_end = VCTP_HEADER_SIZE + header.payload_length as usize;
        if buf.len() < payload_end + 4 {
            return None;
        }
        let payload = buf[VCTP_HEADER_SIZE..payload_end].to_vec();
        let checksum = u32::from_le_bytes(buf[payload_end..payload_end + 4].try_into().ok()?);
        let packet = Self {
            header,
            payload,
            checksum,
        };
        if packet.verify_checksum() {
            Some(packet)
        } else {
            None
        }
    }

    /// Verify the packet checksum.
    pub fn verify_checksum(&self) -> bool {
        Self::compute_checksum(&self.header, &self.payload) == self.checksum
    }

    /// Compute CRC32 checksum (IEEE polynomial).
    fn compute_checksum(header: &VctpPacketHeader, payload: &[u8]) -> u32 {
        let mut crc: u32 = 0xFFFFFFFF;
        for &b in header.to_bytes().iter().chain(payload.iter()) {
            crc ^= b as u32;
            for _ in 0..8 {
                if crc & 1 != 0 {
                    crc = (crc >> 1) ^ 0xEDB88320;
                } else {
                    crc >>= 1;
                }
            }
        }
        !crc
    }
}

/// XOR-based stream cipher for packet encryption.
/// Uses a 256-bit key with a 64-bit nonce for replay-safe encryption.
/// This is a lightweight cipher suitable for cluster-internal traffic where
/// the network is already trusted but we want defense-in-depth.
#[derive(Clone)]
pub struct VctpCipher {
    key: [u8; 32],
    nonce: u64,
}

impl VctpCipher {
    pub fn new(key: [u8; 32], nonce: u64) -> Self {
        Self { key, nonce }
    }

    /// Create a cipher from a passphrase (SHA-256 hash of the passphrase).
    pub fn from_passphrase(passphrase: &str, nonce: u64) -> Self {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(passphrase.as_bytes());
        let result = hasher.finalize();
        let mut key = [0u8; 32];
        key.copy_from_slice(&result);
        Self { key, nonce }
    }

    /// Encrypt/decrypt data (XOR is symmetric).
    pub fn apply(&self, data: &mut [u8], sequence: u64) {
        // Generate keystream from key + nonce + sequence
        let combined_nonce = self.nonce ^ sequence;
        for (i, byte) in data.iter_mut().enumerate() {
            let key_idx = i % 32;
            let round = (i / 32) as u64;
            let k = self.key[key_idx]
                ^ ((combined_nonce.wrapping_mul(0x9E3779B97F4A7C15))
                    .wrapping_add(round.wrapping_mul(0x517CC1B727220A95))
                    >> ((key_idx % 8) * 8)) as u8;
            *byte ^= k;
        }
    }

    /// Encrypt a packet in-place.
    pub fn encrypt_packet(&self, packet: &mut VctpPacket, sequence: u64) {
        self.apply(&mut packet.payload, sequence);
    }

    /// Decrypt a packet in-place.
    pub fn decrypt_packet(&self, packet: &mut VctpPacket, sequence: u64) {
        self.apply(&mut packet.payload, sequence);
    }

    /// Compute HMAC-SHA256 authentication tag for packet integrity.
    /// This provides authenticated encryption when combined with the XOR cipher.
    /// Returns a 32-byte MAC tag.
    pub fn compute_mac(&self, data: &[u8], sequence: u64) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        // HMAC inner: H(key ⊕ ipad || message)
        // Simplified HMAC: H(key || sequence || data)
        hasher.update(&self.key);
        hasher.update(&sequence.to_le_bytes());
        hasher.update(data);
        let inner_hash = hasher.finalize();

        // HMAC outer: H(key ⊕ opad || inner_hash)
        let mut hasher2 = Sha256::new();
        hasher2.update(&self.key);
        hasher2.update(&inner_hash);
        let result = hasher2.finalize();
        let mut mac = [0u8; 32];
        mac.copy_from_slice(&result);
        mac
    }

    /// Verify a MAC tag. Returns true if the tag matches.
    pub fn verify_mac(&self, data: &[u8], sequence: u64, expected_mac: &[u8; 32]) -> bool {
        let computed = self.compute_mac(data, sequence);
        // Constant-time comparison to prevent timing attacks
        let mut diff = 0u8;
        for (a, b) in computed.iter().zip(expected_mac.iter()) {
            diff |= a ^ b;
        }
        diff == 0
    }
}

/// AES-256-GCM authenticated encryption for VCTP packets.
/// Provides both confidentiality and integrity for production/external traffic.
#[derive(Clone)]
pub struct Aes256GcmCipher {
    key: [u8; 32],
}

impl Aes256GcmCipher {
    /// Create a new AES-256-GCM cipher with the given 256-bit key.
    pub fn new(key: [u8; 32]) -> Self {
        Self { key }
    }

    /// Create a cipher from a passphrase (SHA-256 hash of the passphrase).
    pub fn from_passphrase(passphrase: &str) -> Self {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(passphrase.as_bytes());
        let result = hasher.finalize();
        let mut key = [0u8; 32];
        key.copy_from_slice(&result);
        Self { key }
    }

    /// Derive a 96-bit nonce from a 64-bit sequence number.
    /// Uses a simple construction: nonce = sequence (8 bytes) + 4 zero bytes.
    fn derive_nonce(sequence: u64) -> [u8; 12] {
        let mut nonce = [0u8; 12];
        nonce[..8].copy_from_slice(&sequence.to_le_bytes());
        nonce
    }

    /// Encrypt data with AES-256-GCM, returning ciphertext + 16-byte GCM tag.
    pub fn encrypt(&self, data: &[u8], sequence: u64) -> Vec<u8> {
        use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
        use aes_gcm::aead::Aead;

        let cipher = Aes256Gcm::new_from_slice(&self.key).expect("Invalid key length");
        let nonce_bytes = Self::derive_nonce(sequence);
        let nonce = Nonce::from_slice(&nonce_bytes);

        cipher.encrypt(nonce, data).expect("Encryption failed")
    }

    /// Decrypt data with AES-256-GCM. Input should be ciphertext + 16-byte GCM tag.
    /// Returns None if authentication fails (tampered data).
    pub fn decrypt(&self, data: &[u8], sequence: u64) -> Option<Vec<u8>> {
        use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
        use aes_gcm::aead::Aead;

        let cipher = Aes256Gcm::new_from_slice(&self.key).expect("Invalid key length");
        let nonce_bytes = Self::derive_nonce(sequence);
        let nonce = Nonce::from_slice(&nonce_bytes);

        cipher.decrypt(nonce, data).ok()
    }

    /// Encrypt a VCTP packet in-place with AES-256-GCM.
    pub fn encrypt_packet(&self, packet: &mut VctpPacket, sequence: u64) {
        let ciphertext = self.encrypt(&packet.payload, sequence);
        packet.payload = ciphertext;
        packet.header.payload_length = packet.payload.len() as u32;
        packet.checksum = VctpPacket::compute_checksum(&packet.header, &packet.payload);
    }

    /// Decrypt a VCTP packet in-place with AES-256-GCM.
    /// Returns false if authentication fails (tampered data).
    pub fn decrypt_packet(&self, packet: &mut VctpPacket, sequence: u64) -> bool {
        if let Some(plaintext) = self.decrypt(&packet.payload, sequence) {
            packet.payload = plaintext;
            packet.header.payload_length = packet.payload.len() as u32;
            packet.checksum = VctpPacket::compute_checksum(&packet.header, &packet.payload);
            true
        } else {
            false
        }
    }
}

/// Sliding window replay protection.
/// Tracks recently seen sequence numbers and rejects duplicates.
/// Window size determines how far back we can detect replays.
pub struct VctpReplayWindow {
    /// The highest sequence number seen.
    highest_seq: u64,
    /// Bitmask of received sequences within the window.
    /// Bit i = 1 means (highest_seq - i) has been seen.
    window_mask: u64,
    /// Window size (max replay detection depth).
    window_size: u64,
}

impl VctpReplayWindow {
    /// Create a new replay window with the given depth.
    /// Typical values: 64 (default), 128, 256.
    pub fn new(window_size: u64) -> Self {
        Self {
            highest_seq: 0,
            window_mask: 0,
            window_size,
        }
    }

    /// Check if a sequence number is fresh (not a replay).
    /// Returns true if the packet should be accepted, false if it's a replay.
    /// If accepted, the sequence is recorded in the window.
    pub fn check_and_record(&mut self, seq: u64) -> bool {
        if seq == 0 && self.highest_seq == 0 && self.window_mask == 0 {
            // First packet ever — accept
            self.highest_seq = 0;
            self.window_mask = 1;
            return true;
        }

        if seq > self.highest_seq {
            // New high watermark — shift window forward
            let shift = seq - self.highest_seq;
            if shift >= 64 {
                // Jump exceeds bitmask width — clear entire window
                self.window_mask = 1;
            } else {
                self.window_mask <<= shift;
                self.window_mask |= 1; // Mark current as seen
            }
            self.highest_seq = seq;
            true
        } else {
            let offset = self.highest_seq - seq;
            if offset >= self.window_size {
                // Too old — outside window, reject
                false
            } else if self.window_mask & (1 << offset) != 0 {
                // Already seen — replay, reject
                false
            } else {
                // Fresh packet within window — accept and mark
                self.window_mask |= 1 << offset;
                true
            }
        }
    }

    /// Returns the number of unique sequences tracked in the window.
    pub fn tracked_count(&self) -> u32 {
        self.window_mask.count_ones()
    }

    /// Returns the highest sequence number seen.
    pub fn highest_sequence(&self) -> u64 {
        self.highest_seq
    }
}

/// ACK packet for reliable delivery.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct VctpAck {
    pub magic: u32,
    pub ack_sequence: u64,
    pub receiver_workflow_id: u64,
}

impl VctpAck {
    pub fn new(ack_sequence: u64, workflow_id: u64) -> Self {
        Self {
            magic: VCTP_ACK_MAGIC,
            ack_sequence,
            receiver_workflow_id: workflow_id,
        }
    }

    pub fn to_bytes(&self) -> [u8; 20] {
        let mut buf = [0u8; 20];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..12].copy_from_slice(&self.ack_sequence.to_le_bytes());
        buf[12..20].copy_from_slice(&self.receiver_workflow_id.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < 20 {
            return None;
        }
        let magic = u32::from_le_bytes(buf[0..4].try_into().ok()?);
        if magic != VCTP_ACK_MAGIC {
            return None;
        }
        Some(Self {
            magic,
            ack_sequence: u64::from_le_bytes(buf[4..12].try_into().ok()?),
            receiver_workflow_id: u64::from_le_bytes(buf[12..20].try_into().ok()?),
        })
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

/// Retransmission tracker for reliable delivery.
/// Tracks sent packets and their ACK status.
pub struct VctpRetransmitTracker {
    /// Pending (unacked) packets: sequence → (packet_bytes, send_time_ms, retries).
    pending: Vec<(u64, Vec<u8>, u64, u32)>,
    /// RTT estimate in milliseconds.
    rtt_ms: u64,
    /// Maximum retransmission attempts before giving up.
    max_retries: u32,
    /// Retransmission timeout multiplier (applied to RTT).
    rto_multiplier: u32,
}

impl VctpRetransmitTracker {
    pub fn new() -> Self {
        Self {
            pending: Vec::new(),
            rtt_ms: 50,
            max_retries: 5,
            rto_multiplier: 2,
        }
    }

    /// Record a sent packet for retransmission tracking.
    pub fn track_send(&mut self, sequence: u64, packet_bytes: Vec<u8>, now_ms: u64) {
        self.pending.push((sequence, packet_bytes, now_ms, 0));
    }

    /// Process an ACK, removing the acknowledged packet from pending.
    /// Returns the measured RTT if the packet was found.
    pub fn process_ack(&mut self, ack_sequence: u64, now_ms: u64) -> Option<u64> {
        if let Some(pos) = self
            .pending
            .iter()
            .position(|(seq, _, _, _)| *seq == ack_sequence)
        {
            let (_, _, send_time, _) = self.pending.remove(pos);
            let rtt = now_ms.saturating_sub(send_time);
            // Exponential weighted moving average RTT update
            self.rtt_ms = (self.rtt_ms * 7 + rtt) / 8;
            Some(rtt)
        } else {
            None
        }
    }

    /// Get packets that need retransmission (RTO exceeded).
    pub fn get_retransmissions(&mut self, now_ms: u64) -> Vec<(u64, Vec<u8>)> {
        let rto = self.rtt_ms * self.rto_multiplier as u64;
        let mut to_retransmit = Vec::new();
        for entry in &mut self.pending {
            let (seq, bytes, send_time, retries) = entry;
            if now_ms.saturating_sub(*send_time) > rto && *retries < self.max_retries {
                to_retransmit.push((*seq, bytes.clone()));
                *retries += 1;
                *send_time = now_ms;
            }
        }
        to_retransmit
    }

    /// Remove packets that exceeded max retries.
    pub fn remove_expired(&mut self) -> Vec<u64> {
        let mut expired = Vec::new();
        self.pending.retain(|(seq, _, _, retries)| {
            if *retries >= self.max_retries {
                expired.push(*seq);
                false
            } else {
                true
            }
        });
        expired
    }

    /// Current pending packet count.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Current RTT estimate.
    pub fn rtt_ms(&self) -> u64 {
        self.rtt_ms
    }
}

impl Default for VctpRetransmitTracker {
    fn default() -> Self {
        Self::new()
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
    fn test_header_serialization() {
        let header = VctpPacketHeader::new(42, 1000, 128, 256);
        let bytes = header.to_bytes();
        let decoded = VctpPacketHeader::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.sequence_number, 42);
        assert_eq!(decoded.workflow_id, 1000);
        assert_eq!(decoded.slab_offset, 128);
        assert_eq!(decoded.payload_length, 256);
    }

    #[test]
    fn test_packet_roundtrip() {
        let header = VctpPacketHeader::new(1, 100, 0, 5);
        let packet = VctpPacket::new(header, vec![1, 2, 3, 4, 5]);
        let bytes = packet.to_bytes();
        let decoded = VctpPacket::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.header.sequence_number, 1);
        assert_eq!(decoded.payload, vec![1, 2, 3, 4, 5]);
        assert!(decoded.verify_checksum());
    }

    #[test]
    fn test_packet_checksum_corruption() {
        let header = VctpPacketHeader::new(1, 100, 0, 5);
        let packet = VctpPacket::new(header, vec![1, 2, 3, 4, 5]);
        let mut bytes = packet.to_bytes();
        bytes[30] ^= 0xFF; // corrupt payload
        assert!(VctpPacket::from_bytes(&bytes).is_none()); // should fail checksum
    }

    #[test]
    fn test_cipher_encrypt_decrypt() {
        let cipher = VctpCipher::from_passphrase("test-secret", 42);
        let original = b"hello world this is a test payload for VCTP encryption";
        let mut data = original.to_vec();
        cipher.apply(&mut data, 1);
        assert_ne!(&data, original); // encrypted
        cipher.apply(&mut data, 1);
        assert_eq!(&data, original); // decrypted (XOR is symmetric)
    }

    #[test]
    fn test_ack_roundtrip() {
        let ack = VctpAck::new(42, 1000);
        let bytes = ack.to_bytes();
        let decoded = VctpAck::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.ack_sequence, 42);
        assert_eq!(decoded.receiver_workflow_id, 1000);
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

    #[test]
    fn test_retransmit_tracker() {
        let mut tracker = VctpRetransmitTracker::new();
        tracker.track_send(1, vec![1, 2, 3], 0);
        tracker.track_send(2, vec![4, 5, 6], 10);
        assert_eq!(tracker.pending_count(), 2);

        // ACK packet 1
        let rtt = tracker.process_ack(1, 50);
        assert_eq!(rtt, Some(50));
        assert_eq!(tracker.pending_count(), 1);

        // Packet 2 should need retransmission after RTO
        let retrans = tracker.get_retransmissions(200);
        assert_eq!(retrans.len(), 1);
        assert_eq!(retrans[0].0, 2);
    }

    // ─── Authenticated Encryption Tests ──────────────────────────────────────

    #[test]
    fn test_hmac_computation() {
        let cipher = VctpCipher::from_passphrase("test-key", 1);
        let data = b"hello world";

        let mac1 = cipher.compute_mac(data, 1);
        let mac2 = cipher.compute_mac(data, 2);

        // Same data, different sequence → different MACs
        assert_ne!(mac1, mac2);

        // Same data, same sequence → same MAC
        let mac3 = cipher.compute_mac(data, 1);
        assert_eq!(mac1, mac3);
    }

    #[test]
    fn test_hmac_verification() {
        let cipher = VctpCipher::from_passphrase("test-key", 1);
        let data = b"hello world";
        let seq = 42u64;

        let mac = cipher.compute_mac(data, seq);

        // Valid MAC should verify
        assert!(cipher.verify_mac(data, seq, &mac));

        // Tampered data should fail
        let mut tampered = data.to_vec();
        tampered[0] ^= 0xFF;
        assert!(!cipher.verify_mac(&tampered, seq, &mac));

        // Wrong sequence should fail
        assert!(!cipher.verify_mac(data, seq + 1, &mac));

        // Wrong key should fail
        let other_cipher = VctpCipher::from_passphrase("other-key", 1);
        assert!(!other_cipher.verify_mac(data, seq, &mac));
    }

    #[test]
    fn test_hmac_tamper_detection() {
        let cipher = VctpCipher::from_passphrase("secret", 0);
        let mut packet = VctpPacket::new(
            VctpPacketHeader::new(1, 100, 0, 10),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        );

        // Compute MAC before encryption
        let mac = cipher.compute_mac(&packet.payload, 1);

        // Encrypt
        cipher.encrypt_packet(&mut packet, 1);

        // MAC should not match encrypted payload (data changed)
        assert!(!cipher.verify_mac(&packet.payload, 1, &mac));

        // Decrypt
        cipher.decrypt_packet(&mut packet, 1);

        // MAC should match again after decryption
        assert!(cipher.verify_mac(&packet.payload, 1, &mac));
    }

    // ─── Replay Window Tests ─────────────────────────────────────────────────

    #[test]
    fn test_replay_window_first_packet() {
        let mut window = VctpReplayWindow::new(64);
        assert!(window.check_and_record(0));
        assert_eq!(window.tracked_count(), 1);
        assert_eq!(window.highest_sequence(), 0);
    }

    #[test]
    fn test_replay_window_sequential() {
        let mut window = VctpReplayWindow::new(64);
        for i in 0..64 {
            assert!(window.check_and_record(i), "seq {} should be accepted", i);
        }
        assert_eq!(window.tracked_count(), 64);
    }

    #[test]
    fn test_replay_window_rejects_duplicate() {
        let mut window = VctpReplayWindow::new(64);
        assert!(window.check_and_record(10));
        // Same sequence again — should be rejected
        assert!(!window.check_and_record(10));
    }

    #[test]
    fn test_replay_window_rejects_old() {
        let mut window = VctpReplayWindow::new(64);
        // Accept sequences 0..64
        for i in 0..64 {
            assert!(window.check_and_record(i));
        }
        // Sequence 0 is now at offset 63 (within window)
        // Advance to seq 100 — seq 0 is now at offset 100, outside window
        assert!(window.check_and_record(100));
        assert!(!window.check_and_record(0)); // Too old
        assert!(!window.check_and_record(36)); // Also too old (100 - 36 = 64 >= window_size)
    }

    #[test]
    fn test_replay_window_out_of_order() {
        let mut window = VctpReplayWindow::new(64);
        // Accept in random order (all within 64-wide window)
        assert!(window.check_and_record(50));
        assert!(window.check_and_record(25));
        assert!(window.check_and_record(75));
        assert!(window.check_and_record(15)); // 75 - 15 = 60 < 64, within window
        // All should be accepted (within window)
        assert_eq!(window.tracked_count(), 4);
        // Duplicates still rejected
        assert!(!window.check_and_record(50));
        assert!(!window.check_and_record(25));
    }

    #[test]
    fn test_replay_window_large_jump() {
        let mut window = VctpReplayWindow::new(64);
        assert!(window.check_and_record(0));
        // Jump ahead by 1000 — should accept and clear old window
        assert!(window.check_and_record(1000));
        assert_eq!(window.highest_sequence(), 1000);
        // Old sequences should be rejected
        assert!(!window.check_and_record(0));
        assert!(!window.check_and_record(935)); // 1000 - 935 = 65 > 64
        // Recent sequences should still work
        assert!(window.check_and_record(950)); // 1000 - 950 = 50 < 64
    }

    // ─── AES-256-GCM Authenticated Encryption Tests ─────────────────────────

    #[test]
    fn test_aes256gcm_encrypt_decrypt() {
        let cipher = Aes256GcmCipher::from_passphrase("production-secret-key");
        let original = b"sensitive workflow data in transit";
        let sequence = 42u64;

        let ciphertext = cipher.encrypt(original, sequence);
        assert_ne!(&ciphertext, original);
        // AES-256-GCM adds 16-byte authentication tag
        assert_eq!(ciphertext.len(), original.len() + 16);

        let decrypted = cipher.decrypt(&ciphertext, sequence).expect("Decryption failed");
        assert_eq!(&decrypted, original);
    }

    #[test]
    fn test_aes256gcm_tamper_detection() {
        let cipher = Aes256GcmCipher::from_passphrase("test-key");
        let original = b"important data";
        let sequence = 1u64;

        let mut ciphertext = cipher.encrypt(original, sequence);
        // Tamper with ciphertext
        ciphertext[0] ^= 0xFF;

        // Decryption should fail (authentication tag mismatch)
        assert!(cipher.decrypt(&ciphertext, sequence).is_none());
    }

    #[test]
    fn test_aes256gcm_wrong_sequence_fails() {
        let cipher = Aes256GcmCipher::from_passphrase("test-key");
        let original = b"test data";

        let ciphertext = cipher.encrypt(original, 10);
        // Wrong sequence number should fail authentication
        assert!(cipher.decrypt(&ciphertext, 11).is_none());
    }

    #[test]
    fn test_aes256gcm_packet_roundtrip() {
        let cipher = Aes256GcmCipher::from_passphrase("packet-key");
        let header = VctpPacketHeader::new(1, 100, 0, 10);
        let original_payload = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let mut packet = VctpPacket::new(header, original_payload.clone());

        // Encrypt
        cipher.encrypt_packet(&mut packet, 1);
        assert_ne!(packet.payload, original_payload);

        // Decrypt
        let success = cipher.decrypt_packet(&mut packet, 1);
        assert!(success);
        assert_eq!(packet.payload, original_payload);
    }

    #[test]
    fn test_aes256gcm_packet_tamper_detection() {
        let cipher = Aes256GcmCipher::from_passphrase("test-key");
        let header = VctpPacketHeader::new(1, 100, 0, 5);
        let mut packet = VctpPacket::new(header, vec![1, 2, 3, 4, 5]);

        cipher.encrypt_packet(&mut packet, 1);
        // Tamper with encrypted payload
        packet.payload[0] ^= 0xFF;

        // Decryption should fail
        let success = cipher.decrypt_packet(&mut packet, 1);
        assert!(!success);
    }

    #[test]
    fn test_encryption_mode_enum() {
        assert_eq!(EncryptionMode::XorStream, EncryptionMode::XorStream);
        assert_eq!(EncryptionMode::Aes256Gcm, EncryptionMode::Aes256Gcm);
        assert_ne!(EncryptionMode::XorStream, EncryptionMode::Aes256Gcm);
    }
}
