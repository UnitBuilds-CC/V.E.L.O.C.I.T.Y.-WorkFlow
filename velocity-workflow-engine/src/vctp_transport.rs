//! VCTP UDP Transport — real socket-level communication for slab delta replication.
//!
//! Provides a [`VctpTransport`] that binds a UDP socket, sends/receives VCTP packets,
//! handles encryption, ACK tracking, retransmission, and congestion control.
//! This is the actual network transport that makes VCTP a real protocol, not just
//! a header definition.
//!
//! Architecture:
//!   [WorkflowEngine] ──► [VctpTransport] ──UDP──► [Remote VctpTransport]
//!   (sender)               (socket + cipher)        (receiver)

use std::collections::HashMap;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::RwLock;
use std::time::Instant;

use velocity_workflow_core::vctp::{
    AimdController, VctpAck, VctpCipher, VctpPacket, VctpPacketHeader, VctpRetransmitTracker,
};

/// Statistics for the VCTP transport.
#[derive(Debug, Clone, Default)]
pub struct VctpTransportStats {
    pub packets_sent: u64,
    pub packets_received: u64,
    pub packets_acked: u64,
    pub packets_retransmitted: u64,
    pub packets_dropped: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub acks_received: u64,
    pub acks_sent: u64,
    pub checksum_failures: u64,
    pub encryption_errors: u64,
}

/// Configuration for the VCTP transport.
#[derive(Debug, Clone)]
pub struct VctpTransportConfig {
    /// Local address to bind the UDP socket.
    pub bind_addr: String,
    /// Encryption passphrase (empty = no encryption).
    pub encryption_passphrase: String,
    /// Nonce for encryption uniqueness.
    pub nonce: u64,
    /// Maximum retransmission attempts.
    pub max_retries: u32,
    /// Retransmission timeout multiplier.
    pub rto_multiplier: u32,
    /// Receive buffer size.
    pub recv_buffer_size: usize,
}

impl Default for VctpTransportConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:0".to_string(),
            encryption_passphrase: String::new(),
            nonce: 0,
            max_retries: 5,
            rto_multiplier: 2,
            recv_buffer_size: 65536,
        }
    }
}

/// Known peer for VCTP communication.
#[derive(Debug, Clone)]
pub struct VctpPeer {
    pub peer_id: u64,
    pub address: SocketAddr,
    pub last_seen_ms: u64,
    pub packets_sent: u64,
    pub packets_received: u64,
}

/// VCTP UDP Transport — real socket-level communication.
///
/// Binds a UDP socket, manages peers, handles encryption, ACK tracking,
/// retransmission, and congestion control. Provides both blocking and
/// non-blocking send/receive operations.
pub struct VctpTransport {
    socket: UdpSocket,
    #[allow(dead_code)]
    config: VctpTransportConfig,
    cipher: Option<VctpCipher>,
    next_sequence: AtomicU64,
    start_time: Instant,
    peers: RwLock<HashMap<u64, VctpPeer>>,
    /// Address → peer_id reverse lookup.
    addr_to_peer: RwLock<HashMap<SocketAddr, u64>>,
    retransmit: RwLock<VctpRetransmitTracker>,
    congestion: RwLock<AimdController>,
    stats: RwLock<VctpTransportStats>,
    running: AtomicBool,
    /// Receive buffer for batching.
    recv_buf: RwLock<Vec<u8>>,
}

impl VctpTransport {
    /// Create a new VCTP transport bound to the configured address.
    pub fn new(config: VctpTransportConfig) -> Result<Self, String> {
        let socket = UdpSocket::bind(&config.bind_addr)
            .map_err(|e| format!("Failed to bind VCTP socket: {}", e))?;
        socket.set_nonblocking(true)
            .map_err(|e| format!("Failed to set non-blocking: {}", e))?;

        let cipher = if config.encryption_passphrase.is_empty() {
            None
        } else {
            Some(VctpCipher::from_passphrase(&config.encryption_passphrase, config.nonce))
        };

        let recv_buf_size = config.recv_buffer_size;
        let recv_buf = vec![0u8; recv_buf_size];

        Ok(Self {
            socket,
            config,
            cipher,
            next_sequence: AtomicU64::new(1),
            start_time: Instant::now(),
            peers: RwLock::new(HashMap::new()),
            addr_to_peer: RwLock::new(HashMap::new()),
            retransmit: RwLock::new(VctpRetransmitTracker::new()),
            congestion: RwLock::new(AimdController::new()),
            stats: RwLock::new(VctpTransportStats::default()),
            running: AtomicBool::new(true),
            recv_buf: RwLock::new(recv_buf),
        })
    }

    fn now_ms(&self) -> u64 {
        self.start_time.elapsed().as_millis() as u64
    }

    /// Register a peer for communication.
    pub fn add_peer(&self, peer_id: u64, address: SocketAddr) {
        let now = self.now_ms();
        self.peers.write().unwrap().insert(peer_id, VctpPeer {
            peer_id,
            address,
            last_seen_ms: now,
            packets_sent: 0,
            packets_received: 0,
        });
        self.addr_to_peer.write().unwrap().insert(address, peer_id);
    }

    /// Remove a peer.
    pub fn remove_peer(&self, peer_id: u64) {
        if let Some(peer) = self.peers.write().unwrap().remove(&peer_id) {
            self.addr_to_peer.write().unwrap().remove(&peer.address);
        }
    }

    /// Send a VCTP packet to a specific peer.
    pub fn send_to_peer(&self, peer_id: u64, workflow_id: u64, slab_offset: u32, payload: Vec<u8>) -> Result<u64, String> {
        let peer_addr = self.peers.read().unwrap()
            .get(&peer_id)
            .map(|p| p.address)
            .ok_or_else(|| format!("Peer {} not found", peer_id))?;
        self.send_packet(peer_addr, workflow_id, slab_offset, payload)
    }

    /// Send a VCTP packet to a specific address.
    pub fn send_packet(&self, addr: SocketAddr, workflow_id: u64, slab_offset: u32, mut payload: Vec<u8>) -> Result<u64, String> {
        let seq = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        let header = VctpPacketHeader::new(seq, workflow_id, slab_offset, payload.len() as u32);

        // Encrypt if cipher is configured
        if let Some(ref cipher) = self.cipher {
            cipher.apply(&mut payload, seq);
        }

        let packet = VctpPacket::new(header, payload);
        let bytes = packet.to_bytes();

        let sent = self.socket.send_to(&bytes, addr)
            .map_err(|e| format!("VCTP send failed: {}", e))?;

        // Track for retransmission
        self.retransmit.write().unwrap().track_send(seq, bytes.clone(), self.now_ms());

        // Update stats
        let mut stats = self.stats.write().unwrap();
        stats.packets_sent += 1;
        stats.bytes_sent += sent as u64;

        // Update peer stats
        if let Some(peer_addr_id) = self.addr_to_peer.read().unwrap().get(&addr) {
            if let Some(peer) = self.peers.write().unwrap().get_mut(peer_addr_id) {
                peer.packets_sent += 1;
                peer.last_seen_ms = self.now_ms();
            }
        }

        Ok(seq)
    }

    /// Send an ACK for a received packet.
    pub fn send_ack(&self, addr: SocketAddr, ack_sequence: u64, workflow_id: u64) -> Result<(), String> {
        let ack = VctpAck::new(ack_sequence, workflow_id);
        let bytes = ack.to_bytes();
        self.socket.send_to(&bytes, addr)
            .map_err(|e| format!("VCTP ACK send failed: {}", e))?;
        self.stats.write().unwrap().acks_sent += 1;
        Ok(())
    }

    /// Receive and process incoming packets (non-blocking).
    /// Returns a list of received packets.
    pub fn recv_packets(&self) -> Vec<(VctpPacket, SocketAddr)> {
        let mut received = Vec::new();
        let mut buf = self.recv_buf.write().unwrap();

        loop {
            match self.socket.recv_from(&mut buf) {
                Ok((len, src_addr)) => {
                    if len < 20 {
                        continue; // Too small for ACK
                    }

                    // Check if it's an ACK
                    let maybe_ack_magic = u32::from_le_bytes(buf[0..4].try_into().unwrap_or([0; 4]));
                    if maybe_ack_magic == velocity_workflow_core::vctp::VCTP_ACK_MAGIC {
                        if let Some(ack) = VctpAck::from_bytes(&buf[..len]) {
                            let now = self.now_ms();
                            self.retransmit.write().unwrap().process_ack(ack.ack_sequence, now);
                            self.stats.write().unwrap().acks_received += 1;
                            continue;
                        }
                    }

                    // Try to parse as a VCTP packet
                    if let Some(mut packet) = VctpPacket::from_bytes(&buf[..len]) {
                        // Decrypt if cipher is configured
                        if let Some(ref cipher) = self.cipher {
                            cipher.apply(&mut packet.payload, packet.header.sequence_number);
                        }

                        // Send ACK
                        let _ = self.send_ack(src_addr, packet.header.sequence_number, packet.header.workflow_id);

                        // Update peer stats
                        if let Some(&peer_id) = self.addr_to_peer.read().unwrap().get(&src_addr) {
                            if let Some(peer) = self.peers.write().unwrap().get_mut(&peer_id) {
                                peer.packets_received += 1;
                                peer.last_seen_ms = self.now_ms();
                            }
                        }

                        let mut stats = self.stats.write().unwrap();
                        stats.packets_received += 1;
                        stats.bytes_received += len as u64;

                        received.push((packet, src_addr));
                    } else {
                        self.stats.write().unwrap().checksum_failures += 1;
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    break; // No more packets
                }
                Err(_) => {
                    self.stats.write().unwrap().packets_dropped += 1;
                    break;
                }
            }
        }

        received
    }

    /// Process retransmissions for unacknowledged packets.
    pub fn process_retransmissions(&self) -> usize {
        let now = self.now_ms();
        let retrans = self.retransmit.write().unwrap().get_retransmissions(now);
        let count = retrans.len();

        for (_seq, bytes) in retrans {
            // Find the original destination from peers
            let peers = self.peers.read().unwrap();
            for peer in peers.values() {
                let _ = self.socket.send_to(&bytes, peer.address);
            }
            self.stats.write().unwrap().packets_retransmitted += 1;
        }

        // Remove expired packets
        let expired = self.retransmit.write().unwrap().remove_expired();
        self.stats.write().unwrap().packets_dropped += expired.len() as u64;

        count
    }

    /// Update congestion control based on recent loss rate.
    pub fn update_congestion(&self, loss_percent: u32) {
        self.congestion.write().unwrap().update(loss_percent);
    }

    /// Get the current pacing rate in Mbps.
    pub fn pacing_rate_mbps(&self) -> u32 {
        self.congestion.read().unwrap().pacing_rate_mbps
    }

    /// Get transport statistics.
    pub fn stats(&self) -> VctpTransportStats {
        self.stats.read().unwrap().clone()
    }

    /// Get the local socket address.
    pub fn local_addr(&self) -> Result<SocketAddr, String> {
        self.socket.local_addr()
            .map_err(|e| format!("Failed to get local addr: {}", e))
    }

    /// Get the number of registered peers.
    pub fn peer_count(&self) -> usize {
        self.peers.read().unwrap().len()
    }

    /// Get pending (unacked) packet count.
    pub fn pending_count(&self) -> usize {
        self.retransmit.read().unwrap().pending_count()
    }

    /// Get the current RTT estimate.
    pub fn rtt_ms(&self) -> u64 {
        self.retransmit.read().unwrap().rtt_ms()
    }

    /// Shut down the transport.
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Check if the transport is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_transport_creation() {
        let config = VctpTransportConfig {
            bind_addr: "127.0.0.1:0".to_string(),
            ..Default::default()
        };
        let transport = VctpTransport::new(config).unwrap();
        assert!(transport.is_running());
        assert_eq!(transport.peer_count(), 0);
        let addr = transport.local_addr().unwrap();
        assert!(addr.port() > 0); // OS assigned a port
    }

    #[test]
    fn test_peer_management() {
        let config = VctpTransportConfig {
            bind_addr: "127.0.0.1:0".to_string(),
            ..Default::default()
        };
        let transport = VctpTransport::new(config).unwrap();
        let peer_addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        transport.add_peer(1, peer_addr);
        assert_eq!(transport.peer_count(), 1);

        transport.remove_peer(1);
        assert_eq!(transport.peer_count(), 0);
    }

    #[test]
    fn test_send_and_recv_loopback() {
        let config1 = VctpTransportConfig {
            bind_addr: "127.0.0.1:0".to_string(),
            ..Default::default()
        };
        let config2 = VctpTransportConfig {
            bind_addr: "127.0.0.1:0".to_string(),
            ..Default::default()
        };
        let t1 = VctpTransport::new(config1).unwrap();
        let t2 = VctpTransport::new(config2).unwrap();

        let t2_addr = t2.local_addr().unwrap();
        t1.add_peer(2, t2_addr);

        // Send a packet from t1 to t2
        let seq = t1.send_to_peer(2, 42, 0, vec![1, 2, 3, 4]).unwrap();
        assert!(seq > 0);

        // Small delay for UDP delivery
        std::thread::sleep(Duration::from_millis(10));

        // Receive on t2
        let received = t2.recv_packets();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].0.header.workflow_id, 42);
        assert_eq!(received[0].0.payload, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_encrypted_loopback() {
        let passphrase = "shared-secret-key";
        let config1 = VctpTransportConfig {
            bind_addr: "127.0.0.1:0".to_string(),
            encryption_passphrase: passphrase.to_string(),
            nonce: 42,
            ..Default::default()
        };
        let config2 = VctpTransportConfig {
            bind_addr: "127.0.0.1:0".to_string(),
            encryption_passphrase: passphrase.to_string(),
            nonce: 42,
            ..Default::default()
        };
        let t1 = VctpTransport::new(config1).unwrap();
        let t2 = VctpTransport::new(config2).unwrap();

        let t2_addr = t2.local_addr().unwrap();
        t1.add_peer(2, t2_addr);

        let payload = b"sensitive slab delta data".to_vec();
        t1.send_to_peer(2, 100, 64, payload.clone()).unwrap();

        std::thread::sleep(Duration::from_millis(10));

        let received = t2.recv_packets();
        assert_eq!(received.len(), 1);
        // Payload should be decrypted back to original
        assert_eq!(received[0].0.payload, payload);
    }

    #[test]
    fn test_stats_tracking() {
        let config1 = VctpTransportConfig {
            bind_addr: "127.0.0.1:0".to_string(),
            ..Default::default()
        };
        let config2 = VctpTransportConfig {
            bind_addr: "127.0.0.1:0".to_string(),
            ..Default::default()
        };
        let t1 = VctpTransport::new(config1).unwrap();
        let t2 = VctpTransport::new(config2).unwrap();
        let t2_addr = t2.local_addr().unwrap();

        t1.add_peer(2, t2_addr);
        t1.send_to_peer(2, 1, 0, vec![10]).unwrap();
        t1.send_to_peer(2, 2, 0, vec![20]).unwrap();

        let stats = t1.stats();
        assert_eq!(stats.packets_sent, 2);
        assert!(stats.bytes_sent > 0);
    }

    #[test]
    fn test_congestion_control() {
        let config = VctpTransportConfig {
            bind_addr: "127.0.0.1:0".to_string(),
            ..Default::default()
        };
        let transport = VctpTransport::new(config).unwrap();
        assert_eq!(transport.pacing_rate_mbps(), 100);

        transport.update_congestion(0); // No loss
        assert!(transport.pacing_rate_mbps() > 100);

        transport.update_congestion(10); // High loss
        assert!(transport.pacing_rate_mbps() < 120); // Should have decreased
    }

    #[test]
    fn test_shutdown() {
        let config = VctpTransportConfig {
            bind_addr: "127.0.0.1:0".to_string(),
            ..Default::default()
        };
        let transport = VctpTransport::new(config).unwrap();
        assert!(transport.is_running());
        transport.shutdown();
        assert!(!transport.is_running());
    }
}
