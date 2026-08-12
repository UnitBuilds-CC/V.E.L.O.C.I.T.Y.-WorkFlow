//! Real TCP/UDP network replication backend.
//! Provides actual socket-based replication for multi-cluster communication,
//! replacing the in-memory queue-based `ReplicationTransport` with real network I/O.

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use crate::cluster::{ReplicationTask, ReplicationTaskType};

// ─── Wire Protocol ────────────────────────────────────────────────────────────

/// Magic bytes for frame identification.
const FRAME_MAGIC: [u8; 4] = [0x56, 0x45, 0x4C, 0x4F]; // "VELO" in ASCII

/// Frame types for the replication wire protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    /// Handshake: cluster_id (u64) + failover_version (u64)
    Handshake = 1,
    /// Replication task batch
    TaskBatch = 2,
    /// Acknowledgement of received tasks
    Ack = 3,
    /// Heartbeat ping
    Ping = 4,
    /// Heartbeat pong
    Pong = 5,
    /// Graceful shutdown
    Shutdown = 6,
}

impl FrameType {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Handshake),
            2 => Some(Self::TaskBatch),
            3 => Some(Self::Ack),
            4 => Some(Self::Ping),
            5 => Some(Self::Pong),
            6 => Some(Self::Shutdown),
            _ => None,
        }
    }
}

/// A framed message on the wire.
#[derive(Debug, Clone)]
pub struct WireFrame {
    pub frame_type: FrameType,
    pub payload: Vec<u8>,
}

impl WireFrame {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4 + 1 + 4 + self.payload.len());
        buf.extend_from_slice(&FRAME_MAGIC);
        buf.push(self.frame_type as u8);
        buf.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 9 {
            return None;
        }
        if &data[0..4] != &FRAME_MAGIC {
            return None;
        }
        let frame_type = FrameType::from_u8(data[4])?;
        let payload_len = u32::from_be_bytes([data[5], data[6], data[7], data[8]]) as usize;
        if data.len() < 9 + payload_len {
            return None;
        }
        Some(Self {
            frame_type,
            payload: data[9..9 + payload_len].to_vec(),
        })
    }
}

// ─── TCP Replication Server ──────────────────────────────────────────────────

/// Configuration for a TCP replication listener.
#[derive(Debug, Clone)]
pub struct TcpReplicationConfig {
    pub bind_addr: String,
    pub cluster_id: u64,
    pub failover_version: u64,
    pub max_connections: usize,
    pub read_timeout_ms: u64,
    pub write_timeout_ms: u64,
}

impl Default for TcpReplicationConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9090".to_string(),
            cluster_id: 1,
            failover_version: 1,
            max_connections: 64,
            read_timeout_ms: 5000,
            write_timeout_ms: 5000,
        }
    }
}

/// Statistics for a TCP replication server.
#[derive(Debug, Clone, Default)]
pub struct TcpReplicationStats {
    pub connections_accepted: u64,
    pub connections_active: u64,
    pub frames_received: u64,
    pub frames_sent: u64,
    pub bytes_received: u64,
    pub bytes_sent: u64,
    pub tasks_received: u64,
    pub tasks_sent: u64,
    pub handshake_count: u64,
    pub ack_count: u64,
    pub errors: u64,
}

/// A TCP-based replication server that accepts incoming connections from remote clusters.
pub struct TcpReplicationServer {
    config: TcpReplicationConfig,
    listener: Option<TcpListener>,
    stats: RwLock<TcpReplicationStats>,
    incoming_tasks: Mutex<VecDeque<ReplicationTask>>,
    outgoing_tasks: Mutex<VecDeque<ReplicationTask>>,
    connected_peers: Mutex<Vec<SocketAddr>>,
}

impl TcpReplicationServer {
    /// Create a new TCP replication server (does not bind yet).
    pub fn new(config: TcpReplicationConfig) -> Self {
        Self {
            config,
            listener: None,
            stats: RwLock::new(TcpReplicationStats::default()),
            incoming_tasks: Mutex::new(VecDeque::new()),
            outgoing_tasks: Mutex::new(VecDeque::new()),
            connected_peers: Mutex::new(Vec::new()),
        }
    }

    /// Bind the TCP listener to the configured address.
    pub fn bind(&mut self) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(&self.config.bind_addr)?;
        // Use blocking mode with a short timeout so accept() waits briefly for connections
        let _ = listener.set_nonblocking(false);
        self.listener = Some(listener);
        Ok(())
    }

    /// Accept a single incoming connection (with timeout).
    /// Returns (peer_address, accepted_stream) if a connection was accepted.
    pub fn accept_one(&self) -> Option<(SocketAddr, TcpStream)> {
        let listener = self.listener.as_ref()?;
        // Set a temporary read timeout on the listener for this accept call
        let _ = listener.set_nonblocking(true);
        match listener.accept() {
            Ok((stream, addr)) => {
                // Make the accepted stream blocking with timeout
                let _ = stream.set_nonblocking(false);
                let _ = stream
                    .set_read_timeout(Some(Duration::from_millis(self.config.read_timeout_ms)));
                let _ = stream
                    .set_write_timeout(Some(Duration::from_millis(self.config.write_timeout_ms)));
                self.connected_peers.lock().unwrap().push(addr);
                self.stats.write().unwrap().connections_accepted += 1;
                self.stats.write().unwrap().connections_active += 1;
                Some((addr, stream))
            }
            Err(_) => None,
        }
    }

    /// Send a handshake frame to a connected peer.
    pub fn send_handshake(&self, stream: &mut TcpStream) -> Result<(), std::io::Error> {
        let mut payload = Vec::with_capacity(16);
        payload.extend_from_slice(&self.config.cluster_id.to_be_bytes());
        payload.extend_from_slice(&self.config.failover_version.to_be_bytes());

        let frame = WireFrame {
            frame_type: FrameType::Handshake,
            payload,
        };
        let encoded = frame.encode();
        let written = stream.write(&encoded)?;
        self.stats.write().unwrap().frames_sent += 1;
        self.stats.write().unwrap().bytes_sent += written as u64;
        Ok(())
    }

    /// Send a batch of replication tasks to a connected peer.
    pub fn send_task_batch(
        &self,
        stream: &mut TcpStream,
        tasks: &[ReplicationTask],
    ) -> Result<usize, std::io::Error> {
        let payload = encode_tasks(tasks);
        let frame = WireFrame {
            frame_type: FrameType::TaskBatch,
            payload,
        };
        let encoded = frame.encode();
        let written = stream.write(&encoded)?;
        self.stats.write().unwrap().frames_sent += 1;
        self.stats.write().unwrap().bytes_sent += written as u64;
        self.stats.write().unwrap().tasks_sent += tasks.len() as u64;
        Ok(written)
    }

    /// Receive and decode a frame from a TCP stream.
    pub fn receive_frame(&self, stream: &mut TcpStream) -> Option<WireFrame> {
        let mut header = [0u8; 9];
        match stream.read_exact(&mut header) {
            Ok(_) => {}
            Err(_) => return None,
        }
        if &header[0..4] != &FRAME_MAGIC {
            return None;
        }
        let frame_type = FrameType::from_u8(header[4])?;
        let payload_len = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) as usize;

        let mut payload = vec![0u8; payload_len];
        if payload_len > 0 {
            match stream.read_exact(&mut payload) {
                Ok(_) => {}
                Err(_) => return None,
            }
        }

        self.stats.write().unwrap().frames_received += 1;
        self.stats.write().unwrap().bytes_received += (9 + payload_len) as u64;

        Some(WireFrame {
            frame_type,
            payload,
        })
    }

    /// Enqueue a task for outgoing delivery.
    pub fn enqueue_outgoing(&self, task: ReplicationTask) {
        self.outgoing_tasks.lock().unwrap().push_back(task);
    }

    /// Drain outgoing tasks for delivery.
    pub fn drain_outgoing(&self, max_count: usize) -> Vec<ReplicationTask> {
        let mut tasks = self.outgoing_tasks.lock().unwrap();
        let count = max_count.min(tasks.len());
        tasks.drain(..count).collect()
    }

    /// Push received tasks into the incoming queue.
    pub fn push_incoming(&self, tasks: Vec<ReplicationTask>) {
        let mut incoming = self.incoming_tasks.lock().unwrap();
        self.stats.write().unwrap().tasks_received += tasks.len() as u64;
        for task in tasks {
            incoming.push_back(task);
        }
    }

    /// Drain incoming tasks for local processing.
    pub fn drain_incoming(&self, max_count: usize) -> Vec<ReplicationTask> {
        let mut tasks = self.incoming_tasks.lock().unwrap();
        let count = max_count.min(tasks.len());
        tasks.drain(..count).collect()
    }

    /// Get replication statistics.
    pub fn stats(&self) -> TcpReplicationStats {
        self.stats.read().unwrap().clone()
    }

    /// Get connected peer addresses.
    pub fn connected_peers(&self) -> Vec<SocketAddr> {
        self.connected_peers.lock().unwrap().clone()
    }

    /// Get the bound address (if bound).
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.listener.as_ref().and_then(|l| l.local_addr().ok())
    }
}

// ─── UDP Replication Transport ───────────────────────────────────────────────

/// Configuration for UDP-based replication (lower latency, no guaranteed delivery).
#[derive(Debug, Clone)]
pub struct UdpReplicationConfig {
    pub bind_addr: String,
    pub peer_addr: String,
    pub cluster_id: u64,
    pub max_packet_size: usize,
}

impl Default for UdpReplicationConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9091".to_string(),
            peer_addr: "127.0.0.1:9092".to_string(),
            cluster_id: 1,
            max_packet_size: 65536,
        }
    }
}

/// Statistics for UDP replication.
#[derive(Debug, Clone, Default)]
pub struct UdpReplicationStats {
    pub packets_sent: u64,
    pub packets_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub tasks_sent: u64,
    pub tasks_received: u64,
    pub errors: u64,
}

/// UDP-based replication transport for low-latency cluster communication.
/// Uses datagrams — no connection setup, but no guaranteed delivery.
pub struct UdpReplicationTransport {
    socket: Option<UdpSocket>,
    config: UdpReplicationConfig,
    stats: RwLock<UdpReplicationStats>,
}

impl UdpReplicationTransport {
    pub fn new(config: UdpReplicationConfig) -> Self {
        Self {
            socket: None,
            config,
            stats: RwLock::new(UdpReplicationStats::default()),
        }
    }

    /// Bind the UDP socket.
    pub fn bind(&mut self) -> Result<(), std::io::Error> {
        let socket = UdpSocket::bind(&self.config.bind_addr)?;
        socket.set_nonblocking(true)?;
        socket.set_read_timeout(Some(Duration::from_millis(100)))?;
        self.socket = Some(socket);
        Ok(())
    }

    /// Send a replication task to the configured peer.
    pub fn send_task(&self, task: &ReplicationTask) -> Result<usize, std::io::Error> {
        let socket = self.socket.as_ref().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "UDP socket not bound")
        })?;

        let tasks = [task.clone()];
        let payload = encode_tasks(&tasks);
        let frame = WireFrame {
            frame_type: FrameType::TaskBatch,
            payload,
        };
        let encoded = frame.encode();
        let sent = socket.send_to(&encoded, &self.config.peer_addr)?;

        let mut stats = self.stats.write().unwrap();
        stats.packets_sent += 1;
        stats.bytes_sent += sent as u64;
        stats.tasks_sent += 1;

        Ok(sent)
    }

    /// Receive a frame from any peer (non-blocking).
    pub fn receive_frame(&self) -> Option<(WireFrame, SocketAddr)> {
        let socket = self.socket.as_ref()?;
        let mut buf = vec![0u8; self.config.max_packet_size];
        match socket.recv_from(&mut buf) {
            Ok((size, addr)) => {
                let frame = WireFrame::decode(&buf[..size])?;
                let mut stats = self.stats.write().unwrap();
                stats.packets_received += 1;
                stats.bytes_received += size as u64;
                Some((frame, addr))
            }
            Err(_) => None,
        }
    }

    /// Get statistics.
    pub fn stats(&self) -> UdpReplicationStats {
        self.stats.read().unwrap().clone()
    }

    /// Get the local address.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.socket.as_ref().and_then(|s| s.local_addr().ok())
    }
}

// ─── Task Encoding/Decoding ──────────────────────────────────────────────────

/// Encode replication tasks into a binary payload.
pub fn encode_tasks(tasks: &[ReplicationTask]) -> Vec<u8> {
    let mut buf = Vec::new();
    // Count
    buf.extend_from_slice(&(tasks.len() as u32).to_be_bytes());
    for task in tasks {
        buf.extend_from_slice(&task.task_id.to_be_bytes());
        buf.extend_from_slice(&task.source_cluster_id.to_be_bytes());
        buf.extend_from_slice(&task.target_cluster_id.to_be_bytes());
        buf.extend_from_slice(&task.workflow_key.to_be_bytes());
        buf.extend_from_slice(&task.event_type.to_be_bytes());
        buf.extend_from_slice(&(task.payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&task.payload);
        buf.extend_from_slice(&task.failover_version.to_be_bytes());
        buf.push(task.task_type as u8);
        buf.extend_from_slice(&task.first_event_id.to_be_bytes());
        buf.extend_from_slice(&task.last_event_id.to_be_bytes());
        buf.extend_from_slice(&task.created_ms.to_be_bytes());
    }
    buf
}

/// Decode replication tasks from a binary payload.
pub fn decode_tasks(data: &[u8]) -> Vec<ReplicationTask> {
    if data.len() < 4 {
        return Vec::new();
    }
    let count = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let mut tasks = Vec::with_capacity(count);
    let mut offset = 4;

    for _ in 0..count {
        if offset + 72 > data.len() {
            break;
        } // minimum task size

        let task_id = u64::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let source_cluster_id = u64::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let target_cluster_id = u64::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let workflow_key = u64::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let event_type = u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;
        let payload_len = u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if offset + payload_len + 32 > data.len() {
            break;
        }

        let payload = data[offset..offset + payload_len].to_vec();
        offset += payload_len;
        let failover_version = u64::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let task_type_byte = data[offset];
        offset += 1;
        let first_event_id = u64::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let last_event_id = u64::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let created_ms = u64::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let task_type = match task_type_byte {
            0 => ReplicationTaskType::SyncHistory,
            1 => ReplicationTaskType::SyncActivity,
            2 => ReplicationTaskType::SyncWorkflowState,
            3 => ReplicationTaskType::NamespaceMetadata,
            4 => ReplicationTaskType::SyncHSM,
            5 => ReplicationTaskType::VerifyTransition,
            6 => ReplicationTaskType::DeleteExecution,
            7 => ReplicationTaskType::BackfillHistory,
            8 => ReplicationTaskType::SyncVersionedTransition,
            _ => ReplicationTaskType::SyncHistory,
        };

        tasks.push(ReplicationTask {
            task_id,
            source_cluster_id,
            target_cluster_id,
            workflow_key,
            event_type,
            payload,
            failover_version,
            task_type,
            first_event_id,
            last_event_id,
            created_ms,
        });
    }
    tasks
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::ReplicationTaskType;

    fn make_task(workflow_key: u64, event_id: u64) -> ReplicationTask {
        ReplicationTask {
            task_id: event_id,
            source_cluster_id: 1,
            target_cluster_id: 2,
            workflow_key,
            event_type: 1,
            payload: vec![1, 2, 3, 4],
            failover_version: 1,
            task_type: ReplicationTaskType::SyncHistory,
            first_event_id: event_id,
            last_event_id: event_id,
            created_ms: 1000,
        }
    }

    #[test]
    fn test_wire_frame_encode_decode() {
        let frame = WireFrame {
            frame_type: FrameType::Handshake,
            payload: vec![1, 2, 3, 4, 5],
        };
        let encoded = frame.encode();
        let decoded = WireFrame::decode(&encoded).unwrap();
        assert_eq!(decoded.frame_type, FrameType::Handshake);
        assert_eq!(decoded.payload, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_wire_frame_invalid_magic() {
        let data = [0xFF, 0xFF, 0xFF, 0xFF, 1, 0, 0, 0, 0];
        assert!(WireFrame::decode(&data).is_none());
    }

    #[test]
    fn test_wire_frame_too_short() {
        assert!(WireFrame::decode(&[0; 5]).is_none());
    }

    #[test]
    fn test_encode_decode_tasks() {
        let tasks = vec![make_task(100, 1), make_task(200, 2), make_task(300, 3)];
        let encoded = encode_tasks(&tasks);
        let decoded = decode_tasks(&encoded);
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].workflow_key, 100);
        assert_eq!(decoded[1].workflow_key, 200);
        assert_eq!(decoded[2].workflow_key, 300);
        assert_eq!(decoded[0].payload, vec![1, 2, 3, 4]);
        assert_eq!(decoded[0].task_type, ReplicationTaskType::SyncHistory);
    }

    #[test]
    fn test_encode_decode_empty_tasks() {
        let tasks: Vec<ReplicationTask> = vec![];
        let encoded = encode_tasks(&tasks);
        let decoded = decode_tasks(&encoded);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_tcp_server_bind_and_stats() {
        let config = TcpReplicationConfig {
            bind_addr: "127.0.0.1:0".to_string(), // OS assigns port
            cluster_id: 1,
            failover_version: 1,
            max_connections: 16,
            read_timeout_ms: 1000,
            write_timeout_ms: 1000,
        };
        let mut server = TcpReplicationServer::new(config);
        server.bind().unwrap();
        let addr = server.local_addr().unwrap();
        assert!(addr.port() > 0);

        let stats = server.stats();
        assert_eq!(stats.connections_accepted, 0);
        assert_eq!(stats.frames_sent, 0);
    }

    #[test]
    fn test_tcp_server_enqueue_drain() {
        let config = TcpReplicationConfig::default();
        let server = TcpReplicationServer::new(config);

        server.enqueue_outgoing(make_task(100, 1));
        server.enqueue_outgoing(make_task(200, 2));

        let drained = server.drain_outgoing(10);
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].workflow_key, 100);
    }

    #[test]
    fn test_tcp_server_push_drain_incoming() {
        let config = TcpReplicationConfig::default();
        let server = TcpReplicationServer::new(config);

        server.push_incoming(vec![make_task(300, 5), make_task(400, 6)]);
        let drained = server.drain_incoming(10);
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].workflow_key, 300);
    }

    #[test]
    fn test_tcp_handshake_and_batch_over_loopback() {
        // Bind server
        let config = TcpReplicationConfig {
            bind_addr: "127.0.0.1:0".to_string(),
            cluster_id: 1,
            failover_version: 42,
            ..Default::default()
        };
        let mut server = TcpReplicationServer::new(config);
        server.bind().unwrap();
        let addr = server.local_addr().unwrap();

        // Client connects
        let mut client = TcpStream::connect(addr).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        client
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        // Brief pause to ensure connection is in the accept queue
        std::thread::sleep(Duration::from_millis(50));

        // Accept connection — get the server-side stream
        let accepted = server.accept_one();
        assert!(accepted.is_some());
        let (_peer_addr, mut server_stream) = accepted.unwrap();
        assert_eq!(server.stats().connections_accepted, 1);

        // Send handshake from server to client via the accepted stream
        server.send_handshake(&mut server_stream).unwrap();
        assert_eq!(server.stats().frames_sent, 1);

        // Client reads the handshake frame
        let mut header = [0u8; 9];
        client.read_exact(&mut header).unwrap();
        let frame_type = header[4];
        let payload_len = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) as usize;
        let mut payload = vec![0u8; payload_len];
        client.read_exact(&mut payload).unwrap();

        assert_eq!(frame_type, FrameType::Handshake as u8);
        // Decode cluster_id and failover_version
        let cluster_id = u64::from_be_bytes(payload[0..8].try_into().unwrap());
        let failover_version = u64::from_be_bytes(payload[8..16].try_into().unwrap());
        assert_eq!(cluster_id, 1);
        assert_eq!(failover_version, 42);
    }

    #[test]
    fn test_tcp_send_task_batch_over_loopback() {
        let config = TcpReplicationConfig {
            bind_addr: "127.0.0.1:0".to_string(),
            ..Default::default()
        };
        let mut server = TcpReplicationServer::new(config);
        server.bind().unwrap();
        let addr = server.local_addr().unwrap();

        let mut client = TcpStream::connect(addr).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        client
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        std::thread::sleep(Duration::from_millis(50));
        let accepted = server.accept_one();
        assert!(accepted.is_some());
        let (_peer_addr, mut server_stream) = accepted.unwrap();

        // Send a batch of tasks via the server-side stream
        let tasks = vec![make_task(500, 10), make_task(600, 11)];
        let written = server.send_task_batch(&mut server_stream, &tasks).unwrap();
        assert!(written > 0);
        assert_eq!(server.stats().tasks_sent, 2);

        // Client reads and decodes
        let mut header = [0u8; 9];
        client.read_exact(&mut header).unwrap();
        let payload_len = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) as usize;
        let mut payload = vec![0u8; payload_len];
        client.read_exact(&mut payload).unwrap();

        let decoded = decode_tasks(&payload);
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].workflow_key, 500);
        assert_eq!(decoded[1].workflow_key, 600);
    }

    #[test]
    fn test_udp_bind_and_stats() {
        let config = UdpReplicationConfig {
            bind_addr: "127.0.0.1:0".to_string(),
            peer_addr: "127.0.0.1:0".to_string(),
            cluster_id: 1,
            max_packet_size: 65536,
        };
        let mut transport = UdpReplicationTransport::new(config);
        transport.bind().unwrap();
        let addr = transport.local_addr().unwrap();
        assert!(addr.port() > 0);

        let stats = transport.stats();
        assert_eq!(stats.packets_sent, 0);
        assert_eq!(stats.packets_received, 0);
    }

    #[test]
    fn test_udp_send_receive_loopback() {
        // Bind receiver
        let recv_config = UdpReplicationConfig {
            bind_addr: "127.0.0.1:0".to_string(),
            peer_addr: "127.0.0.1:0".to_string(),
            cluster_id: 2,
            max_packet_size: 65536,
        };
        let mut receiver = UdpReplicationTransport::new(recv_config);
        receiver.bind().unwrap();
        let recv_addr = receiver.local_addr().unwrap();

        // Bind sender
        let send_config = UdpReplicationConfig {
            bind_addr: "127.0.0.1:0".to_string(),
            peer_addr: recv_addr.to_string(),
            cluster_id: 1,
            max_packet_size: 65536,
        };
        let mut sender = UdpReplicationTransport::new(send_config);
        sender.bind().unwrap();

        // Send a task
        let task = make_task(700, 20);
        let sent = sender.send_task(&task).unwrap();
        assert!(sent > 0);
        assert_eq!(sender.stats().packets_sent, 1);

        // Receive the frame
        let (frame, from_addr) = receiver.receive_frame().unwrap();
        assert_eq!(frame.frame_type, FrameType::TaskBatch);
        assert_eq!(from_addr, sender.local_addr().unwrap());

        // Decode tasks from the frame
        let decoded = decode_tasks(&frame.payload);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].workflow_key, 700);
    }

    #[test]
    fn test_frame_type_roundtrip() {
        for ft in [
            FrameType::Handshake,
            FrameType::TaskBatch,
            FrameType::Ack,
            FrameType::Ping,
            FrameType::Pong,
            FrameType::Shutdown,
        ] {
            assert_eq!(FrameType::from_u8(ft as u8), Some(ft));
        }
        assert_eq!(FrameType::from_u8(255), None);
    }
}
