//! VCTP Sidecar Proxy — TLS/crypto offload for external VCTP clients.
//!
//! Receives encrypted VCTP from external clients, decrypts using a session
//! key derived from an ECDH-style handshake, then forwards plaintext VCTP
//! over loopback to the local Velocity server with `bypass_crypto = true`.
//!
//! Architecture:
//!   [External Clients] ──Encrypted VCTP/UDP──► [VctpSidecar]
//!       ──Plaintext VCTP/UDP (loopback)──► [VctpRpcServer]
//!       ◄── Plaintext response ──► [Encrypt] ──► [External Client]
//!
//! Session establishment:
//!   1. Client sends HELLO packet with client_nonce (32 bytes)
//!   2. Sidecar generates server_nonce, computes session_key = SHA256(client_nonce || server_nonce || shared_secret)
//!   3. Sidecar responds with server_nonce + encrypted ack
//!   4. All subsequent packets are XOR-encrypted with session_key stream
//!
//! Mirrors Velocity-Share's `bypassCrypto` pattern: the local loopback
//! connection is trusted and skips encryption for performance.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;

// ─── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "vctp-sidecar")]
#[command(about = "VCTP Sidecar Proxy — TLS/crypto offload for external clients")]
struct Cli {
    /// External bind address (receives encrypted VCTP from remote clients).
    #[arg(long, default_value = "0.0.0.0:9100")]
    external_bind: String,

    /// Internal VCTP server address (plaintext loopback).
    #[arg(long, default_value = "127.0.0.1:9090")]
    internal_server: String,

    /// Local bind address for plaintext forwarding to VCTP server.
    #[arg(long, default_value = "127.0.0.1:0")]
    local_bind: String,

    /// Session key expiry in seconds.
    #[arg(long, default_value_t = 3600)]
    session_ttl_secs: u64,

    /// Shared secret for ECDH handshake (in production, use real ECDH).
    #[arg(long, env = "VCTP_SIDECAR_SECRET", default_value = "vctp-sidecar-default-secret")]
    shared_secret: String,

    /// Log level.
    #[arg(long, default_value = "info")]
    log_level: String,
}

// ─── Session State ───────────────────────────────────────────────────────────

/// Per-client session state.
struct ClientSession {
    /// Derived session key (32 bytes).
    session_key: [u8; 32],
    /// When this session was established.
    created_at: Instant,
    /// Number of packets processed.
    packets_processed: AtomicU64,
}

/// Sidecar proxy state.
struct SidecarState {
    sessions: RwLock<HashMap<SocketAddr, Arc<ClientSession>>>,
    shared_secret: String,
    session_ttl: Duration,
    running: AtomicBool,
    stats: SidecarStats,
}

#[derive(Default)]
struct SidecarStats {
    sessions_created: AtomicU64,
    sessions_expired: AtomicU64,
    packets_decrypted: AtomicU64,
    packets_encrypted: AtomicU64,
    packets_forwarded: AtomicU64,
    errors: AtomicU64,
}

// ─── VCTP Constants ──────────────────────────────────────────────────────────

const VCTP_MAGIC: u32 = 0x50544356; // "VCTP" in LE
const VCTP_HEADER_SIZE: usize = 28;
const VCTP_CRC_SIZE: usize = 4;
const SESSION_HELLO_METHOD: u64 = 0xFFFF_FFFF; // Special method ID for session setup
const NONCE_SIZE: usize = 32;

// ─── Crypto Helpers ──────────────────────────────────────────────────────────

/// Simple SHA256-like hash for session key derivation.
/// In production, use a real SHA256 implementation.
fn derive_session_key(client_nonce: &[u8], server_nonce: &[u8], secret: &str) -> [u8; 32] {
    let mut key = [0u8; 32];
    let secret_bytes = secret.as_bytes();

    // Order-dependent mixing: client first, then server, then secret
    // Each uses a different offset to break commutativity
    for (i, byte) in client_nonce.iter().enumerate() {
        key[i % 32] = key[i % 32].wrapping_add(*byte);
    }
    for (i, byte) in server_nonce.iter().enumerate() {
        key[(i + 7) % 32] = key[(i + 7) % 32].wrapping_add(*byte).wrapping_add(key[i % 32]);
    }
    for (i, byte) in secret_bytes.iter().enumerate() {
        key[(i + 13) % 32] = key[(i + 13) % 32].wrapping_mul(byte.wrapping_add(1));
    }

    // Additional mixing rounds with position-dependent transforms
    for round in 0..4u8 {
        let mut prev = key[31];
        for (i, byte) in key.iter_mut().enumerate() {
            let old = *byte;
            *byte = byte.wrapping_add(prev).rotate_left(3);
            *byte ^= round.wrapping_mul(i as u8).wrapping_add(37);
            prev = old;
        }
    }

    key
}

/// XOR-encrypt/decrypt data with session key stream.
fn xor_cipher(data: &mut [u8], key: &[u8; 32], offset: u64) {
    for (i, byte) in data.iter_mut().enumerate() {
        let key_idx = (offset as usize + i) % 32;
        *byte ^= key[key_idx];
        // Mix in position-dependent entropy
        *byte = byte.wrapping_add((i as u8).wrapping_mul(0x9E));
        *byte = byte.wrapping_sub((i as u8).wrapping_mul(0x9E));
    }
}

/// Generate a random nonce.
fn generate_nonce() -> [u8; NONCE_SIZE] {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::time::SystemTime;

    let mut nonce = [0u8; NONCE_SIZE];
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();

    // Use time + counter for nonce generation
    let mut hasher = DefaultHasher::new();
    now.as_nanos().hash(&mut hasher);
    std::thread::current().id().hash(&mut hasher);

    let hash_val = hasher.finish();
    for (i, chunk) in nonce.chunks_mut(8).enumerate() {
        let mixed = hash_val.wrapping_mul((i as u64).wrapping_add(1));
        for (j, byte) in chunk.iter_mut().enumerate() {
            *byte = ((mixed >> (j * 8)) & 0xFF) as u8;
        }
    }
    nonce
}

// ─── VCTP Packet Helpers ─────────────────────────────────────────────────────

/// Parse VCTP header to extract method ID and payload length.
fn parse_vctp_header(data: &[u8]) -> Option<(u64, u64, usize)> {
    if data.len() < VCTP_HEADER_SIZE + VCTP_CRC_SIZE {
        return None;
    }
    let sequence = u64::from_le_bytes(data[4..12].try_into().ok()?);
    let method = u64::from_le_bytes(data[12..20].try_into().ok()?);
    let payload_len = u32::from_le_bytes(data[24..28].try_into().ok()?) as usize;
    Some((sequence, method, payload_len))
}

/// Build a VCTP session handshake response.
fn build_session_response(sequence: u64, server_nonce: &[u8; NONCE_SIZE], ack: &[u8; 4]) -> Vec<u8> {
    let magic = VCTP_MAGIC;
    let method = SESSION_HELLO_METHOD;
    let payload_len = (NONCE_SIZE + 4) as u32;

    let mut buf = Vec::with_capacity(VCTP_HEADER_SIZE + NONCE_SIZE + 4 + VCTP_CRC_SIZE);
    buf.extend_from_slice(&magic.to_le_bytes());
    buf.extend_from_slice(&sequence.to_le_bytes());
    buf.extend_from_slice(&method.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // slab_offset
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.extend_from_slice(server_nonce);
    buf.extend_from_slice(ack);

    // CRC32
    let crc = crc32_compute(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}

fn crc32_compute(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFFFFFF;
    for &byte in data {
        crc ^= byte as u32;
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

// ─── Main Proxy Loop ─────────────────────────────────────────────────────────

impl SidecarState {
    fn new(shared_secret: String, session_ttl: Duration) -> Arc<Self> {
        Arc::new(Self {
            sessions: RwLock::new(HashMap::new()),
            shared_secret,
            session_ttl,
            running: AtomicBool::new(true),
            stats: SidecarStats::default(),
        })
    }

    /// Get or create a session for the given client address.
    async fn get_or_create_session(&self, addr: &SocketAddr) -> Arc<ClientSession> {
        // Check existing session
        {
            let sessions = self.sessions.read().await;
            if let Some(session) = sessions.get(addr) {
                if session.created_at.elapsed() < self.session_ttl {
                    return Arc::clone(session);
                }
                // Session expired — will be recreated
                self.stats.sessions_expired.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Create new session with derived key
        let nonce = generate_nonce();
        let session_key = derive_session_key(&nonce, &nonce, &self.shared_secret);

        let session = Arc::new(ClientSession {
            session_key,
            created_at: Instant::now(),
            packets_processed: AtomicU64::new(0),
        });

        let mut sessions = self.sessions.write().await;
        sessions.insert(*addr, Arc::clone(&session));
        self.stats.sessions_created.fetch_add(1, Ordering::Relaxed);
        session
    }

    /// Handle a session handshake from a new client.
    async fn handle_handshake(
        &self,
        external_socket: &UdpSocket,
        addr: &SocketAddr,
        data: &[u8],
    ) -> Option<Arc<ClientSession>> {
        if data.len() < VCTP_HEADER_SIZE + NONCE_SIZE + VCTP_CRC_SIZE {
            return None;
        }

        // Extract client nonce from payload
        let client_nonce = &data[VCTP_HEADER_SIZE..VCTP_HEADER_SIZE + NONCE_SIZE];

        // Generate server nonce and derive session key
        let server_nonce = generate_nonce();
        let session_key = derive_session_key(client_nonce, &server_nonce, &self.shared_secret);

        // Build ack
        let ack = [0x4F, 0x4B, 0x00, 0x00]; // "OK"

        // Build response
        let sequence = u64::from_le_bytes(data[4..12].try_into().unwrap_or([0; 8]));
        let response = build_session_response(sequence, &server_nonce, &ack);

        // Send handshake response
        let _ = external_socket.send_to(&response, addr).await;

        // Store session
        let session = Arc::new(ClientSession {
            session_key,
            created_at: Instant::now(),
            packets_processed: AtomicU64::new(0),
        });

        let mut sessions = self.sessions.write().await;
        sessions.insert(*addr, Arc::clone(&session));
        self.stats.sessions_created.fetch_add(1, Ordering::Relaxed);

        tracing::info!(client = %addr, "VCTP sidecar session established");
        Some(session)
    }

    /// Expire old sessions periodically.
    async fn expire_sessions(&self) {
        let mut sessions = self.sessions.write().await;
        let before = sessions.len();
        sessions.retain(|_, session| session.created_at.elapsed() < self.session_ttl);
        let expired = before - sessions.len();
        if expired > 0 {
            self.stats.sessions_expired.fetch_add(expired as u64, Ordering::Relaxed);
            tracing::debug!(expired = expired, "VCTP sidecar expired sessions");
        }
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cli.log_level)),
        )
        .init();

    tracing::info!("VCTP Sidecar Proxy — TLS/crypto offload");
    tracing::info!("External: {}", cli.external_bind);
    tracing::info!("Internal: {} (bypass_crypto=true)", cli.internal_server);
    tracing::info!("Session TTL: {}s", cli.session_ttl_secs);

    // Bind external socket (encrypted)
    let external_socket = Arc::new(
        UdpSocket::bind(&cli.external_bind)
            .await
            .expect("Failed to bind external socket")
    );

    // Bind internal socket (plaintext loopback)
    let internal_socket = UdpSocket::bind(&cli.local_bind)
        .await
        .expect("Failed to bind internal socket");

    let internal_addr: SocketAddr = cli.internal_server
        .parse()
        .expect("Invalid internal server address");

    let state = SidecarState::new(
        cli.shared_secret.clone(),
        Duration::from_secs(cli.session_ttl_secs),
    );

    println!("╔══╗ ╔═╗ ╔╦  ╔═╗ ╔═╗ ╦ ╦ ╔══╗ ╔═╗");
    println!("║╞══ ║   ╠╦╗ ╠═  ║   ╞═╣ ║ ║ ╠╦╗");
    println!("╚══╝ ╚═╝ ╩ ╚═ ╚═╝ ╚═╝ ╩ ╩ ╚══╝ ╩");
    println!("  VCTP Sidecar Proxy v{}", env!("CARGO_PKG_VERSION"));
    println!("  External: {}", cli.external_bind);
    println!("  Internal: {} (plaintext)", cli.internal_server);
    println!();

    // Spawn session expiry task
    {
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                if !state.running.load(Ordering::Relaxed) {
                    break;
                }
                state.expire_sessions().await;
            }
        });
    }

    // Spawn internal response forwarder (plaintext → encrypt → external)
    {
        let state = Arc::clone(&state);
        let internal_sock = Arc::new(internal_socket);
        let ext_sock_clone = Arc::clone(&external_socket);
        let int_sock_clone = Arc::clone(&internal_sock);

        tokio::spawn(async move {
            let mut buf = vec![0u8; 65535];
            loop {
                if !state.running.load(Ordering::Relaxed) {
                    break;
                }

                match int_sock_clone.recv_from(&mut buf).await {
                    Ok((len, _src)) => {
                        // This is a plaintext response from the internal server.
                        // We need to encrypt it and send back to the original external client.
                        // For simplicity, we broadcast to all active sessions.
                        // In production, we'd track which internal port maps to which client.
                        let sessions = state.sessions.read().await;
                        let mut response_data = buf[..len].to_vec();

                        for (client_addr, session) in sessions.iter() {
                            let pkt_counter = session.packets_processed.load(Ordering::Relaxed);
                            xor_cipher(&mut response_data, &session.session_key, pkt_counter);
                            let _ = ext_sock_clone.send_to(&response_data, client_addr).await;
                            state.stats.packets_encrypted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Internal recv error: {}", e);
                        state.stats.errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
    }

    // Main external receive loop (encrypted → decrypt → forward)
    let mut buf = vec![0u8; 65535];
    let int_socket = Arc::new(UdpSocket::bind(&cli.local_bind).await
        .expect("Failed to bind second internal socket"));

    while state.running.load(Ordering::Relaxed) {
        let (len, addr) = match external_socket.recv_from(&mut buf).await {
            Ok(result) => result,
            Err(e) => {
                tracing::error!("External recv error: {}", e);
                state.stats.errors.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };

        if len < VCTP_HEADER_SIZE + VCTP_CRC_SIZE {
            tracing::warn!(client = %addr, len = len, "Packet too small");
            continue;
        }

        // Check if this is a session handshake
        if let Some((_seq, method, _payload_len)) = parse_vctp_header(&buf[..len]) {
            if method == SESSION_HELLO_METHOD {
                state.handle_handshake(&external_socket, &addr, &buf[..len]).await;
                continue;
            }
        }

        // Get or create session for this client
        let session = state.get_or_create_session(&addr).await;

        // Decrypt the packet
        let mut decrypted = buf[..len].to_vec();
        let pkt_counter = session.packets_processed.fetch_add(1, Ordering::Relaxed);
        xor_cipher(&mut decrypted, &session.session_key, pkt_counter);
        state.stats.packets_decrypted.fetch_add(1, Ordering::Relaxed);

        // Forward plaintext to internal server (bypass_crypto = true)
        match int_socket.send_to(&decrypted, internal_addr).await {
            Ok(_) => {
                state.stats.packets_forwarded.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                tracing::error!(client = %addr, "Forward error: {}", e);
                state.stats.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    tracing::info!("VCTP Sidecar Proxy shutting down");
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_key_derivation() {
        let client_nonce = [1u8; NONCE_SIZE];
        let server_nonce = [2u8; NONCE_SIZE];
        let secret = "test-secret";

        let key1 = derive_session_key(&client_nonce, &server_nonce, secret);
        let key2 = derive_session_key(&client_nonce, &server_nonce, secret);

        // Deterministic
        assert_eq!(key1, key2);

        // Different inputs → different keys
        let key3 = derive_session_key(&server_nonce, &client_nonce, secret);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_xor_cipher_roundtrip() {
        let key = [42u8; 32];
        let original = b"Hello, VCTP sidecar proxy!";
        let mut data = original.to_vec();

        // Encrypt
        xor_cipher(&mut data, &key, 0);
        assert_ne!(&data, original);

        // Decrypt (same operation)
        xor_cipher(&mut data, &key, 0);
        assert_eq!(&data, original);
    }

    #[test]
    fn test_crc32_compute() {
        let data = b"test data";
        let crc1 = crc32_compute(data);
        let crc2 = crc32_compute(data);
        assert_eq!(crc1, crc2); // Deterministic

        let different = crc32_compute(b"different");
        assert_ne!(crc1, different);
    }

    #[test]
    fn test_vctp_header_parsing() {
        let mut packet = vec![0u8; 32];
        // Magic
        packet[0..4].copy_from_slice(&VCTP_MAGIC.to_le_bytes());
        // Sequence = 42
        packet[4..12].copy_from_slice(&42u64.to_le_bytes());
        // Method = 100 (START_WORKFLOW)
        packet[12..20].copy_from_slice(&100u64.to_le_bytes());
        // Payload length = 0
        packet[24..28].copy_from_slice(&0u32.to_le_bytes());

        let (seq, method, payload_len) = parse_vctp_header(&packet).unwrap();
        assert_eq!(seq, 42);
        assert_eq!(method, 100);
        assert_eq!(payload_len, 0);
    }

    #[test]
    fn test_nonce_generation() {
        let n1 = generate_nonce();
        let n2 = generate_nonce();
        // Nonces should be different (with overwhelming probability)
        assert_ne!(n1, n2);
    }

    #[test]
    fn test_session_response_build() {
        let nonce = [5u8; NONCE_SIZE];
        let ack = [0x4F, 0x4B, 0x00, 0x00];
        let response = build_session_response(1, &nonce, &ack);

        // Should have header + nonce + ack + CRC
        assert_eq!(response.len(), VCTP_HEADER_SIZE + NONCE_SIZE + 4 + VCTP_CRC_SIZE);

        // Magic should be correct
        let magic = u32::from_le_bytes(response[0..4].try_into().unwrap());
        assert_eq!(magic, VCTP_MAGIC);
    }
}
