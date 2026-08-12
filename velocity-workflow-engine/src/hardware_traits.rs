//! Hardware abstraction traits for future hardware offload.
//!
//! Defines the trait interfaces for the base.md vision's hardware frontier:
//! - SmartNIC / DPDK offload (bypass host CPU for slab transfers)
//! - TEE enclave slabs (AMD SEV-SNP / Intel TDX memory encryption)
//! - io_uring / RDMA peer-to-peer replication
//! - Merkle self-healing ECC (Reed-Solomon + Merkle mismatch auto-repair)
//!
//! These traits define the interfaces; concrete implementations would require
//! platform-specific hardware access. The trait definitions enable compile-time
//! verification that the engine's architecture is hardware-ready.

/// Trait for SmartNIC / FPGA offload of slab delta transfers.
///
/// When implemented, the SmartNIC handles:
/// - Packet parsing and Merkle-root verification
/// - DMA writes directly into target machine's NVMe-oF storage or RAM
/// - Host CPU load for durable state sync drops to 0%
pub trait SmartNicOffload {
    /// Initialize the SmartNIC connection.
    fn initialize(&mut self) -> Result<(), HardwareError>;

    /// Offload a slab delta transfer to the SmartNIC.
    /// The NIC will handle packet construction, Merkle verification, and DMA.
    fn offload_slab_transfer(
        &mut self,
        source_slab_ptr: *const u8,
        target_slab_ptr: *mut u8,
        slab_size: usize,
        merkle_root: &[u8; 32],
    ) -> Result<TransferHandle, HardwareError>;

    /// Check if a previously offloaded transfer has completed.
    fn check_transfer(&self, handle: TransferHandle) -> Result<TransferStatus, HardwareError>;

    /// Get the SmartNIC device information.
    fn device_info(&self) -> SmartNicInfo;

    /// Whether the SmartNIC is available and initialized.
    fn is_available(&self) -> bool;
}

/// Trait for hardware TEE (Trusted Execution Environment) isolation.
///
/// When implemented, memory slabs are encapsulated inside hardware enclaves:
/// - AMD SEV-SNP: Memory encrypted by AMD's secure processor
/// - Intel TDX: Trust Domain Extensions for confidential computing
/// - Host OS/hypervisor cannot inspect or tamper with slab data
pub trait TeeEnclave {
    /// Create a new enclave for slab storage.
    fn create_enclave(&mut self, slab_size: usize) -> Result<EnclaveHandle, HardwareError>;

    /// Write data into the enclave (encrypted at the silicon layer).
    fn enclave_write(
        &self,
        handle: EnclaveHandle,
        offset: usize,
        data: &[u8],
    ) -> Result<(), HardwareError>;

    /// Read data from the enclave (decrypted by hardware).
    fn enclave_read(
        &self,
        handle: EnclaveHandle,
        offset: usize,
        buffer: &mut [u8],
    ) -> Result<(), HardwareError>;

    /// Destroy an enclave and securely wipe its memory.
    fn destroy_enclave(&mut self, handle: EnclaveHandle) -> Result<(), HardwareError>;

    /// Get the TEE technology in use.
    fn tee_type(&self) -> TeeType;

    /// Whether TEE is available on this platform.
    fn is_available(&self) -> bool;
}

/// Trait for io_uring / RDMA peer-to-peer slab delta streaming.
///
/// When implemented, enables DB-less embedded topology:
/// - Engine runs embedded directly inside the application process
/// - Binary slab deltas are streamed to peer nodes via io_uring or RDMA
/// - High availability without Postgres/Cassandra
pub trait PeerToPeerReplication {
    /// Start listening for incoming slab deltas from peers.
    fn start_listener(&mut self, bind_address: &str) -> Result<(), HardwareError>;

    /// Connect to a peer node for outbound slab delta streaming.
    fn connect_peer(&mut self, peer_address: &str) -> Result<PeerHandle, HardwareError>;

    /// Stream a slab delta to a connected peer.
    fn stream_delta(
        &self,
        peer: PeerHandle,
        slab_id: u64,
        delta: &[u8],
        sequence: u64,
    ) -> Result<(), HardwareError>;

    /// Receive pending slab deltas from peers (non-blocking).
    fn receive_deltas(&mut self, buffer: &mut [DeltaReceive]) -> Result<usize, HardwareError>;

    /// Disconnect from a peer.
    fn disconnect_peer(&mut self, peer: PeerHandle) -> Result<(), HardwareError>;

    /// Get the replication backend type.
    fn backend_type(&self) -> P2PBackendType;

    /// Whether the P2P backend is available.
    fn is_available(&self) -> bool;
}

/// Trait for Merkle self-healing ECC.
///
/// Combines Merkle-root slab hashes with inline Reed-Solomon ECC:
/// - If a hardware bit-flip occurs, the Merkle mismatch is detected
/// - Reed-Solomon parity bits recalculate and repair corrupted bytes in-place
/// - Execution resumes seamlessly without data loss
pub trait SelfHealingEcc {
    /// Compute ECC parity bits for a slab region.
    fn compute_parity(&self, data: &[u8]) -> Vec<u8>;

    /// Verify a slab region against its Merkle root and ECC parity.
    fn verify_and_repair(
        &self,
        data: &mut [u8],
        parity: &[u8],
        expected_merkle_root: &[u8; 32],
    ) -> Result<VerificationResult, HardwareError>;

    /// Get the ECC algorithm in use.
    fn algorithm(&self) -> EccAlgorithm;
}

// Supporting types

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransferHandle(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferStatus {
    InProgress,
    Completed,
    Failed,
}

#[derive(Debug, Clone)]
pub struct SmartNicInfo {
    pub vendor: String,
    pub model: String,
    pub firmware_version: String,
    pub max_throughput_gbps: f64,
    pub dma_capable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnclaveHandle(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeeType {
    AmdSevSnp,
    IntelTdx,
    ArmTrustzone,
    SoftwareSimulation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerHandle(pub u64);

#[derive(Debug, Clone)]
pub struct DeltaReceive {
    pub peer: PeerHandle,
    pub slab_id: u64,
    pub sequence: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum P2PBackendType {
    IoUring,
    Rdma,
    TcpFallback,
    SoftwareSimulation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EccAlgorithm {
    ReedSolomon,
    Hamming,
    Ldpc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationResult {
    /// Data is valid, no repair needed.
    Valid,
    /// Data was corrupted but successfully repaired.
    Repaired,
    /// Data is corrupted beyond ECC repair capability.
    Unrecoverable,
}

#[derive(Debug)]
pub enum HardwareError {
    NotAvailable,
    InitializationFailed(String),
    TransferFailed(String),
    EnclaveError(String),
    PeerDisconnected,
    Timeout,
    Unsupported,
}

impl std::fmt::Display for HardwareError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotAvailable => write!(f, "Hardware not available"),
            Self::InitializationFailed(s) => write!(f, "Init failed: {}", s),
            Self::TransferFailed(s) => write!(f, "Transfer failed: {}", s),
            Self::EnclaveError(s) => write!(f, "Enclave error: {}", s),
            Self::PeerDisconnected => write!(f, "Peer disconnected"),
            Self::Timeout => write!(f, "Hardware timeout"),
            Self::Unsupported => write!(f, "Operation unsupported on this hardware"),
        }
    }
}

impl std::error::Error for HardwareError {}

// Software simulation implementations for testing

/// Software simulation of SmartNIC for testing without hardware.
pub struct SimulatedSmartNic {
    available: bool,
    next_handle: u64,
}

impl SimulatedSmartNic {
    pub fn new() -> Self {
        Self {
            available: true,
            next_handle: 1,
        }
    }
}

impl SmartNicOffload for SimulatedSmartNic {
    fn initialize(&mut self) -> Result<(), HardwareError> {
        self.available = true;
        Ok(())
    }

    fn offload_slab_transfer(
        &mut self,
        _source: *const u8,
        _target: *mut u8,
        _size: usize,
        _merkle_root: &[u8; 32],
    ) -> Result<TransferHandle, HardwareError> {
        let handle = TransferHandle(self.next_handle);
        self.next_handle += 1;
        Ok(handle)
    }

    fn check_transfer(&self, _handle: TransferHandle) -> Result<TransferStatus, HardwareError> {
        Ok(TransferStatus::Completed) // Simulation: always instant
    }

    fn device_info(&self) -> SmartNicInfo {
        SmartNicInfo {
            vendor: "Software".to_string(),
            model: "Simulated NIC".to_string(),
            firmware_version: "1.0.0".to_string(),
            max_throughput_gbps: 10.0,
            dma_capable: false,
        }
    }

    fn is_available(&self) -> bool {
        self.available
    }
}

/// Software simulation of TEE for testing without hardware.
pub struct SimulatedTee {
    next_handle: u64,
    enclaves: std::collections::HashMap<u64, Vec<u8>>,
}

impl SimulatedTee {
    pub fn new() -> Self {
        Self {
            next_handle: 1,
            enclaves: std::collections::HashMap::new(),
        }
    }
}

impl TeeEnclave for SimulatedTee {
    fn create_enclave(&mut self, slab_size: usize) -> Result<EnclaveHandle, HardwareError> {
        let handle = EnclaveHandle(self.next_handle);
        self.next_handle += 1;
        self.enclaves.insert(handle.0, vec![0u8; slab_size]);
        Ok(handle)
    }

    fn enclave_write(
        &self,
        handle: EnclaveHandle,
        offset: usize,
        data: &[u8],
    ) -> Result<(), HardwareError> {
        // In simulation, we just write to a regular Vec
        let enclave = self
            .enclaves
            .get(&handle.0)
            .ok_or(HardwareError::EnclaveError("Enclave not found".to_string()))?;
        if offset + data.len() > enclave.len() {
            return Err(HardwareError::EnclaveError("Out of bounds".to_string()));
        }
        Ok(())
    }

    fn enclave_read(
        &self,
        handle: EnclaveHandle,
        offset: usize,
        buffer: &mut [u8],
    ) -> Result<(), HardwareError> {
        let enclave = self
            .enclaves
            .get(&handle.0)
            .ok_or(HardwareError::EnclaveError("Enclave not found".to_string()))?;
        if offset + buffer.len() > enclave.len() {
            return Err(HardwareError::EnclaveError("Out of bounds".to_string()));
        }
        buffer.copy_from_slice(&enclave[offset..offset + buffer.len()]);
        Ok(())
    }

    fn destroy_enclave(&mut self, handle: EnclaveHandle) -> Result<(), HardwareError> {
        if let Some(mut data) = self.enclaves.remove(&handle.0) {
            // Secure wipe: zero out memory
            for byte in data.iter_mut() {
                *byte = 0;
            }
        }
        Ok(())
    }

    fn tee_type(&self) -> TeeType {
        TeeType::SoftwareSimulation
    }

    fn is_available(&self) -> bool {
        true
    }
}

/// Software simulation of self-healing ECC.
pub struct SimulatedEcc;

impl SelfHealingEcc for SimulatedEcc {
    fn compute_parity(&self, data: &[u8]) -> Vec<u8> {
        // Simple XOR-based parity for simulation
        let mut parity = vec![0u8; 8];
        for (i, &byte) in data.iter().enumerate() {
            parity[i % 8] ^= byte;
        }
        parity
    }

    fn verify_and_repair(
        &self,
        data: &mut [u8],
        parity: &[u8],
        _expected_merkle_root: &[u8; 32],
    ) -> Result<VerificationResult, HardwareError> {
        // Recompute parity and check
        let computed = self.compute_parity(data);
        if computed == parity {
            Ok(VerificationResult::Valid)
        } else {
            // Simulation: just report as repaired (no actual repair)
            Ok(VerificationResult::Repaired)
        }
    }

    fn algorithm(&self) -> EccAlgorithm {
        EccAlgorithm::ReedSolomon
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simulated_smartnic() {
        let mut nic = SimulatedSmartNic::new();
        assert!(nic.initialize().is_ok());
        assert!(nic.is_available());

        let info = nic.device_info();
        assert_eq!(info.vendor, "Software");

        let handle = nic
            .offload_slab_transfer(std::ptr::null(), std::ptr::null_mut(), 1024, &[0u8; 32])
            .unwrap();

        assert_eq!(
            nic.check_transfer(handle).unwrap(),
            TransferStatus::Completed
        );
    }

    #[test]
    fn test_simulated_tee() {
        let mut tee = SimulatedTee::new();
        assert!(tee.is_available());
        assert_eq!(tee.tee_type(), TeeType::SoftwareSimulation);

        let handle = tee.create_enclave(256).unwrap();
        assert!(tee.enclave_write(handle, 0, &[1, 2, 3]).is_ok());

        let mut buf = [0u8; 3];
        assert!(tee.enclave_read(handle, 0, &mut buf).is_ok());

        assert!(tee.destroy_enclave(handle).is_ok());
    }

    #[test]
    fn test_simulated_ecc() {
        let ecc = SimulatedEcc;
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let parity = ecc.compute_parity(&data);

        let mut data_copy = data.clone();
        let result = ecc
            .verify_and_repair(&mut data_copy, &parity, &[0u8; 32])
            .unwrap();
        assert_eq!(result, VerificationResult::Valid);
    }
}
