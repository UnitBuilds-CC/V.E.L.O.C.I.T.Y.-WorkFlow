use std::net::UdpSocket;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use velocity_workflow_core::{SlabHeader, VctpPacketHeader};
use velocity_workflow_engine::{
    WorkflowEngine, WorkerRegistry, TaskQueue, TaskItem, TaskKind,
};

/// VCTP Packet Types for daemon protocol
#[repr(u8)]
enum VctpPacketType {
    /// Worker registration: worker announces itself with capabilities
    WorkerRegister = 1,
    /// Worker heartbeat: periodic keep-alive
    WorkerHeartbeat = 2,
    /// Task dispatch request: worker polls for tasks
    TaskPoll = 3,
    /// Task result: worker returns completed task
    TaskResult = 4,
    /// Worker deregister: graceful shutdown
    WorkerDeregister = 5,
    /// Ack response from daemon
    Ack = 128,
}

/// Simple packet framing: [type(1)] [worker_id(8)] [payload_len(4)] [payload...]
struct DaemonPacket {
    packet_type: u8,
    worker_id: u64,
    payload: Vec<u8>,
}

impl DaemonPacket {
    fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 13 { return None; }
        let packet_type = data[0];
        let worker_id = u64::from_le_bytes(data[1..9].try_into().ok()?);
        let payload_len = u32::from_le_bytes(data[9..13].try_into().ok()?) as usize;
        if data.len() < 13 + payload_len { return None; }
        let payload = data[13..13 + payload_len].to_vec();
        Some(Self { packet_type, worker_id, payload })
    }

    fn encode_ack(worker_id: u64, status: u8, payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(14 + payload.len());
        buf.push(VctpPacketType::Ack as u8);
        buf.extend_from_slice(&worker_id.to_le_bytes());
        buf.push(status);
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(payload);
        buf
    }
}

fn main() -> std::io::Result<()> {
    println!("=========================================================");
    println!(" V.E.L.O.C.I.T.Y.-WorkFlow Hardware Daemon ");
    println!(" Listening on UDP 0.0.0.0:9090 (VCTP Memory Transport)  ");
    println!("=========================================================");

    let socket = UdpSocket::bind("127.0.0.1:9090")?;
    socket.set_nonblocking(false)?;
    let mut buf = [0u8; 65536];

    // Create the engine with worker registry
    let engine = Arc::new(WorkflowEngine::new());
    let running = Arc::new(AtomicBool::new(true));

    // Handle Ctrl+C for graceful shutdown
    let r = running.clone();
    ctrlc_handler(r);

    println!("[Daemon] Engine initialized with worker registry");
    println!("[Daemon] Accepting VCTP packets...");

    let mut packet_count = 0u64;

    while running.load(Ordering::Relaxed) {
        match socket.recv_from(&mut buf) {
            Ok((amt, src)) => {
                packet_count += 1;
                if let Some(packet) = DaemonPacket::parse(&buf[..amt]) {
                    match packet.packet_type {
                        // Worker registration
                        1 => {
                            let addr = String::from_utf8_lossy(&packet.payload).to_string();
                            let worker_id = engine.worker_registry().register_worker(
                                &addr, &[], &[], "1.0"
                            );
                            println!("[Daemon] Worker registered: id={} addr={} (from {})", worker_id, addr, src);
                            let ack = DaemonPacket::encode_ack(worker_id, 1, &worker_id.to_le_bytes());
                            let _ = socket.send_to(&ack, src);
                        }
                        // Worker heartbeat
                        2 => {
                            let found = engine.worker_registry().heartbeat(packet.worker_id);
                            let status = if found { 1u8 } else { 0u8 };
                            let ack = DaemonPacket::encode_ack(packet.worker_id, status, &[]);
                            let _ = socket.send_to(&ack, src);
                        }
                        // Task poll request
                        3 => {
                            if packet.payload.len() >= 8 {
                                let tq_hash = u64::from_le_bytes(packet.payload[0..8].try_into().unwrap_or([0;8]));
                                if let Some(task) = engine.task_queue().try_poll(tq_hash) {
                                    // Encode task response
                                    let mut resp = Vec::with_capacity(32);
                                    resp.extend_from_slice(&task.task_id.to_le_bytes());
                                    resp.push(task.kind as u8);
                                    resp.extend_from_slice(&task.workflow_key.to_le_bytes());
                                    resp.extend_from_slice(&task.step_index.to_le_bytes());
                                    resp.extend_from_slice(&task.activity_name_id.to_le_bytes());
                                    resp.extend_from_slice(&task.attempt.to_le_bytes());
                                    let ack = DaemonPacket::encode_ack(packet.worker_id, 1, &resp);
                                    let _ = socket.send_to(&ack, src);
                                    // Record task as being worked on
                                    engine.worker_registry().record_task_completed(packet.worker_id);
                                } else {
                                    let ack = DaemonPacket::encode_ack(packet.worker_id, 0, &[]);
                                    let _ = socket.send_to(&ack, src);
                                }
                            }
                        }
                        // Task result
                        4 => {
                            println!("[Daemon] Task result from worker {} ({} bytes)", packet.worker_id, packet.payload.len());
                            engine.worker_registry().record_task_completed(packet.worker_id);
                            let ack = DaemonPacket::encode_ack(packet.worker_id, 1, &[]);
                            let _ = socket.send_to(&ack, src);
                        }
                        // Worker deregister
                        5 => {
                            let removed = engine.worker_registry().unregister_worker(packet.worker_id);
                            println!("[Daemon] Worker {} deregistered: {}", packet.worker_id, removed);
                            let status = if removed { 1u8 } else { 0u8 };
                            let ack = DaemonPacket::encode_ack(packet.worker_id, status, &[]);
                            let _ = socket.send_to(&ack, src);
                        }
                        _ => {
                            println!("[Daemon] Unknown packet type {} from {}", packet.packet_type, src);
                        }
                    }
                } else {
                    println!("[Daemon] Invalid packet from {} ({} bytes)", src, amt);
                }

                // Periodic status report every 100 packets
                if packet_count % 100 == 0 {
                    println!("[Daemon] Status: {} packets processed, {} workers ({} active), {} tasks completed",
                        packet_count,
                        engine.worker_registry().worker_count(),
                        engine.worker_registry().active_worker_count(),
                        engine.worker_registry().total_tasks_completed());
                    
                    // Detect stale workers (30s timeout)
                    let stale = engine.worker_registry().detect_stale_workers(30_000);
                    if !stale.is_empty() {
                        println!("[Daemon] Detected {} stale workers", stale.len());
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(e) => {
                eprintln!("[Daemon] Socket error: {}", e);
                break;
            }
        }
    }

    println!("[Daemon] Shutting down after {} packets", packet_count);
    engine.worker_registry().list_worker_ids().iter().for_each(|&wid| {
        engine.worker_registry().set_worker_status(wid, velocity_workflow_engine::WorkerStatus::Offline);
    });

    Ok(())
}

fn ctrlc_handler(running: Arc<AtomicBool>) {
    // Simple signal handling - in production would use signal_hook crate
    std::thread::spawn(move || {
        // Wait for a signal or just run indefinitely
        loop {
            std::thread::sleep(std::time::Duration::from_secs(3600));
            if !running.load(Ordering::Relaxed) { break; }
        }
    });
}
