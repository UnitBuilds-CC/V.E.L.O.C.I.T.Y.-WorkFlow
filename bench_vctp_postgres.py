#!/usr/bin/env python3
"""Benchmark the Velocity Workflow Server (VCTP/UDP) with full PostgreSQL persistence.

Constructs proper VCTP binary frames (28-byte header + JSON payload + CRC32)
and sends them over UDP, measuring throughput and latency.

Wire format (little-endian):
  Header (28 bytes):
    magic:            u32  = 0x50544356
    sequence_number:  u64
    workflow_id:      u64
    slab_offset:      u32
    payload_length:   u32
  Payload:            JSON RPC envelope (payload_length bytes)
  Checksum:           u32  = CRC32(header + payload)
"""

import socket
import struct
import json
import time
import sys
import os

VCTP_MAGIC = 0x50544356
VCTP_ACK_MAGIC = 0x4B435656
VCTP_HEADER_SIZE = 28

# CRC32 (same algorithm as Rust: standard CRC32 with polynomial 0xEDB88320)
import binascii

def crc32(data: bytes) -> int:
    """Compute CRC32 matching the Rust implementation (standard CRC32)."""
    return binascii.crc32(data) & 0xFFFFFFFF


def build_vctp_packet(sequence: int, workflow_id: int, payload: bytes) -> bytes:
    """Build a complete VCTP packet: header + payload + CRC32."""
    header = struct.pack('<IQQII',
        VCTP_MAGIC,
        sequence,
        workflow_id,
        0,                  # slab_offset
        len(payload),       # payload_length
    )
    checksum = crc32(header + payload)
    return header + payload + struct.pack('<I', checksum)


def recv_response(sock, timeout=5.0):
    """Receive a VCTP response, skipping any ACK packets."""
    sock.settimeout(timeout)
    while True:
        data, addr = sock.recvfrom(65535)
        if len(data) < 4:
            continue
        magic = struct.unpack('<I', data[:4])[0]
        if magic == VCTP_ACK_MAGIC:
            continue  # Skip ACK packets
        if magic == VCTP_MAGIC:
            return data
        # Unknown packet, skip


def parse_vctp_response(data: bytes) -> dict:
    """Parse a VCTP response packet and return the JSON payload."""
    if len(data) < VCTP_HEADER_SIZE + 4:
        return {"error": "response too small"}
    magic, seq, wf_id, slab_off, payload_len = struct.unpack('<IQQII', data[:VCTP_HEADER_SIZE])
    if magic != VCTP_MAGIC:
        return {"error": f"bad magic: 0x{magic:08X}"}
    payload = data[VCTP_HEADER_SIZE:VCTP_HEADER_SIZE + payload_len]
    try:
        return json.loads(payload)
    except json.JSONDecodeError as e:
        return {"error": str(e)}


def make_start_workflow_request(sequence: int, wf_id: str) -> bytes:
    """Create a VCTP start_workflow request packet."""
    req = {
        "method": 100,           # VctpMethods::START_WORKFLOW
        "namespace": "default",
        "workflow_id": wf_id,
        "workflow_type": "bench",
        "total_steps": 10,
    }
    payload = json.dumps(req).encode('utf-8')
    return build_vctp_packet(sequence, 0, payload)


def benchmark(host: str, port: int, iterations: int):
    """Run the benchmark."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(5.0)
    
    print(f"=== Velocity Workflow Server (VCTP/UDP) Benchmark ===")
    print(f"Target: {host}:{port} (UDP)")
    print(f"Iterations: {iterations}")
    print(f"PostgreSQL persistence: enabled")
    print()

    # Warmup: send 10 packets
    print("Warming up (10 workflows)...")
    for i in range(10):
        pkt = make_start_workflow_request(i, f"warmup-{i}")
        sock.sendto(pkt, (host, port))
        try:
            data = recv_response(sock)
            resp = parse_vctp_response(data)
            if resp.get("status") == 0:
                pass  # OK
            else:
                print(f"  Warmup {i} failed: {resp}")
        except socket.timeout:
            print(f"  Warmup {i} timed out")
    print("Warmup done.\n")

    # Main benchmark
    seq_start = 100
    successes = 0
    failures = 0
    
    print(f"Running {iterations} workflows...")
    start_time = time.time()
    
    for i in range(iterations):
        seq = seq_start + i
        wf_id = f"vctp-wf-{i}"
        pkt = make_start_workflow_request(seq, wf_id)
        
        try:
            sock.sendto(pkt, (host, port))
            data = recv_response(sock)
            resp = parse_vctp_response(data)
            if resp.get("status") == 0:
                successes += 1
            else:
                failures += 1
                if failures <= 3:
                    print(f"  Failed: {resp}")
        except socket.timeout:
            failures += 1
            if failures <= 3:
                print(f"  Timeout on iteration {i}")
    
    elapsed = time.time() - start_time
    
    print()
    print(f"=== Results ===")
    print(f"Completed: {successes} success, {failures} failures")
    print(f"Time: {elapsed:.3f}s")
    if successes > 0:
        throughput = successes / elapsed
        latency_ms = elapsed / successes * 1000
        print(f"Throughput: {throughput:.0f} workflows/sec")
        print(f"Latency: {latency_ms:.3f} ms/workflow")
    print()
    
    sock.close()
    return successes, elapsed


if __name__ == "__main__":
    host = os.environ.get("VCTP_HOST", "workflow-server")
    port = int(os.environ.get("VCTP_PORT", "7234"))
    iterations = int(os.environ.get("ITERATIONS", "5000"))
    
    # Allow CLI overrides
    if len(sys.argv) > 1:
        host = sys.argv[1]
    if len(sys.argv) > 2:
        port = int(sys.argv[2])
    if len(sys.argv) > 3:
        iterations = int(sys.argv[3])
    
    benchmark(host, port, iterations)
