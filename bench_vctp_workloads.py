#!/usr/bin/env python3
"""VCTP vs HTTP benchmark — same workloads, same engine, different transport.

Sends VCTP/UDP binary frames to the Velocity VCTP server and measures
throughput/latency for the same workloads the HTTP bench server runs.

Wire format (little-endian):
  Header (28 bytes):
    magic:            u32  = 0x50544356
    sequence_number:  u64
    workflow_id:      u64
    slab_offset:      u32
    payload_length:   u32
  Payload:            JSON RPC envelope
  Checksum:           u32  = CRC32(header + payload)
"""

import socket
import struct
import json
import time
import sys
import statistics

VCTP_MAGIC = 0x50544356
VCTP_ACK_MAGIC = 0x4B435656
VCTP_HEADER_SIZE = 28

import binascii

def crc32(data: bytes) -> int:
    return binascii.crc32(data) & 0xFFFFFFFF

def build_vctp_packet(sequence: int, workflow_id: int, payload: bytes) -> bytes:
    header = struct.pack('<IQQII',
        VCTP_MAGIC, sequence, workflow_id, 0, len(payload))
    checksum = crc32(header + payload)
    return header + payload + struct.pack('<I', checksum)

def recv_response(sock, timeout=5.0):
    sock.settimeout(timeout)
    while True:
        data, addr = sock.recvfrom(65535)
        if len(data) < 4:
            continue
        magic = struct.unpack('<I', data[:4])[0]
        if magic == VCTP_ACK_MAGIC:
            continue
        if magic == VCTP_MAGIC:
            return data
        # Unknown magic, skip

def parse_vctp_response(data: bytes) -> dict:
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

def send_rpc(sock, host, port, seq, method, **kwargs):
    """Send a VCTP RPC and return (response_dict, latency_ms)."""
    req = {"method": method}
    req.update(kwargs)
    payload = json.dumps(req).encode('utf-8')
    pkt = build_vctp_packet(seq, 0, payload)

    t0 = time.perf_counter()
    sock.sendto(pkt, (host, port))
    data = recv_response(sock)
    t1 = time.perf_counter()
    resp = parse_vctp_response(data)
    return resp, (t1 - t0) * 1000.0


def bench_workload(sock, host, port, name, method, warmup_iters, bench_iters, **kwargs):
    """Run a single workload benchmark and return results."""
    print(f"\n{'='*60}")
    print(f"  Workload: {name}")
    print(f"  Method: {method}, iters: {bench_iters}")
    if kwargs:
        print(f"  Params: {kwargs}")
    print(f"{'='*60}")

    # Warmup
    seq = 1
    for i in range(warmup_iters):
        resp, _ = send_rpc(sock, host, port, seq, method, **kwargs)
        seq += 1

    # Benchmark
    latencies = []
    successes = 0
    failures = 0
    start_time = time.perf_counter()

    for i in range(bench_iters):
        try:
            resp, lat = send_rpc(sock, host, port, seq, method, **kwargs)
            if "error" not in resp:
                successes += 1
                latencies.append(lat)
            else:
                failures += 1
                if failures <= 2:
                    print(f"  FAIL: {resp}")
            seq += 1
        except socket.timeout:
            failures += 1
            if failures <= 2:
                print(f"  TIMEOUT on iter {i}")
        except Exception as e:
            failures += 1
            if failures <= 2:
                print(f"  ERROR: {e}")

    elapsed = time.perf_counter() - start_time

    # Results
    if latencies:
        latencies.sort()
        avg = statistics.mean(latencies)
        p50 = latencies[int(len(latencies) * 0.50)]
        p95 = latencies[int(len(latencies) * 0.95)]
        p99 = latencies[int(len(latencies) * 0.99)]
        mn = latencies[0]
        mx = latencies[-1]
        ops_s = successes / elapsed if elapsed > 0 else 0

        print(f"\n  Results ({name}):")
        print(f"    Success: {successes}, Fail: {failures}")
        print(f"    Throughput: {ops_s:.1f} ops/s")
        print(f"    Latency avg: {avg:.3f}ms, p50: {p50:.3f}ms, p95: {p95:.3f}ms, p99: {p99:.3f}ms")
        print(f"    Latency min: {mn:.3f}ms, max: {mx:.3f}ms")
        print(f"    Total time: {elapsed:.3f}s")

        return {
            "name": name,
            "ops_s": ops_s,
            "avg_ms": avg,
            "p50_ms": p50,
            "p95_ms": p95,
            "p99_ms": p99,
            "min_ms": mn,
            "max_ms": mx,
            "successes": successes,
            "failures": failures,
        }
    else:
        print(f"\n  Results ({name}): NO SUCCESSFUL OPS")
        return {"name": name, "ops_s": 0, "avg_ms": 0, "p50_ms": 0, "p95_ms": 0, "p99_ms": 0, "min_ms": 0, "max_ms": 0, "successes": 0, "failures": failures}


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 7234
    warmup = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    iters = int(sys.argv[4]) if len(sys.argv) > 4 else 100

    print(f"+{'='*60}+")
    print(f"|  VCTP/UDP Benchmark - Velocity Workflow Server        |")
    print(f"+{'='*60}+")
    print(f"|  Target:  {host}:{port} (UDP)")
    print(f"|  Warmup:  {warmup} iters")
    print(f"|  Benchmark: {iters} iters per workload")
    print(f"+{'='*60}+")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    results = []

    # 1. simple_workflow — 10 steps (matches HTTP bench)
    results.append(bench_workload(
        sock, host, port,
        "simple_workflow (10 steps)",
        100,  # START_WORKFLOW
        warmup, iters,
        namespace="default",
        workflow_id="vctp-simple",
        workflow_type="simple",
        total_steps=10,
    ))

    # 2. multi_step — 100 steps (the key workload)
    results.append(bench_workload(
        sock, host, port,
        "multi_step (100 steps)",
        100,
        warmup, iters,
        namespace="default",
        workflow_id="vctp-multi",
        workflow_type="multi_step",
        total_steps=100,
    ))

    # 3. multi_step — 50 steps
    results.append(bench_workload(
        sock, host, port,
        "multi_step (50 steps)",
        100,
        warmup, iters,
        namespace="default",
        workflow_id="vctp-multi50",
        workflow_type="multi_step_50",
        total_steps=50,
    ))

    # 4. signal_storm — 10 steps + 50 signals
    results.append(bench_workload(
        sock, host, port,
        "signal_storm (10 steps)",
        100,
        warmup, iters,
        namespace="default",
        workflow_id="vctp-signal",
        workflow_type="signal_storm",
        total_steps=10,
    ))

    # 5. cold_start — 1 step
    results.append(bench_workload(
        sock, host, port,
        "cold_start (1 step)",
        100,
        warmup, iters,
        namespace="default",
        workflow_id="vctp-cold",
        workflow_type="cold_start",
        total_steps=1,
    ))

    # 6. stateful — 2 steps
    results.append(bench_workload(
        sock, host, port,
        "stateful (2 steps)",
        100,
        warmup, iters,
        namespace="default",
        workflow_id="vctp-stateful",
        workflow_type="stateful",
        total_steps=2,
    ))

    sock.close()

    # Summary table
    print(f"\n{'='*80}")
    print(f"  VCTP BENCHMARK SUMMARY")
    print(f"{'='*80}")
    print(f"  {'Workload':<30} {'ops/s':>8} {'avg(ms)':>10} {'p50(ms)':>10} {'p99(ms)':>10}")
    print(f"  {'-'*30} {'-'*8} {'-'*10} {'-'*10} {'-'*10}")
    for r in results:
        print(f"  {r['name']:<30} {r['ops_s']:>8.1f} {r['avg_ms']:>10.3f} {r['p50_ms']:>10.3f} {r['p99_ms']:>10.3f}")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
