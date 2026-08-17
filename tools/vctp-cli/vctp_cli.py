#!/usr/bin/env python3
"""vctp-cli — Command-line tool for speaking VCTP over UDP.

Usage:
    python vctp_cli.py health --server 127.0.0.1:9090
    python vctp_cli.py start-workflow --server 127.0.0.1:9090 --type MyWorkflow --steps 5
    python vctp_cli.py signal --server 127.0.0.1:9090 --workflow-id wf-1 --signal-name my-signal
    python vctp_cli.py query --server 127.0.0.1:9090 --workflow-id wf-1
    python vctp_cli.py describe --server 127.0.0.1:9090 --workflow-id wf-1
    python vctp_cli.py cancel --server 127.0.0.1:9090 --workflow-id wf-1
    python vctp_cli.py terminate --server 127.0.0.1:9090 --workflow-id wf-1
    python vctp_cli.py list --server 127.0.0.1:9090
    python vctp_cli.py count --server 127.0.0.1:9090

Auth flags:
    --api-key KEY         Use API key authentication
    --auth-token TOKEN    Use JWT bearer token authentication
    --idempotency-key KEY Include idempotency key for duplicate detection
"""

import argparse
import json
import socket
import struct
import sys
import time
import zlib

# VCTP protocol constants
VCTP_MAGIC = 0x50544356  # "VCTP" in little-endian
VCTP_HEADER_SIZE = 28     # 4 + 8 + 8 + 4 + 4

# Method IDs (must match VctpMethods in vctp_rpc.rs)
METHODS = {
    "START_WORKFLOW": 100,
    "SIGNAL_WORKFLOW": 101,
    "QUERY_WORKFLOW": 102,
    "CANCEL_WORKFLOW": 103,
    "TERMINATE_WORKFLOW": 104,
    "DESCRIBE_WORKFLOW": 105,
    "LIST_WORKFLOWS": 106,
    "RESET_WORKFLOW": 107,
    "UPDATE_WORKFLOW": 108,
    "COMPLETE_WORKFLOW": 109,
    "HEALTH_CHECK": 500,
    "COUNT_WORKFLOWS": 502,
    "BATCH_SIGNAL": 503,
    "BATCH_TERMINATE": 504,
    "SIGNAL_WITH_START": 606,
    "REGISTER_NAMESPACE": 300,
    "DESCRIBE_NAMESPACE": 301,
}


def compute_crc32(data: bytes) -> int:
    """Compute CRC32 checksum (same as Rust's crc32 fast)."""
    return zlib.crc32(data) & 0xFFFFFFFF


def build_vctp_packet(sequence: int, method_id: int, payload: bytes) -> bytes:
    """Build a complete VCTP packet with header + payload + CRC32.

    Wire format:
        [magic:u32][sequence:u64][workflow_id:u64][slab_offset:u32][payload_length:u32]
        [payload:bytes]
        [crc32:u32]
    """
    header = struct.pack(
        "<IQQII",
        VCTP_MAGIC,
        sequence,
        method_id,        # workflow_id field carries the method ID
        0,                # slab_offset (0 = no fragmentation)
        len(payload),     # payload_length
    )
    packet_without_crc = header + payload
    crc = compute_crc32(packet_without_crc)
    return packet_without_crc + struct.pack("<I", crc)


def parse_vctp_response(data: bytes) -> dict:
    """Parse a VCTP response packet and extract the JSON payload."""
    if len(data) < VCTP_HEADER_SIZE + 4:
        return {"error": f"Response too small ({len(data)} bytes)"}

    magic = struct.unpack_from("<I", data, 0)[0]
    if magic != VCTP_MAGIC:
        return {"error": f"Invalid magic: 0x{magic:08X}"}

    sequence = struct.unpack_from("<Q", data, 4)[0]
    payload_len = struct.unpack_from("<I", data, 24)[0]

    # Verify CRC32
    packet_without_crc = data[:VCTP_HEADER_SIZE + payload_len]
    expected_crc = struct.unpack_from("<I", data, VCTP_HEADER_SIZE + payload_len)[0]
    actual_crc = compute_crc32(packet_without_crc)
    if actual_crc != expected_crc:
        return {"error": f"CRC32 mismatch: expected 0x{expected_crc:08X}, got 0x{actual_crc:08X}"}

    # Extract JSON payload
    payload = data[VCTP_HEADER_SIZE:VCTP_HEADER_SIZE + payload_len]
    try:
        return json.loads(payload)
    except json.JSONDecodeError as e:
        return {"error": f"Invalid JSON payload: {e}", "raw": payload.hex()}


def send_vctp_request(
    server: str,
    method: str,
    request_body: dict,
    timeout: float = 5.0,
) -> dict:
    """Send a VCTP RPC request and wait for a response."""
    host, port_str = server.rsplit(":", 1)
    port = int(port_str)

    method_id = METHODS.get(method)
    if method_id is None:
        return {"error": f"Unknown method: {method}"}

    payload = json.dumps(request_body).encode("utf-8")
    sequence = int(time.time() * 1000) & 0xFFFFFFFFFFFFFFFF
    packet = build_vctp_packet(sequence, method_id, payload)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)

    try:
        sock.sendto(packet, (host, port))
        data, _ = sock.recvfrom(65535)
        return parse_vctp_response(data)
    except socket.timeout:
        return {"error": f"Request timed out after {timeout}s"}
    except ConnectionRefusedError:
        return {"error": f"Connection refused to {server}"}
    except Exception as e:
        return {"error": str(e)}
    finally:
        sock.close()


def format_response(response: dict) -> str:
    """Pretty-print a VCTP response."""
    status = response.get("status", -1)
    if status == 0:
        # Success — show relevant fields
        parts = ["OK"]
        if response.get("workflow_id"):
            parts.append(f"workflow_id={response['workflow_id']}")
        if response.get("run_id"):
            parts.append(f"run_id={response['run_id']}")
        if response.get("run_status"):
            parts.append(f"status={response['run_status']}")
        if response.get("count") is not None:
            parts.append(f"count={response['count']}")
        if response.get("payload"):
            parts.append(f"payload={response['payload']}")
        return " | ".join(parts)
    else:
        error = response.get("error", "unknown error")
        return f"ERROR {status}: {error}"


def main():
    parser = argparse.ArgumentParser(
        prog="vctp-cli",
        description="VCTP command-line client — speak VCTP over UDP",
    )
    parser.add_argument(
        "command",
        choices=[
            "health", "start-workflow", "signal", "query", "describe",
            "list", "cancel", "terminate", "count", "reset",
            "register-namespace", "describe-namespace",
            "batch-signal", "signal-with-start",
        ],
        help="VCTP method to invoke",
    )
    parser.add_argument(
        "--server", "-s",
        default="127.0.0.1:9090",
        help="VCTP server address (default: 127.0.0.1:9090)",
    )
    parser.add_argument(
        "--workflow-id", "-w",
        default="",
        help="Workflow ID (auto-generated if omitted for start-workflow)",
    )
    parser.add_argument(
        "--type", "-t",
        default="DefaultWorkflow",
        dest="workflow_type",
        help="Workflow type name (for start-workflow)",
    )
    parser.add_argument(
        "--steps",
        type=int,
        default=10,
        help="Number of steps (for start-workflow)",
    )
    parser.add_argument(
        "--signal-name",
        default="signal",
        help="Signal name (for signal)",
    )
    parser.add_argument(
        "--namespace", "-n",
        default="default",
        help="Namespace (default: 'default')",
    )
    parser.add_argument(
        "--api-key",
        help="API key for authentication",
    )
    parser.add_argument(
        "--auth-token",
        help="JWT bearer token for authentication",
    )
    parser.add_argument(
        "--idempotency-key",
        help="Idempotency key for duplicate detection",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="Request timeout in seconds (default: 5.0)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Output raw JSON response",
    )
    parser.add_argument(
        "--signal-count",
        type=int,
        default=1,
        help="Number of signals (for batch-signal)",
    )

    args = parser.parse_args()

    # Map command to VCTP method
    command_to_method = {
        "health": "HEALTH_CHECK",
        "start-workflow": "START_WORKFLOW",
        "signal": "SIGNAL_WORKFLOW",
        "query": "QUERY_WORKFLOW",
        "describe": "DESCRIBE_WORKFLOW",
        "list": "LIST_WORKFLOWS",
        "cancel": "CANCEL_WORKFLOW",
        "terminate": "TERMINATE_WORKFLOW",
        "count": "COUNT_WORKFLOWS",
        "reset": "RESET_WORKFLOW",
        "register-namespace": "REGISTER_NAMESPACE",
        "describe-namespace": "DESCRIBE_NAMESPACE",
        "batch-signal": "BATCH_SIGNAL",
        "signal-with-start": "SIGNAL_WITH_START",
    }

    method = command_to_method[args.command]

    # Build request body
    request = {
        "method": METHODS[method],
        "namespace": args.namespace,
        "workflow_id": args.workflow_id,
    }

    # Add method-specific fields
    if args.command == "start-workflow" or args.command == "signal-with-start":
        request["workflow_type"] = args.workflow_type
        request["total_steps"] = args.steps

    if args.command in ("signal", "batch-signal", "signal-with-start"):
        request["signal_name"] = args.signal_name
        if args.command == "batch-signal":
            request["signal_count"] = args.signal_count

    # Auth fields
    if args.api_key:
        request["api_key"] = args.api_key
    if args.auth_token:
        request["auth_token"] = args.auth_token
    if args.idempotency_key:
        request["idempotency_key"] = args.idempotency_key

    # Send request
    response = send_vctp_request(args.server, method, request, args.timeout)

    # Output
    if args.json_output:
        print(json.dumps(response, indent=2))
    else:
        print(format_response(response))

    # Exit code based on status
    status = response.get("status", -1)
    sys.exit(0 if status == 0 else 1)


if __name__ == "__main__":
    main()
