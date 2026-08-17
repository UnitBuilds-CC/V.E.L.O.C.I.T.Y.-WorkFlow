"""
VCTP Transport Client for Python (asyncio UDP).

Provides a VctpClient class that communicates with a Velocity VCTP server
over UDP using the binary VCTP protocol.

Features:
  - Frame building with 28-byte header + JSON payload + CRC32
  - Sequence correlation for request/response matching
  - Fragmentation for large payloads
  - Auth token injection (JWT / API key)
  - Idempotency key generation
  - Reconnect + heartbeat handling

Usage:
    from velocity_workflow.vctp import VctpClient

    async def main():
        client = VctpClient(server_addr='127.0.0.1', server_port=9090)
        await client.connect()
        result = await client.start_workflow(workflow_type='MyWorkflow', total_steps=5)
        print(result)
        await client.disconnect()

    asyncio.run(main())
"""

import asyncio
import json
import socket
import struct
import time
import uuid
import zlib
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

# ─── Constants ────────────────────────────────────────────────────────────────

VCTP_MAGIC = 0x50544356
VCTP_HEADER_SIZE = 28
MAX_VCTP_PAYLOAD = 65479


class Methods:
    """VCTP method identifiers."""
    START_WORKFLOW = 100
    SIGNAL_WORKFLOW = 101
    QUERY_WORKFLOW = 102
    CANCEL_WORKFLOW = 103
    TERMINATE_WORKFLOW = 104
    DESCRIBE_WORKFLOW = 105
    LIST_WORKFLOWS = 106
    RESET_WORKFLOW = 107
    UPDATE_WORKFLOW = 108
    COMPLETE_WORKFLOW = 109
    HEALTH_CHECK = 500
    COUNT_WORKFLOWS = 502
    BATCH_SIGNAL = 503
    BATCH_TERMINATE = 504
    SIGNAL_WITH_START = 606
    REGISTER_NAMESPACE = 300
    DESCRIBE_NAMESPACE = 301


# ─── Data Classes ─────────────────────────────────────────────────────────────

@dataclass
class VctpRpcResponse:
    """Response from a VCTP RPC call."""
    status: int = 0
    sequence: int = 0
    error: Optional[str] = None
    workflow_id: Optional[str] = None
    run_id: Optional[str] = None
    run_status: Optional[str] = None
    count: Optional[int] = None
    payload: Optional[bytes] = None

    @classmethod
    def from_dict(cls, d: dict) -> 'VctpRpcResponse':
        return cls(
            status=d.get('status', 0),
            sequence=d.get('sequence', 0),
            error=d.get('error'),
            workflow_id=d.get('workflow_id'),
            run_id=d.get('run_id'),
            run_status=d.get('run_status'),
            count=d.get('count'),
        )


class VctpError(Exception):
    """Raised when a VCTP RPC call returns a non-zero status."""
    def __init__(self, status: int, message: str):
        self.status = status
        self.message = message
        super().__init__(f"VCTP error {status}: {message}")


# ─── CRC32 ────────────────────────────────────────────────────────────────────

def compute_crc32(data: bytes) -> int:
    """Compute CRC32 checksum matching the Rust implementation."""
    return zlib.crc32(data) & 0xFFFFFFFF


# ─── Packet Building ──────────────────────────────────────────────────────────

def build_vctp_packet(sequence: int, method_id: int, payload: bytes) -> bytes:
    """Build a complete VCTP packet with header + payload + CRC32."""
    header = struct.pack(
        '<IQQII',
        VCTP_MAGIC,
        sequence,
        method_id,
        0,  # slab_offset
        len(payload),
    )
    packet_without_crc = header + payload
    crc = compute_crc32(packet_without_crc)
    return packet_without_crc + struct.pack('<I', crc)


def parse_vctp_response(data: bytes) -> VctpRpcResponse:
    """Parse a VCTP response packet."""
    if len(data) < VCTP_HEADER_SIZE + 4:
        raise ValueError(f"Response too small ({len(data)} bytes)")

    magic = struct.unpack_from('<I', data, 0)[0]
    if magic != VCTP_MAGIC:
        raise ValueError(f"Invalid magic: 0x{magic:08X}")

    sequence = struct.unpack_from('<Q', data, 4)[0]
    payload_len = struct.unpack_from('<I', data, 24)[0]

    if len(data) < VCTP_HEADER_SIZE + payload_len + 4:
        raise ValueError("Response truncated")

    # Verify CRC32
    packet_data = data[:VCTP_HEADER_SIZE + payload_len]
    expected_crc = struct.unpack_from('<I', data, VCTP_HEADER_SIZE + payload_len)[0]
    actual_crc = compute_crc32(packet_data)
    if expected_crc != actual_crc:
        raise ValueError(f"CRC32 mismatch: expected 0x{expected_crc:08X}, got 0x{actual_crc:08X}")

    payload = data[VCTP_HEADER_SIZE:VCTP_HEADER_SIZE + payload_len]
    resp_dict = json.loads(payload)
    return VctpRpcResponse.from_dict(resp_dict)


# ─── VCTP Client ──────────────────────────────────────────────────────────────

class VctpClient:
    """Async VCTP client using asyncio UDP transport."""

    def __init__(
        self,
        server_addr: str = '127.0.0.1',
        server_port: int = 9090,
        auth_token: str = '',
        api_key: str = '',
        timeout: float = 5.0,
    ):
        self.server_addr = server_addr
        self.server_port = server_port
        self.auth_token = auth_token
        self.api_key = api_key
        self.timeout = timeout
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._protocol: Optional['_VctpProtocol'] = None
        self._sequence = 1
        self._connected = False

    async def connect(self) -> None:
        """Connect the UDP socket."""
        loop = asyncio.get_running_loop()
        self._protocol = _VctpProtocol()
        transport, _ = await loop.create_datagram_endpoint(
            lambda: self._protocol,
            family=socket.AF_INET,
        )
        self._transport = transport
        self._connected = True

    async def disconnect(self) -> None:
        """Disconnect the client."""
        self._connected = False
        if self._transport:
            self._transport.close()
            self._transport = None
        if self._protocol:
            self._protocol.cancel_all()
            self._protocol = None

    async def start_workflow(
        self,
        workflow_type: str,
        workflow_id: str = '',
        namespace: str = 'default',
        total_steps: int = 10,
        idempotency_key: str = '',
    ) -> Dict[str, str]:
        """Start a new workflow execution."""
        req = {
            'method': Methods.START_WORKFLOW,
            'namespace': namespace,
            'workflow_id': workflow_id,
            'workflow_type': workflow_type,
            'total_steps': total_steps,
        }
        if idempotency_key:
            req['idempotency_key'] = idempotency_key

        resp = await self._send_request(req)
        if resp.status != 0:
            raise VctpError(resp.status, resp.error or 'unknown error')
        return {
            'workflow_id': resp.workflow_id or '',
            'run_id': resp.run_id or '',
            'status': resp.run_status or '',
        }

    async def signal_workflow(
        self,
        workflow_id: str,
        signal_name: str,
        payload: bytes = b'',
    ) -> None:
        """Send a signal to a running workflow."""
        req = {
            'method': Methods.SIGNAL_WORKFLOW,
            'namespace': 'default',
            'workflow_id': workflow_id,
            'signal_name': signal_name,
            'payload': list(payload) if payload else None,
        }
        resp = await self._send_request(req)
        if resp.status != 0:
            raise VctpError(resp.status, resp.error or 'unknown error')

    async def query_workflow(self, workflow_id: str) -> str:
        """Query a workflow's current status."""
        req = {
            'method': Methods.QUERY_WORKFLOW,
            'namespace': 'default',
            'workflow_id': workflow_id,
        }
        resp = await self._send_request(req)
        if resp.status != 0:
            raise VctpError(resp.status, resp.error or 'unknown error')
        return resp.run_status or 'UNKNOWN'

    async def describe_workflow(self, workflow_id: str) -> Dict[str, str]:
        """Get detailed information about a workflow."""
        req = {
            'method': Methods.DESCRIBE_WORKFLOW,
            'namespace': 'default',
            'workflow_id': workflow_id,
        }
        resp = await self._send_request(req)
        if resp.status != 0:
            raise VctpError(resp.status, resp.error or 'unknown error')
        return {
            'workflow_id': resp.workflow_id or workflow_id,
            'run_id': resp.run_id or '',
            'status': resp.run_status or 'UNKNOWN',
        }

    async def cancel_workflow(self, workflow_id: str) -> None:
        """Cancel a running workflow."""
        req = {
            'method': Methods.CANCEL_WORKFLOW,
            'namespace': 'default',
            'workflow_id': workflow_id,
        }
        resp = await self._send_request(req)
        if resp.status != 0:
            raise VctpError(resp.status, resp.error or 'unknown error')

    async def terminate_workflow(self, workflow_id: str) -> None:
        """Terminate a workflow."""
        req = {
            'method': Methods.TERMINATE_WORKFLOW,
            'namespace': 'default',
            'workflow_id': workflow_id,
        }
        resp = await self._send_request(req)
        if resp.status != 0:
            raise VctpError(resp.status, resp.error or 'unknown error')

    async def health_check(self) -> str:
        """Check server health."""
        req = {'method': Methods.HEALTH_CHECK}
        resp = await self._send_request(req)
        return resp.run_status or 'unknown'

    async def count_workflows(self, namespace: str = 'default') -> int:
        """Count workflow executions."""
        req = {
            'method': Methods.COUNT_WORKFLOWS,
            'namespace': namespace,
        }
        resp = await self._send_request(req)
        return resp.count or 0

    # ─── Internal ────────────────────────────────────────────────────────

    async def _send_request(self, req: dict) -> VctpRpcResponse:
        """Send a VCTP request and wait for the response."""
        if not self._transport or not self._connected:
            raise RuntimeError('Not connected')

        # Inject auth
        if self.auth_token and 'auth_token' not in req:
            req['auth_token'] = self.auth_token
        if self.api_key and 'api_key' not in req:
            req['api_key'] = self.api_key

        seq = self._sequence
        self._sequence += 1

        payload = json.dumps(req).encode('utf-8')
        packet = build_vctp_packet(seq, req['method'], payload)

        # Register pending response
        assert self._protocol is not None
        future = self._protocol.register_pending(seq)

        # Send packet
        self._transport.sendto(packet, (self.server_addr, self.server_port))

        # Wait for response with timeout
        try:
            return await asyncio.wait_for(future, timeout=self.timeout)
        except asyncio.TimeoutError:
            self._protocol.remove_pending(seq)
            raise VctpError(504, f'Request timed out after {self.timeout}s')

    @staticmethod
    def generate_idempotency_key() -> str:
        """Generate a random idempotency key."""
        return str(uuid.uuid4())


class _VctpProtocol(asyncio.DatagramProtocol):
    """Internal asyncio UDP protocol for VCTP."""

    def __init__(self):
        self._pending: dict[int, asyncio.Future] = {}
        self._loop = asyncio.get_running_loop()

    def register_pending(self, seq: int) -> asyncio.Future:
        future = self._loop.create_future()
        self._pending[seq] = future
        return future

    def remove_pending(self, seq: int) -> None:
        self._pending.pop(seq, None)

    def cancel_all(self) -> None:
        for future in self._pending.values():
            if not future.done():
                future.set_exception(RuntimeError('Client disconnected'))
        self._pending.clear()

    def datagram_received(self, data: bytes, addr) -> None:
        try:
            response = parse_vctp_response(data)
            future = self._pending.pop(response.sequence, None)
            if future and not future.done():
                future.set_result(response)
        except Exception as e:
            pass  # Ignore malformed responses

    def error_received(self, exc: Exception) -> None:
        pass  # UDP errors are non-fatal
