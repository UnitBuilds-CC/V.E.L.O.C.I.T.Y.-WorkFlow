#!/usr/bin/env python3
"""Benchmark Classic and Embedded servers via NMCP WebSocket."""
import websocket
import json
import time
import struct
import sys

def make_nmcp_frame(frame_type, payload_dict):
    payload = json.dumps(payload_dict).encode()
    header = struct.pack('<IIII', 0x50434D4E, frame_type, len(payload), 1)
    return header + payload

def parse_nmcp_response(data):
    magic, ftype, plen, seq = struct.unpack('<IIII', data[:16])
    body = json.loads(data[16:16+plen])
    return body

def bench(host, port, frame_type, name, n=1000):
    ws = websocket.create_connection(f'ws://{host}:{port}')
    
    # Warmup
    for i in range(10):
        ws.send_binary(make_nmcp_frame(frame_type, {'workflow_type': 'bench', 'workflow_id': f'w-{i}'}))
        parse_nmcp_response(ws.recv())
    
    start = time.time()
    ok = 0
    for i in range(n):
        ws.send_binary(make_nmcp_frame(frame_type, {'workflow_type': 'bench', 'workflow_id': f'{name}-{i}'}))
        r = parse_nmcp_response(ws.recv())
        if r.get('success'):
            ok += 1
    elapsed = time.time() - start
    
    print(f'{name}: {ok}/{n} in {elapsed:.3f}s = {ok/elapsed:.1f} wf/s, {elapsed/ok*1000:.1f} ms/wf')
    ws.close()
    return ok, elapsed

if __name__ == '__main__':
    bench('classic-server', 8083, 50, 'classic', int(sys.argv[1]) if len(sys.argv) > 1 else 1000)
    bench('embedded-server', 8084, 70, 'embedded', int(sys.argv[1]) if len(sys.argv) > 1 else 1000)
