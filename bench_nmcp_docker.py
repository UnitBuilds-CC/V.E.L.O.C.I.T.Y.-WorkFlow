import websocket, json, time, struct

def make_frame(ftype, payload):
    p = json.dumps(payload).encode()
    return struct.pack('<IIII', 0x50434D4E, ftype, len(p), 1) + p

def parse_resp(data):
    magic, ft, plen, seq = struct.unpack('<IIII', data[:16])
    return json.loads(data[16:16+plen])

N = 500

# Benchmark Classic Server (NMCP frame type 50 = START_WORKFLOW)
print("=" * 60)
print("Velocity NMCP Benchmark — Docker")
print("=" * 60)

ws = websocket.create_connection('ws://velocity-classic-server:8083')
start = time.time()
for i in range(N):
    f = make_frame(50, {'workflow_type': 'bench', 'workflow_id': f'wf-{i}'})
    ws.send_binary(f)
    r = ws.recv()
    b = parse_resp(r)
    assert b.get('success'), f'Failed: {b}'
elapsed = time.time() - start
print(f'Classic  NMCP: {N} workflows in {elapsed:.3f}s = {N/elapsed:.0f} wf/s ({elapsed/N*1000:.2f}ms/wf)')
ws.close()

# Benchmark Embedded Server (NMCP frame type 70 = EXECUTE_WORKFLOW)
ws = websocket.create_connection('ws://velocity-embedded-server:8084')
start = time.time()
for i in range(N):
    f = make_frame(70, {'workflow_type': 'bench', 'workflow_id': f'emb-{i}'})
    ws.send_binary(f)
    r = ws.recv()
    b = parse_resp(r)
    assert b.get('success'), f'Failed: {b}'
elapsed = time.time() - start
print(f'Embedded NMCP: {N} workflows in {elapsed:.3f}s = {N/elapsed:.0f} wf/s ({elapsed/N*1000:.2f}ms/wf)')
ws.close()

print("=" * 60)
print("Done!")
