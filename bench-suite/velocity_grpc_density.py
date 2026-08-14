#!/usr/bin/env python3
"""Quick Velocity gRPC density test: run N workflows, measure WAL growth."""
import subprocess, sys, time

try:
    import grpc
    from grpc_tools import protoc
    HAS_GRPC = True
except ImportError:
    HAS_GRPC = False
    print("grpcio not installed. Installing...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "grpcio", "grpcio-tools", "-q"])
    import grpc

# Generate gRPC stubs from proto
import os
proto_dir = os.path.join(os.path.dirname(__file__), "..", "velocity-bench", "proto")
proto_file = os.path.join(proto_dir, "benchmark.proto")
out_dir = os.path.join(os.path.dirname(__file__), "_grpc_stubs")
os.makedirs(out_dir, exist_ok=True)

from grpc_tools import protoc as grpc_protoc
grpc_protoc.main([
    "grpc_tools.protoc",
    f"-I{proto_dir}",
    f"--python_out={out_dir}",
    f"--grpc_python_out={out_dir}",
    proto_file,
])
sys.path.insert(0, out_dir)

# Import generated stubs
import importlib
benchmark_pb2 = importlib.import_module("benchmark_pb2")
benchmark_pb2_grpc = importlib.import_module("benchmark_pb2_grpc")

N = 20
FLAVORS = [
    ("Classic", "localhost:7234"),
    ("Runtime", "localhost:7235"),
    ("Embedded", "localhost:7236"),
]

for label, addr in FLAVORS:
    print(f"\n=== {label} ({addr}): {N} workflows ===")
    channel = grpc.insecure_channel(addr)
    stub = benchmark_pb2_grpc.BenchmarkServiceStub(channel)
    
    # Reset
    try:
        stub.Reset(benchmark_pb2.ResetRequest(namespace="default"))
    except:
        pass
    
    ok = 0
    t0 = time.time()
    for i in range(N):
        wid = f"density_{label.lower()}_{i}_{int(time.time())}"
        try:
            resp = stub.StartWorkflow(benchmark_pb2.StartWorkflowRequest(
                workflow_type="simple_workflow",
                workflow_id=wid,
                namespace="default",
                task_queue="bench",
                step_count=3,
            ))
            run_id = resp.run_id
            
            # Wait for completion
            wresp = stub.WaitForCompletion(benchmark_pb2.WaitForCompletionRequest(
                workflow_id=wid,
                run_id=run_id,
                namespace="default",
                timeout_ms=5000,
            ))
            if wresp.success:
                ok += 1
        except Exception as e:
            print(f"  op {i}: {e}")
        
        if (i+1) % 5 == 0:
            print(f"  {i+1}/{N} ({ok} ok)")
    
    elapsed = time.time() - t0
    print(f"  {label}: {ok}/{N} ok in {elapsed:.1f}s ({ok/max(elapsed,0.1):.1f} ops/s)")
    channel.close()

print("\nDone! Now check WAL sizes.")
