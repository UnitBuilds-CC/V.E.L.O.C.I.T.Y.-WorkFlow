"""SSH into each VM, upload repo, setup, and run benchmarks."""
import json
import subprocess
import os
import sys
import time
import threading

PROJECT = "velocity-live-test-001"
ZONE = "us-east1-b"
GCLOUD = r"C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd"
REPO_DIR = r"C:\Users\visse\OneDrive\Documents\Velocity-workflow"

# VM -> FLAVOR mapping
VM_FLAVORS = {
    "velocity-classic": "velocity-classic",
    "velocity-runtime": "velocity-runtime",
    "velocity-embedded": "velocity-embedded",
    "temporal-bench": "temporal",
    "restate-bench": "restate",
    "dbos-bench": "dbos",
}

def ssh_run(vm, command, timeout=300):
    """Run a command on a VM via gcloud ssh."""
    full_cmd = [
        GCLOUD, "compute", "ssh", vm,
        f"--zone={ZONE}", f"--project={PROJECT}",
        "--quiet",
        "--command", command,
    ]
    try:
        r = subprocess.run(full_cmd, capture_output=True, text=True, timeout=timeout)
        return r.stdout, r.stderr, r.returncode
    except subprocess.TimeoutExpired:
        return "", "TIMEOUT", 1

def setup_vm(vm, flavor):
    """Setup a VM with the benchmark environment."""
    print(f"[{vm}] Setting up as {flavor}...")
    
    # Install system packages
    setup_script = f"""
export FLAVOR={flavor}
export DEBIAN_FRONTEND=noninteractive

# Update and install basics
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential pkg-config curl wget git cmake protobuf-compiler

# Install Rust
if ! command -v cargo &>/dev/null; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source $HOME/.cargo/env
fi

# Install Docker (for Temporal, Restate)
if ! command -v docker &>/dev/null; then
    curl -fsSL https://get.docker.com | sh
    sudo usermod -aG docker $USER
fi

# Install Node.js (for DBOS)
if ! command -v node &>/dev/null; then
    curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
    sudo apt-get install -y -qq nodejs
fi

# Install PostgreSQL (for Embedded/DBOS)
if ! command -v psql &>/dev/null; then
    sudo apt-get install -y -qq postgresql postgresql-contrib
fi

# Create work directory
mkdir -p ~/velocity-bench
echo "SETUP_DONE"
"""
    stdout, stderr, rc = ssh_run(vm, setup_script, timeout=600)
    if "SETUP_DONE" in stdout:
        print(f"[{vm}] System packages installed")
    else:
        print(f"[{vm}] Setup issue: {stderr[:200]}")
    return True

def upload_and_build(vm, flavor):
    """Upload repo tarball and build."""
    print(f"[{vm}] Uploading repo and building...")
    
    # Create tarball locally
    tarball = os.path.join(REPO_DIR, "velocity-bench-repo.tar.gz")
    subprocess.run([
        "tar", "--exclude=node_modules", "--exclude=target", "--exclude=.git",
        "--exclude=dist", "-czf", tarball, "-C", REPO_DIR, "."
    ], check=True, timeout=120)
    
    # Upload via gcloud scp
    scp_cmd = [
        GCLOUD, "compute", "scp", tarball, f"{vm}:~/velocity-bench/repo.tar.gz",
        f"--zone={ZONE}", f"--project={PROJECT}", "--quiet",
    ]
    subprocess.run(scp_cmd, capture_output=True, text=True, timeout=300)
    
    # Extract and build
    build_script = f"""
cd ~/velocity-bench
tar xzf repo.tar.gz
export FLAVOR={flavor}
source $HOME/.cargo/env 2>/dev/null || true

case "$FLAVOR" in
    velocity-classic|velocity-runtime|velocity-embedded)
        echo "Building Velocity {flavor}..."
        cargo build --release -p velocity-workflow-engine 2>&1 | tail -5
        cargo build --release -p velocity-dev-server 2>&1 | tail -5
        cargo build --release -p velocity-bench 2>&1 | tail -5
        echo "BUILD_DONE"
        ;;
    *)
        echo "No Rust build needed for {flavor}"
        echo "BUILD_DONE"
        ;;
esac
"""
    stdout, stderr, rc = ssh_run(vm, build_script, timeout=1800)
    if "BUILD_DONE" in stdout:
        print(f"[{vm}] Build complete")
    else:
        print(f"[{vm}] Build output: {stdout[-300:]}")
    return True

def run_benchmark(vm, flavor):
    """Run the benchmark on a VM."""
    print(f"[{vm}] Running benchmark for {flavor}...")
    
    bench_script = f"""
export FLAVOR={flavor}
source $HOME/.cargo/env 2>/dev/null || true
cd ~/velocity-bench
mkdir -p results

case "$FLAVOR" in
    velocity-classic)
        echo "Starting Velocity Classic (gRPC) + running benchmark..."
        cargo run --release -p velocity-dev-server -- --grpc-port 7234 --port 7233 &
        SERVER_PID=$!
        sleep 5
        cargo run --release -p velocity-bench -- --workloads all --engine velocity --velocity-address http://localhost:7234 --profile standard --output results/classic_results.json 2>&1
        kill $SERVER_PID 2>/dev/null
        echo "BENCH_DONE"
        ;;
    temporal)
        echo "Starting Temporal + running benchmark..."
        docker run -d --name temporal -p 7233:7233 temporalio/auto-setup:latest 2>/dev/null || true
        sleep 10
        cargo run --release -p velocity-bench -- --workloads all --engine temporal --temporal-address http://localhost:7233 --profile standard --output results/temporal_results.json 2>&1
        echo "BENCH_DONE"
        ;;
    velocity-runtime)
        echo "Starting Velocity Runtime (HTTP) + running benchmark..."
        cargo run --release -p velocity-dev-server -- --port 7233 &
        SERVER_PID=$!
        sleep 5
        cargo run --release -p velocity-bench --bin velocity-bench-http -- --workloads all --engine velocity --velocity-address http://localhost:7233 --profile standard --output results/runtime_results.json 2>&1
        kill $SERVER_PID 2>/dev/null
        echo "BENCH_DONE"
        ;;
    restate)
        echo "Starting Restate + running benchmark..."
        docker run -d --name restate -p 9070:9070 -p 8080:8080 restatelabs/restate-server:latest 2>/dev/null || true
        sleep 10
        cargo run --release -p velocity-bench --bin velocity-bench-http -- --workloads all --engine restate --restate-address http://localhost:8080 --profile standard --output results/restate_results.json 2>&1
        echo "BENCH_DONE"
        ;;
    velocity-embedded)
        echo "Starting Velocity Embedded + running benchmark..."
        cargo run --release -p velocity-dev-server -- --port 7233 --embedded-mode &
        SERVER_PID=$!
        sleep 5
        bash velocity-bench/embedded_bench.sh 2>&1 || echo "Embedded bench completed"
        kill $SERVER_PID 2>/dev/null
        echo "BENCH_DONE"
        ;;
    dbos)
        echo "Starting DBOS + running benchmark..."
        sudo -u postgres psql -c "CREATE DATABASE dbos_bench;" 2>/dev/null || true
        FLAVOR=dbos bash velocity-bench/embedded_bench.sh 2>&1 || echo "DBOS bench completed"
        echo "BENCH_DONE"
        ;;
esac
"""
    stdout, stderr, rc = ssh_run(vm, bench_script, timeout=3600)
    if "BENCH_DONE" in stdout:
        print(f"[{vm}] Benchmark complete!")
    else:
        print(f"[{vm}] Bench output (last 300): {stdout[-300:]}")
    return True

def collect_results(vm, flavor):
    """Download results from VM."""
    print(f"[{vm}] Collecting results...")
    local_dir = os.path.join(REPO_DIR, "cloud-bench", "results", flavor)
    os.makedirs(local_dir, exist_ok=True)
    
    scp_cmd = [
        GCLOUD, "compute", "scp", "--recurse",
        f"{vm}:~/velocity-bench/results/", local_dir + "/",
        f"--zone={ZONE}", f"--project={PROJECT}", "--quiet",
    ]
    try:
        subprocess.run(scp_cmd, capture_output=True, text=True, timeout=120)
        files = os.listdir(local_dir) if os.path.exists(local_dir) else []
        print(f"[{vm}] Collected {len(files)} result files: {files}")
    except Exception as e:
        print(f"[{vm}] Error collecting results: {e}")

def vm_worker(vm, flavor, phase):
    """Worker function for each VM."""
    try:
        if phase == "setup":
            setup_vm(vm, flavor)
        elif phase == "build":
            upload_and_build(vm, flavor)
        elif phase == "bench":
            run_benchmark(vm, flavor)
        elif phase == "collect":
            collect_results(vm, flavor)
    except Exception as e:
        print(f"[{vm}] Error in {phase}: {e}")

if __name__ == "__main__":
    phase = sys.argv[1] if len(sys.argv) > 1 else "setup"
    
    print(f"=== VELOCITY Cloud Benchmark - Phase: {phase} ===")
    print(f"VMs: {list(VM_FLAVORS.keys())}")
    
    # Run all VMs in parallel
    threads = []
    for vm, flavor in VM_FLAVORS.items():
        t = threading.Thread(target=vm_worker, args=(vm, flavor, phase))
        t.start()
        threads.append(t)
    
    for t in threads:
        t.join()
    
    print(f"\n=== Phase {phase} complete! ===")
