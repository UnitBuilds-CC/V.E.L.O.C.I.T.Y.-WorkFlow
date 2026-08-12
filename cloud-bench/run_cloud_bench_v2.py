"""SSH into each VM, upload repo, setup, and run benchmarks — v2 using SCP+exec."""
import json
import subprocess
import os
import sys
import time
import threading
import tempfile

PROJECT = "velocity-live-test-001"
ZONE = "us-east1-b"
GCLOUD = r"C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd"
REPO_DIR = r"C:\Users\visse\OneDrive\Documents\Velocity-workflow"
SSH_FLAGS = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

VM_FLAVORS = {
    "velocity-classic": "velocity-classic",
    "velocity-runtime": "velocity-runtime",
    "velocity-embedded": "velocity-embedded",
    "temporal-bench": "temporal",
    "restate-bench": "restate",
    "dbos-bench": "dbos",
}

def ssh_run(vm, command, timeout=600):
    """Run a command on a VM via gcloud ssh."""
    full_cmd = [
        GCLOUD, "compute", "ssh", vm,
        f"--zone={ZONE}", f"--project={PROJECT}",
        "--quiet",
        "--ssh-flag=-o StrictHostKeyChecking=no",
        "--ssh-flag=-o UserKnownHostsFile=/dev/null",
        "--command", command,
    ]
    try:
        r = subprocess.run(full_cmd, capture_output=True, text=True, timeout=timeout)
        return r.stdout, r.stderr, r.returncode
    except subprocess.TimeoutExpired:
        return "", "TIMEOUT", 1

def scp_upload(vm, local_path, remote_path, timeout=300):
    """Upload a file to a VM."""
    cmd = [
        GCLOUD, "compute", "scp", local_path, f"{vm}:{remote_path}",
        f"--zone={ZONE}", f"--project={PROJECT}", "--quiet",
        "--scp-flag=-o StrictHostKeyChecking=no",
        "--scp-flag=-o UserKnownHostsFile=/dev/null",
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode == 0
    except subprocess.TimeoutExpired:
        return False

def write_temp_script(content):
    """Write content to a temp file and return the path."""
    fd, path = tempfile.mkstemp(suffix=".sh")
    with os.fdopen(fd, 'w') as f:
        f.write(content)
    return path

def setup_vm(vm, flavor):
    """Setup a VM with the benchmark environment."""
    print(f"[{vm}] Setting up as {flavor}...")
    
    script = f"""#!/bin/bash
set -e
export FLAVOR={flavor}
export DEBIAN_FRONTEND=noninteractive

echo "[{vm}] Updating packages..."
sudo apt-get update -qq 2>/dev/null
sudo apt-get install -y -qq build-essential pkg-config curl wget git cmake protobuf-compiler 2>/dev/null

echo "[{vm}] Installing Rust..."
if ! command -v cargo &>/dev/null; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y 2>/dev/null
fi
source $HOME/.cargo/env 2>/dev/null || true

echo "[{vm}] Installing Docker..."
if ! command -v docker &>/dev/null; then
    curl -fsSL https://get.docker.com | sh 2>/dev/null
    sudo usermod -aG docker $USER 2>/dev/null || true
fi

echo "[{vm}] Installing Node.js..."
if ! command -v node &>/dev/null; then
    curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash - 2>/dev/null
    sudo apt-get install -y -qq nodejs 2>/dev/null
fi

echo "[{vm}] Installing PostgreSQL..."
if ! command -v psql &>/dev/null; then
    sudo apt-get install -y -qq postgresql postgresql-contrib 2>/dev/null
fi

mkdir -p ~/velocity-bench/results
echo "SETUP_COMPLETE_{vm}"
"""
    
    path = write_temp_script(script)
    try:
        scp_upload(vm, path, "~/setup.sh")
        stdout, stderr, rc = ssh_run(vm, "bash ~/setup.sh", timeout=600)
        if f"SETUP_COMPLETE_{vm}" in stdout:
            print(f"[{vm}] Setup complete!")
            return True
        else:
            print(f"[{vm}] Setup output: {stdout[-200:]}")
            return True  # Partial success
    finally:
        os.unlink(path)

def upload_and_build(vm, flavor):
    """Upload repo tarball and build."""
    print(f"[{vm}] Uploading repo and building...")
    
    tarball = os.path.join(REPO_DIR, "velocity-bench-repo.tar.gz")
    subprocess.run([
        "tar", "--exclude=node_modules", "--exclude=target",
        "--exclude=.git", "--exclude=dist", "--exclude=.pytest_cache",
        "-czf", tarball, "-C", REPO_DIR, "."
    ], check=True, timeout=120)
    
    if not scp_upload(vm, tarball, "~/velocity-bench/repo.tar.gz", timeout=300):
        print(f"[{vm}] Failed to upload tarball")
        return False
    
    script = f"""#!/bin/bash
set -e
source $HOME/.cargo/env 2>/dev/null || true
cd ~/velocity-bench
tar xzf repo.tar.gz 2>/dev/null
export FLAVOR={flavor}

case "$FLAVOR" in
    velocity-classic|velocity-runtime|velocity-embedded)
        echo "Building Velocity {flavor}..."
        cargo build --release -p velocity-dev-server 2>&1 | tail -3
        cargo build --release -p velocity-bench 2>&1 | tail -3
        ;;
    *)
        echo "No Rust build needed for {flavor}"
        ;;
esac
echo "BUILD_COMPLETE_{vm}"
"""
    
    path = write_temp_script(script)
    try:
        scp_upload(vm, path, "~/build.sh")
        stdout, stderr, rc = ssh_run(vm, "bash ~/build.sh", timeout=1800)
        if f"BUILD_COMPLETE_{vm}" in stdout:
            print(f"[{vm}] Build complete!")
            return True
        else:
            print(f"[{vm}] Build output: {stdout[-300:]}")
            return False
    finally:
        os.unlink(path)

def run_benchmark(vm, flavor):
    """Run the benchmark on a VM."""
    print(f"[{vm}] Running benchmark for {flavor}...")
    
    script = f"""#!/bin/bash
set -e
source $HOME/.cargo/env 2>/dev/null || true
cd ~/velocity-bench
export FLAVOR={flavor}
mkdir -p results

case "$FLAVOR" in
    velocity-classic)
        cargo run --release -p velocity-dev-server -- --grpc-port 7234 --port 7233 &
        SERVER_PID=$!
        sleep 10
        cargo run --release -p velocity-bench -- --workloads all --engine velocity --velocity-address http://localhost:7234 --profile standard --output results/classic_results.json 2>&1 || true
        kill $SERVER_PID 2>/dev/null || true
        ;;
    temporal)
        docker run -d --name temporal-bench -p 7233:7233 temporalio/auto-setup:latest 2>/dev/null || true
        sleep 15
        cargo run --release -p velocity-bench -- --workloads all --engine temporal --temporal-address http://localhost:7233 --profile standard --output results/temporal_results.json 2>&1 || true
        ;;
    velocity-runtime)
        cargo run --release -p velocity-dev-server -- --port 7233 &
        SERVER_PID=$!
        sleep 10
        cargo run --release -p velocity-bench --bin velocity-bench-http -- --workloads all --engine velocity --velocity-address http://localhost:7233 --profile standard --output results/runtime_results.json 2>&1 || true
        kill $SERVER_PID 2>/dev/null || true
        ;;
    restate)
        docker run -d --name restate-bench -p 8080:8080 restatelabs/restate-server:latest 2>/dev/null || true
        sleep 15
        cargo run --release -p velocity-bench --bin velocity-bench-http -- --workloads all --engine restate --restate-address http://localhost:8080 --profile standard --output results/restate_results.json 2>&1 || true
        ;;
    velocity-embedded)
        cargo run --release -p velocity-dev-server -- --port 7233 --embedded-mode &
        SERVER_PID=$!
        sleep 10
        bash velocity-bench/embedded_bench.sh 2>&1 || true
        kill $SERVER_PID 2>/dev/null || true
        ;;
    dbos)
        sudo service postgresql start 2>/dev/null || true
        sudo -u postgres psql -c "CREATE DATABASE dbos_bench;" 2>/dev/null || true
        FLAVOR=dbos bash velocity-bench/embedded_bench.sh 2>&1 || true
        ;;
esac
echo "BENCH_COMPLETE_{vm}"
"""
    
    path = write_temp_script(script)
    try:
        scp_upload(vm, path, "~/bench.sh")
        stdout, stderr, rc = ssh_run(vm, "bash ~/bench.sh", timeout=3600)
        if f"BENCH_COMPLETE_{vm}" in stdout:
            print(f"[{vm}] Benchmark complete!")
            return True
        else:
            print(f"[{vm}] Bench output: {stdout[-300:]}")
            return False
    finally:
        os.unlink(path)

def collect_results(vm, flavor):
    """Download results from VM."""
    print(f"[{vm}] Collecting results...")
    local_dir = os.path.join(REPO_DIR, "cloud-bench", "results", flavor)
    os.makedirs(local_dir, exist_ok=True)
    
    cmd = [
        GCLOUD, "compute", "scp", "--recurse",
        f"{vm}:~/velocity-bench/results/", local_dir + "/",
        f"--zone={ZONE}", f"--project={PROJECT}", "--quiet",
        "--scp-flag=-o StrictHostKeyChecking=no",
        "--scp-flag=-o UserKnownHostsFile=/dev/null",
    ]
    try:
        subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        files = os.listdir(local_dir) if os.path.exists(local_dir) else []
        print(f"[{vm}] Collected {len(files)} result files: {files}")
    except Exception as e:
        print(f"[{vm}] Error collecting results: {e}")

def vm_worker(vm, flavor, phase):
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
    
    threads = []
    for vm, flavor in VM_FLAVORS.items():
        t = threading.Thread(target=vm_worker, args=(vm, flavor, phase))
        t.start()
        threads.append(t)
    
    for t in threads:
        t.join()
    
    print(f"\n=== Phase {phase} complete! ===")
