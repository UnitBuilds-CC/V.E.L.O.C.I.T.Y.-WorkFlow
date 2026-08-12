"""Cloud benchmark orchestrator using direct SSH (bypasses gcloud plink issues on Windows).

Uses system OpenSSH with gcloud's generated key at ~/.ssh/google_compute_engine.
"""
import json
import subprocess
import os
import sys
import time
import threading
import tarfile

PROJECT = "velocity-live-test-001"
ZONE = "us-east1-b"
REPO_DIR = r"C:\Users\visse\OneDrive\Documents\Velocity-workflow"
SSH_KEY = os.path.join(os.environ["USERPROFILE"], ".ssh", "google_compute_engine")
SSH_USER = "ian_unitbuilds_com"
SSH_OPTS = [
    "-o", "StrictHostKeyChecking=no",
    "-o", "UserKnownHostsFile=/dev/null",
    "-o", "LogLevel=ERROR",
    "-o", "ConnectTimeout=10",
    "-i", SSH_KEY,
]

# VM name -> (external IP, flavor)
VM_CONFIG = {}

def load_vm_ips():
    """Load VM IPs from vm_ips.json."""
    ips_path = os.path.join(os.path.dirname(__file__), "vm_ips.json")
    with open(ips_path) as f:
        ips = json.load(f)
    
    flavor_map = {
        "velocity-classic": "velocity-classic",
        "velocity-runtime": "velocity-runtime",
        "velocity-embedded": "velocity-embedded",
        "temporal-bench": "temporal",
        "restate-bench": "restate",
        "dbos-bench": "dbos",
    }
    
    for vm_name, ip in ips.items():
        if vm_name in flavor_map:
            VM_CONFIG[vm_name] = {"ip": ip, "flavor": flavor_map[vm_name]}

def ssh_run(vm_name, command, timeout=600):
    """Run a command on a VM via direct SSH."""
    ip = VM_CONFIG[vm_name]["ip"]
    cmd = ["ssh"] + SSH_OPTS + [f"{SSH_USER}@{ip}", command]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                          encoding='utf-8', errors='replace')
        return r.stdout, r.stderr, r.returncode
    except subprocess.TimeoutExpired:
        return "", "TIMEOUT", 1
    except Exception as e:
        return "", str(e), 1

def scp_upload(vm_name, local_path, remote_path, timeout=600):
    """Upload a file to a VM via SCP."""
    ip = VM_CONFIG[vm_name]["ip"]
    cmd = ["scp"] + SSH_OPTS + [local_path, f"{SSH_USER}@{ip}:{remote_path}"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                          encoding='utf-8', errors='replace')
        return r.returncode == 0, r.stderr
    except subprocess.TimeoutExpired:
        return False, "TIMEOUT"
    except Exception as e:
        return False, str(e)

def scp_download(vm_name, remote_path, local_dir, timeout=120):
    """Download files from a VM via SCP."""
    ip = VM_CONFIG[vm_name]["ip"]
    os.makedirs(local_dir, exist_ok=True)
    cmd = ["scp"] + SSH_OPTS + ["-r", f"{SSH_USER}@{ip}:{remote_path}", local_dir]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                          encoding='utf-8', errors='replace')
        return r.returncode == 0, r.stderr
    except subprocess.TimeoutExpired:
        return False, "TIMEOUT"
    except Exception as e:
        return False, str(e)

def test_ssh(vm_name):
    """Test SSH connectivity."""
    stdout, stderr, rc = ssh_run(vm_name, "echo SSH_OK", timeout=15)
    return "SSH_OK" in stdout

def setup_vm(vm_name, flavor):
    """Setup a VM with the benchmark environment."""
    print(f"[{vm_name}] Testing SSH connectivity...")
    if not test_ssh(vm_name):
        print(f"[{vm_name}] SSH connection failed!")
        return False

    print(f"[{vm_name}] SSH works. Setting up as {flavor}...")

    setup_cmd = """
export DEBIAN_FRONTEND=noninteractive && \
sudo apt-get update -qq && \
sudo apt-get install -y -qq build-essential pkg-config curl wget git cmake protobuf-compiler && \
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
source $HOME/.cargo/env && \
curl -fsSL https://get.docker.com | sh && \
sudo usermod -aG docker $USER && \
mkdir -p ~/velocity-bench/results && \
echo SETUP_DONE
"""

    stdout, stderr, rc = ssh_run(vm_name, setup_cmd, timeout=600)
    if "SETUP_DONE" in stdout:
        print(f"[{vm_name}] Setup complete!")
        return True
    else:
        print(f"[{vm_name}] Setup issue. stdout_tail={stdout[-200:]}, stderr_tail={stderr[-200:]}")
        return False

def upload_and_build(vm_name, flavor):
    """Upload repo and build."""
    print(f"[{vm_name}] Creating tarball...")
    tarball = os.path.join(REPO_DIR, "velocity-bench-repo.tar.gz")

    if not os.path.exists(tarball):
        with tarfile.open(tarball, "w:gz") as tar:
            for item in os.listdir(REPO_DIR):
                if item in ('node_modules', '.git', 'target', 'dist', '.pytest_cache',
                            'velocity-bench-repo.tar.gz', 'cloud-bench/results'):
                    continue
                tar.add(os.path.join(REPO_DIR, item), arcname=item)

    size_mb = os.path.getsize(tarball) // 1024 // 1024
    print(f"[{vm_name}] Uploading tarball ({size_mb}MB)...")
    ok, err = scp_upload(vm_name, tarball, "~/velocity-bench/repo.tar.gz", timeout=600)
    if not ok:
        print(f"[{vm_name}] Upload failed: {err}")
        return False
    print(f"[{vm_name}] Upload complete.")

    print(f"[{vm_name}] Building (flavor={flavor})...")
    build_cmd = f"""
source $HOME/.cargo/env 2>/dev/null && \
cd ~/velocity-bench && \
tar xzf repo.tar.gz && \
echo EXTRACTED && \
case '{flavor}' in
    velocity-classic|velocity-runtime|velocity-embedded)
        cargo build --release -p velocity-workflow-server 2>&1 | tail -5 && \
        cargo build --release -p velocity-bench 2>&1 | tail -5 && \
        echo BUILD_DONE
        ;;
    *)
        echo BUILD_DONE
        ;;
esac
"""
    stdout, stderr, rc = ssh_run(vm_name, build_cmd, timeout=1800)
    if "BUILD_DONE" in stdout:
        print(f"[{vm_name}] Build complete!")
        return True
    else:
        print(f"[{vm_name}] Build output: {stdout[-400:]}")
        return False

def run_benchmark(vm_name, flavor):
    """Run benchmark on VM."""
    print(f"[{vm_name}] Running benchmark for {flavor}...")

    bench_cmd = f"""
source $HOME/.cargo/env 2>/dev/null && \
cd ~/velocity-bench && \
mkdir -p results && \
case '{flavor}' in
    velocity-classic)
        ./target/release/velocity-server --ip 0.0.0.0 --grpc-port 7234 > /tmp/velocity.log 2>&1 &
        SERVER_PID=$!
        sleep 10
        cargo run --release -p velocity-bench --bin velocity-bench -- --workloads all --engine velocity --velocity-address http://localhost:7234 --profile standard --output ~/velocity-bench/results/classic_results.json 2>&1
        kill $SERVER_PID 2>/dev/null
        ;;
    temporal)
        docker run -d --name temporal -p 7233:7233 temporalio/auto-setup:latest 2>/dev/null || true
        sleep 15
        cargo run --release -p velocity-bench --bin velocity-bench -- --workloads all --engine temporal --temporal-address http://localhost:7233 --profile standard --output ~/velocity-bench/results/temporal_results.json 2>&1
        ;;
    velocity-runtime)
        ./target/release/velocity-server --ip 0.0.0.0 --grpc-port 7233 > /tmp/velocity.log 2>&1 &
        SERVER_PID=$!
        sleep 10
        cargo run --release -p velocity-bench --bin velocity-bench -- --workloads all --engine velocity --velocity-address http://localhost:7233 --profile standard --output ~/velocity-bench/results/runtime_results.json 2>&1
        kill $SERVER_PID 2>/dev/null
        ;;
    restate)
        docker run -d --name restate -p 8080:8080 restatelabs/restate-server:latest 2>/dev/null || true
        sleep 15
        cargo run --release -p velocity-bench --bin velocity-bench -- --workloads all --engine restate --restate-address http://localhost:8080 --profile standard --output ~/velocity-bench/results/restate_results.json 2>&1
        ;;
    velocity-embedded)
        ./target/release/velocity-server --ip 0.0.0.0 --grpc-port 7233 > /tmp/velocity.log 2>&1 &
        SERVER_PID=$!
        sleep 10
        echo '{{"flavor":"embedded","status":"placeholder"}}' > ~/velocity-bench/results/embedded_results.json
        kill $SERVER_PID 2>/dev/null
        ;;
    dbos)
        echo '{{"flavor":"dbos","status":"placeholder"}}' > ~/velocity-bench/results/dbos_results.json
        ;;
esac && \
echo BENCH_DONE
"""
    stdout, stderr, rc = ssh_run(vm_name, bench_cmd, timeout=3600)
    if "BENCH_DONE" in stdout:
        print(f"[{vm_name}] Benchmark complete!")
        return True
    else:
        print(f"[{vm_name}] Bench output: {stdout[-400:]}")
        return False

def collect_results(vm_name, flavor):
    """Download results."""
    print(f"[{vm_name}] Collecting results...")
    local_dir = os.path.join(REPO_DIR, "cloud-bench", "results", flavor)
    os.makedirs(local_dir, exist_ok=True)

    ok, err = scp_download(vm_name, "~/velocity-bench/results/", local_dir, timeout=120)
    if ok:
        files = os.listdir(local_dir) if os.path.exists(local_dir) else []
        print(f"[{vm_name}] Collected {len(files)} files: {files}")
    else:
        print(f"[{vm_name}] SCP download issue: {err}")
        # Fallback: use SSH to cat results
        stdout, _, _ = ssh_run(vm_name, "cat ~/velocity-bench/results/*.json 2>/dev/null || echo NO_RESULTS", timeout=30)
        if stdout and "NO_RESULTS" not in stdout:
            result_file = os.path.join(local_dir, "raw_output.txt")
            with open(result_file, 'w') as f:
                f.write(stdout)
            print(f"[{vm_name}] Saved raw SSH output to {result_file}")

def vm_worker(vm_name, flavor, phase):
    try:
        if phase == "setup":
            setup_vm(vm_name, flavor)
        elif phase == "build":
            upload_and_build(vm_name, flavor)
        elif phase == "bench":
            run_benchmark(vm_name, flavor)
        elif phase == "collect":
            collect_results(vm_name, flavor)
    except Exception as e:
        print(f"[{vm_name}] Error in {phase}: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    phase = sys.argv[1] if len(sys.argv) > 1 else "setup"
    
    load_vm_ips()
    if not VM_CONFIG:
        print("ERROR: No VM IPs found. Run provision_vms.py first.")
        sys.exit(1)
    
    print(f"=== VELOCITY Cloud Benchmark - Phase: {phase} ===")
    print(f"Project: {PROJECT}, Zone: {ZONE}")
    print(f"SSH Key: {SSH_KEY}")
    print(f"VMs:")
    for vm, cfg in VM_CONFIG.items():
        print(f"  {vm}: {cfg['ip']} (flavor={cfg['flavor']})")
    print()

    threads = []
    for vm_name, cfg in VM_CONFIG.items():
        t = threading.Thread(target=vm_worker, args=(vm_name, cfg["flavor"], phase))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    print(f"\n=== Phase {phase} complete! ===")
