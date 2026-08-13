"""Parallel cloud benchmark — upload, build, run, collect on all 6 VMs.

Usage: python run_parallel_bench.py
"""
import json, subprocess, os, sys, time, threading, tarfile

REPO_DIR = r"C:\Users\visse\OneDrive\Documents\Velocity-workflow"
SSH_KEY = os.path.join(os.environ["USERPROFILE"], ".ssh", "google_compute_engine")
SSH_USER = "ian_unitbuilds_com"
SSH_OPTS = ["-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
            "-o", "LogLevel=ERROR", "-o", "ConnectTimeout=10", "-i", SSH_KEY]

VMS = {
    "classic":    {"ip": "34.26.15.38",    "flavor": "classic"},
    "runtime":    {"ip": "35.231.148.207", "flavor": "runtime"},
    "embedded":   {"ip": "34.75.54.239",   "flavor": "embedded"},
    "temporal":   {"ip": "34.139.181.220", "flavor": "temporal"},
    "restate":    {"ip": "35.227.44.141",  "flavor": "restate"},
    "dbos":       {"ip": "34.26.33.56",    "flavor": "dbos"},
}

log_lock = threading.Lock()
def log(vm, msg):
    with log_lock:
        print(f"[{vm}] {msg}", flush=True)

def ssh(ip, cmd, timeout=900):
    try:
        r = subprocess.run(["ssh"] + SSH_OPTS + [f"{SSH_USER}@{ip}", cmd],
                          capture_output=True, text=True, timeout=timeout,
                          encoding='utf-8', errors='replace')
        return r.stdout, r.stderr, r.returncode
    except subprocess.TimeoutExpired:
        return "", "TIMEOUT", 1
    except Exception as e:
        return "", str(e), 1

def scp(ip, local, remote, timeout=600):
    try:
        r = subprocess.run(["scp"] + SSH_OPTS + [local, f"{SSH_USER}@{ip}:{remote}"],
                          capture_output=True, text=True, timeout=timeout,
                          encoding='utf-8', errors='replace')
        return r.returncode == 0
    except:
        return False

# ── Phase 1: Create tarball ──────────────────────────────────────────────────
def create_tarball():
    tarball = os.path.join(REPO_DIR, "velocity-bench-repo.tar.gz")
    if os.path.exists(tarball):
        os.remove(tarball)
    print("Creating tarball...")
    skip = {'node_modules', '.git', 'target', 'dist', '.pytest_cache',
            'velocity-bench-repo.tar.gz', 'cloud-bench/results', '.qoder'}
    with tarfile.open(tarball, "w:gz") as tar:
        for item in os.listdir(REPO_DIR):
            if item in skip:
                continue
            tar.add(os.path.join(REPO_DIR, item), arcname=item)
    size = os.path.getsize(tarball) // 1024 // 1024
    print(f"Tarball created: {size}MB")
    return tarball

# ── Phase 2: Upload + Build ──────────────────────────────────────────────────
def upload_and_build(name, ip, flavor, tarball):
    log(name, f"Uploading tarball to {ip}...")
    if not scp(ip, tarball, "~/velocity-bench/repo.tar.gz"):
        log(name, "UPLOAD FAILED")
        return False
    log(name, "Upload done. Extracting + building...")

    build_cmd = """
cd ~/velocity-bench && \
rm -rf src velocity-workflow-* velocity-bench velocity-classic velocity-embedded \
       velocity-dev-server velocity-runtime-* velocity-sdk-* velocity-test-framework \
       velocity-workflow-core velocity-workflow-daemon velocity-workflow-server \
       velocity-workflow-engine proto migrations Cargo.toml Cargo.lock && \
tar xzf repo.tar.gz && \
source $HOME/.cargo/env 2>/dev/null && \
case 'FLAVOR_PLACEHOLDER' in
    classic|runtime|embedded)
        cargo build --release -p velocity-workflow-server 2>&1 | tail -5 && \
        cargo build --release -p velocity-bench 2>&1 | tail -5 && \
        echo BUILD_SUCCESS
        ;;
    temporal)
        cargo build --release -p velocity-bench 2>&1 | tail -5 && \
        echo BUILD_SUCCESS
        ;;
    *)
        echo BUILD_SUCCESS
        ;;
esac
"""
    out, err, rc = ssh(ip, build_cmd.replace('FLAVOR_PLACEHOLDER', flavor), timeout=1800)
    if "BUILD_SUCCESS" in out:
        log(name, "BUILD SUCCESS")
        return True
    else:
        log(name, f"BUILD FAILED: {out[-300:]}")
        return False

# ── Phase 3: Run Benchmark ───────────────────────────────────────────────────
def run_bench(name, ip, flavor):
    log(name, f"Running benchmark...")

    if flavor == "classic":
        # Velocity Classic vs Temporal on same VM (symmetric comparison)
        cmd = """
cd ~/velocity-bench && \
pkill -f velocity-server 2>/dev/null; pkill -f temporal-bridge 2>/dev/null; sleep 2 && \
./target/release/velocity-server --ip 0.0.0.0 --grpc-port 7234 --wal-path '' > /tmp/velocity.log 2>&1 &
VPID=$!
./target/release/temporal-bridge --ip 0.0.0.0 --port 7235 > /tmp/temporal.log 2>&1 &
TPID=$!
sleep 8
./target/release/velocity-bench --engine both \
  --velocity-address http://localhost:7234 \
  --temporal-address http://localhost:7235 \
  --workloads all --profile standard --format json \
  -o ~/velocity-bench/results/classic_comparison.json 2>&1
kill $VPID $TPID 2>/dev/null
echo BENCH_DONE
"""
    elif flavor == "runtime":
        cmd = """
cd ~/velocity-bench && \
pkill -f velocity-server 2>/dev/null; sleep 2 && \
./target/release/velocity-server --ip 0.0.0.0 --grpc-port 7233 --wal-path '' > /tmp/velocity.log 2>&1 &
VPID=$!
sleep 8
./target/release/velocity-bench --engine velocity \
  --velocity-address http://localhost:7233 \
  --workloads all --profile standard --format json \
  -o ~/velocity-bench/results/runtime_results.json 2>&1
kill $VPID 2>/dev/null
echo BENCH_DONE
"""
    elif flavor == "embedded":
        cmd = """
cd ~/velocity-bench && \
pkill -f velocity-server 2>/dev/null; sleep 2 && \
./target/release/velocity-server --ip 0.0.0.0 --grpc-port 7233 --wal-path '' > /tmp/velocity.log 2>&1 &
VPID=$!
sleep 8
./target/release/velocity-bench --engine velocity \
  --velocity-address http://localhost:7233 \
  --workloads all --profile standard --format json \
  -o ~/velocity-bench/results/embedded_results.json 2>&1
kill $VPID 2>/dev/null
echo BENCH_DONE
"""
    elif flavor == "temporal":
        cmd = """
cd ~/velocity-bench && \
docker rm -f temporal 2>/dev/null; sleep 2 && \
docker run -d --name temporal -p 7233:7233 temporalio/auto-setup:latest 2>/dev/null && \
sleep 20 && \
./target/release/velocity-bench --engine temporal \
  --temporal-address http://localhost:7233 \
  --workloads all --profile standard --format json \
  -o ~/velocity-bench/results/temporal_results.json 2>&1
echo BENCH_DONE
"""
    elif flavor == "restate":
        cmd = """
cd ~/velocity-bench && \
echo '{"flavor":"restate","status":"adapter_not_implemented"}' > ~/velocity-bench/results/restate_results.json && \
echo BENCH_DONE
"""
    elif flavor == "dbos":
        cmd = """
cd ~/velocity-bench && \
echo '{"flavor":"dbos","status":"placeholder"}' > ~/velocity-bench/results/dbos_results.json && \
echo BENCH_DONE
"""
    else:
        log(name, f"Unknown flavor: {flavor}")
        return False

    out, err, rc = ssh(ip, cmd, timeout=1800)
    if "BENCH_DONE" in out:
        log(name, "BENCHMARK COMPLETE")
        return True
    else:
        log(name, f"BENCH ISSUE: {out[-400:]}")
        return False

# ── Phase 4: Collect Results ─────────────────────────────────────────────────
def collect(name, ip, flavor):
    log(name, "Collecting results...")
    local_dir = os.path.join(REPO_DIR, "cloud-bench", "results")
    os.makedirs(local_dir, exist_ok=True)

    # Download JSON files
    out, _, _ = ssh(ip, "cat ~/velocity-bench/results/*.json 2>/dev/null", timeout=30)
    if out and out.strip():
        # Save each file
        for line in out.split("\n"):
            pass  # Just save the raw output
        fname = os.path.join(local_dir, f"{flavor}_results_raw.json")
        with open(fname, 'w') as f:
            f.write(out)
        log(name, f"Saved results to {fname}")

        # Also try to get the specific comparison file
        specific_files = {
            "classic": "classic_comparison.json",
            "runtime": "runtime_results.json",
            "embedded": "embedded_results.json",
            "temporal": "temporal_results.json",
        }
        if flavor in specific_files:
            out2, _, _ = ssh(ip, f"cat ~/velocity-bench/results/{specific_files[flavor]} 2>/dev/null", timeout=30)
            if out2 and out2.strip():
                dest = os.path.join(local_dir, specific_files[flavor])
                with open(dest, 'w') as f:
                    f.write(out2)
                log(name, f"Saved {specific_files[flavor]}")
    else:
        log(name, "No results found")

# ── Main ─────────────────────────────────────────────────────────────────────
def vm_pipeline(name, cfg, tarball, skip_build=False):
    ip, flavor = cfg["ip"], cfg["flavor"]
    try:
        # Check SSH
        out, _, rc = ssh(ip, "echo OK", timeout=15)
        if "OK" not in out:
            log(name, "SSH FAILED")
            return
        log(name, "SSH OK")

        # Build
        if not skip_build:
            if not upload_and_build(name, ip, flavor, tarball):
                log(name, "Build failed, trying to run bench anyway...")

        # Benchmark
        run_bench(name, ip, flavor)

        # Collect
        collect(name, ip, flavor)
    except Exception as e:
        log(name, f"ERROR: {e}")
        import traceback; traceback.print_exc()

if __name__ == "__main__":
    print("=" * 60)
    print("VELOCITY Cloud Benchmark — Parallel Pipeline")
    print("=" * 60)

    tarball = create_tarball()

    print(f"\nStarting parallel benchmark on {len(VMS)} VMs...")
    start = time.time()

    threads = []
    for name, cfg in VMS.items():
        t = threading.Thread(target=vm_pipeline, args=(name, cfg, tarball))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    elapsed = time.time() - start
    print(f"\n{'=' * 60}")
    print(f"All benchmarks complete in {elapsed:.0f}s ({elapsed/60:.1f}min)")
    print(f"Results in: {os.path.join(REPO_DIR, 'cloud-bench', 'results')}")
    print("=" * 60)
