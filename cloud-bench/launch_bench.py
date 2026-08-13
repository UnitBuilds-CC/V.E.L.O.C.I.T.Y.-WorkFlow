"""Launch benchmarks on all VMs using nohup, then poll for results."""
import subprocess, os, time, threading, json

SSH_KEY = os.path.join(os.environ["USERPROFILE"], ".ssh", "google_compute_engine")
SSH_USER = "ian_unitbuilds_com"
SSH_OPTS = ["-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
            "-o", "LogLevel=ERROR", "-o", "ConnectTimeout=10", "-i", SSH_KEY]
REPO_DIR = r"C:\Users\visse\OneDrive\Documents\Velocity-workflow"

VMS = {
    "classic":  {"ip": "34.26.15.38",    "cmd": r"""
cd ~/velocity-bench && mkdir -p results && \
nohup bash -c '
./target/release/velocity-server --ip 0.0.0.0 --grpc-port 7234 --wal-path "" > /tmp/velocity.log 2>&1 &
VPID=$!
./target/release/temporal-bridge --ip 0.0.0.0 --grpc-port 7235 > /tmp/temporal.log 2>&1 &
TPID=$!
sleep 8
./target/release/velocity-bench --engine both \
  --velocity-address http://localhost:7234 \
  --temporal-address http://localhost:7235 \
  --workloads all --profile standard --format json \
  -o ~/velocity-bench/results/classic_comparison.json
kill $VPID $TPID 2>/dev/null
echo BENCH_DONE > /tmp/bench_status
' > /tmp/bench_stdout.log 2>&1 &
echo LAUNCHED
"""},
    "runtime":  {"ip": "35.231.148.207", "cmd": r"""
cd ~/velocity-bench && mkdir -p results && \
nohup bash -c '
./target/release/velocity-server --ip 0.0.0.0 --grpc-port 7233 --wal-path "" > /tmp/velocity.log 2>&1 &
VPID=$!
sleep 8
./target/release/velocity-bench --engine velocity \
  --velocity-address http://localhost:7233 \
  --workloads all --profile standard --format json \
  -o ~/velocity-bench/results/runtime_results.json
kill $VPID 2>/dev/null
echo BENCH_DONE > /tmp/bench_status
' > /tmp/bench_stdout.log 2>&1 &
echo LAUNCHED
"""},
    "embedded": {"ip": "34.75.54.239",   "cmd": r"""
cd ~/velocity-bench && mkdir -p results && \
nohup bash -c '
./target/release/velocity-server --ip 0.0.0.0 --grpc-port 7233 --wal-path "" > /tmp/velocity.log 2>&1 &
VPID=$!
sleep 8
./target/release/velocity-bench --engine velocity \
  --velocity-address http://localhost:7233 \
  --workloads all --profile standard --format json \
  -o ~/velocity-bench/results/embedded_results.json
kill $VPID 2>/dev/null
echo BENCH_DONE > /tmp/bench_status
' > /tmp/bench_stdout.log 2>&1 &
echo LAUNCHED
"""},
    "temporal": {"ip": "34.139.181.220", "cmd": r"""
cd ~/velocity-bench && mkdir -p results && \
nohup bash -c '
./target/release/velocity-bench --engine temporal \
  --temporal-address http://localhost:7233 \
  --workloads all --profile standard --format json \
  -o ~/velocity-bench/results/temporal_results.json
echo BENCH_DONE > /tmp/bench_status
' > /tmp/bench_stdout.log 2>&1 &
echo LAUNCHED
"""},
}

lock = threading.Lock()
def log(name, msg):
    with lock:
        print(f"[{name}] {msg}", flush=True)

def ssh(ip, cmd, timeout=30):
    try:
        r = subprocess.run(["ssh"] + SSH_OPTS + [f"{SSH_USER}@{ip}", cmd],
                          capture_output=True, text=True, timeout=timeout,
                          encoding='utf-8', errors='replace')
        return r.stdout, r.returncode
    except Exception as e:
        return str(e), -1

def launch(name, cfg):
    ip = cfg["ip"]
    log(name, f"Launching benchmark on {ip}...")
    out, rc = ssh(ip, cfg["cmd"])
    if "LAUNCHED" in out:
        log(name, "Benchmark launched (running in background)")
    else:
        log(name, f"Launch issue: {out[:200]}")

def check_done(name, ip):
    out, rc = ssh(ip, "cat /tmp/bench_status 2>/dev/null || echo NOT_DONE")
    return "BENCH_DONE" in out

def collect(name, ip, filename):
    out, rc = ssh(ip, f"cat ~/velocity-bench/results/{filename} 2>/dev/null || echo NO_FILE", timeout=30)
    if out and "NO_FILE" not in out:
        dest = os.path.join(REPO_DIR, "cloud-bench", "results", filename)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(dest, 'w', encoding='utf-8') as f:
            f.write(out)
        log(name, f"Collected {filename} ({len(out)} bytes)")
        return True
    else:
        log(name, f"No results file yet")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("VELOCITY Cloud Benchmark — Launch & Poll")
    print("=" * 60)

    # Launch all benchmarks
    threads = []
    for name, cfg in VMS.items():
        t = threading.Thread(target=launch, args=(name, cfg))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    print(f"\nAll benchmarks launched at {time.strftime('%H:%M:%S')}")
    print("Polling for completion (max 20 minutes)...\n")

    start = time.time()
    done = {name: False for name in VMS}
    results_files = {
        "classic": "classic_comparison.json",
        "runtime": "runtime_results.json",
        "embedded": "embedded_results.json",
        "temporal": "temporal_results.json",
    }

    while time.time() - start < 1200:  # 20 minutes max
        all_done = True
        for name, cfg in VMS.items():
            if done[name]:
                continue
            if check_done(name, cfg["ip"]):
                done[name] = True
                log(name, "BENCHMARK COMPLETE")
                collect(name, cfg["ip"], results_files[name])
            else:
                all_done = False

        if all_done:
            print(f"\nAll benchmarks complete in {time.time()-start:.0f}s!")
            break

        elapsed = time.time() - start
        remaining = [n for n, d in done.items() if not d]
        print(f"  [{elapsed:.0f}s] Still running: {', '.join(remaining)}", flush=True)
        time.sleep(30)

    # Final collection even if not all done
    print("\n--- Final Results Collection ---")
    for name, cfg in VMS.items():
        if not done[name]:
            log(name, "Still running, collecting partial results...")
            collect(name, cfg["ip"], results_files[name])

    # Also get the bench stdout logs
    print("\n--- Benchmark Logs ---")
    for name, cfg in VMS.items():
        out, _ = ssh(cfg["ip"], "tail -20 /tmp/bench_stdout.log 2>/dev/null", timeout=15)
        if out:
            log(name, f"Log tail:\n{out[:500]}")

    print(f"\nTotal time: {time.time()-start:.0f}s")
    print(f"Results in: {os.path.join(REPO_DIR, 'cloud-bench', 'results')}")
