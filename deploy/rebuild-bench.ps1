$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'

# Clean and create directory structure
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="sudo rm -rf /home/bench-context/velocity-bench; sudo mkdir -p /home/bench-context/velocity-bench/src /home/bench-context/velocity-bench/proto; sudo chown -R ian_unitbuilds_com:ian_unitbuilds_com /home/bench-context"
Start-Sleep -Seconds 3

# Upload individual source files
$srcFiles = @("main.rs", "metrics.rs", "report.rs", "workloads.rs", "engine.rs", "lib.rs", "temporal_bridge.rs")
foreach ($f in $srcFiles) {
    & $gcloud compute scp "c:\Users\visse\OneDrive\Documents\Velocity-workflow\velocity-bench\src\$f" "velocity-classic:/home/bench-context/velocity-bench/src/$f" --zone=us-east1-b --quiet
}

# Upload proto
& $gcloud compute scp "c:\Users\visse\OneDrive\Documents\Velocity-workflow\velocity-bench\proto\benchmark.proto" "velocity-classic:/home/bench-context/velocity-bench/proto/benchmark.proto" --zone=us-east1-b --quiet

# Upload Cargo.toml and build.rs
& $gcloud compute scp "c:\Users\visse\OneDrive\Documents\Velocity-workflow\velocity-bench\Cargo.toml" "velocity-classic:/home/bench-context/velocity-bench/Cargo.toml" --zone=us-east1-b --quiet
& $gcloud compute scp "c:\Users\visse\OneDrive\Documents\Velocity-workflow\velocity-bench\build.rs" "velocity-classic:/home/bench-context/velocity-bench/build.rs" --zone=us-east1-b --quiet
Start-Sleep -Seconds 2

# Verify
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="find /home/bench-context/velocity-bench -type f | sort"
Start-Sleep -Seconds 2

# Rebuild
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="cd /home/bench-context; sudo docker build -f Dockerfile.bench -t velocity-bench . 2>&1 | tail -10"
