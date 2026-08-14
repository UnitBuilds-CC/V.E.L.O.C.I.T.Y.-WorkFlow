$Zone = "us-east1-b"

# Build velocity-bench on temporal VM (only need the bench binary, not server)
Write-Host "=== Building velocity-bench on temporal-bench ===" -ForegroundColor Cyan
gcloud compute ssh temporal-bench --zone=$Zone --quiet --command "export PATH=`$HOME/.cargo/bin:`$PATH; source `$HOME/.cargo/env 2>/dev/null || true; cd `$HOME/V.E.L.O.C.I.T.Y.-WorkFlow; rm -f Cargo.lock; cargo build --release -p velocity-bench 2>&1 | tail -5; ls -lh target/release/velocity-bench" 2>&1
