#!/bin/bash
# Update Rust and build Velocity on a GCE VM
set -e
REPO_DIR="$HOME/V.E.L.O.C.I.T.Y.-WorkFlow"

# Source cargo env
export PATH="$HOME/.cargo/bin:$PATH"
source "$HOME/.cargo/env" 2>/dev/null || true

echo "=== Current Rust ==="
echo "Rust: $(rustc --version 2>/dev/null || echo 'NOT FOUND')"
which rustc 2>/dev/null || echo "rustc not in PATH"

echo ""
echo "=== Updating Rust ==="
rustup update stable 2>&1 | tail -3
rustup default stable
echo "Updated Rust: $(rustc --version)"

cd "$REPO_DIR"

# Remove old Cargo.lock to regenerate
rm -f Cargo.lock

echo ""
echo "=== Building Velocity server ==="
cargo build --release -p velocity-workflow-server 2>&1 | tail -5
echo "Server: $(ls -lh target/release/velocity-server 2>/dev/null || echo 'NOT FOUND')"

echo ""
echo "=== Building velocity-bench ==="
cargo build --release -p velocity-bench 2>&1 | tail -5
echo "Bench: $(ls -lh target/release/velocity-bench 2>/dev/null || echo 'NOT FOUND')"

echo ""
echo "=== Build complete ==="
