#!/bin/bash
set -e
export PATH="$HOME/.cargo/bin:$PATH"

echo "=== Extracting source ==="
cd ~
rm -rf vel-bench
mkdir vel-bench
cd vel-bench
tar -xzf ~/vel-ws.tar.gz
echo "Files extracted:"
ls Cargo.toml velocity-workflow-core/Cargo.toml

echo "=== Building velocity-server and velocity-bench ==="
cargo build --release -p velocity-workflow-server -p velocity-bench --bin velocity-server --bin velocity-bench 2>&1 | tail -5
echo "BUILD_DONE exit=$?"

echo "=== Verifying binaries ==="
ls -lh target/release/velocity-server target/release/velocity-bench
