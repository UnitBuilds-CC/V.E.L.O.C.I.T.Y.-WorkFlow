#!/bin/bash
set -e
export PATH="$HOME/.cargo/bin:$PATH"

echo "Building temporal-bridge..."
cd ~/velocity-bench/velocity-bench
cargo build --release --bin temporal-bridge 2>&1

echo "BUILD_COMPLETE"
ls -la target/release/temporal-bridge
