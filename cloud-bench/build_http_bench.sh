#!/bin/bash
set -e
export PATH="$HOME/.cargo/bin:$PATH"

echo "Building velocity-bench-http..."
cd ~/velocity-bench/velocity-bench
cargo build --release --bin velocity-bench-http 2>&1

echo "BUILD_COMPLETE"
ls -la target/release/velocity-bench-http
