#!/bin/bash
export PATH="/home/ian_unitbuilds_com/.cargo/bin:$PATH"
echo "Cargo check:"
which cargo && cargo --version || echo "NO CARGO"
echo "Rustup check:"
ls /home/ian_unitbuilds_com/.cargo/bin/ 2>/dev/null | head -10 || echo "no cargo bin dir"
