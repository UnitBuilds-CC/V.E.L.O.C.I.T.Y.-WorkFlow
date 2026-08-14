#!/bin/bash
# Deploy code to VM via tar+scp (no GitHub auth needed)
set -e

echo "=== Deploy on $(hostname) ==="
REPO_DIR="$HOME/V.E.L.O.C.I.T.Y.-WorkFlow"

# Check if tarball was uploaded
if [ -f /tmp/velocity-repo.tar.gz ]; then
    echo "Found uploaded tarball, extracting..."
    rm -rf "$REPO_DIR"
    mkdir -p "$REPO_DIR"
    tar xzf /tmp/velocity-repo.tar.gz -C "$REPO_DIR"
    cd "$REPO_DIR"
    echo "Deployed: $(git log --oneline -1 2>/dev/null || echo 'extracted')"
    ls bench-suite/ 2>/dev/null | head -10
else
    echo "No tarball found at /tmp/velocity-repo.tar.gz"
    ls -la /tmp/ 2>/dev/null | head -10
fi
echo "=== Done ==="
