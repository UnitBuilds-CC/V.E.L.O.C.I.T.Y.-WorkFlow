#!/bin/bash
# Deploy latest code to a GCE VM - clone if needed, pull if exists
set -e
REPO_DIR="$HOME/V.E.L.O.C.I.T.Y.-WorkFlow"
REPO_URL="https://github.com/UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow.git"

echo "=== Deploy on $(hostname) ==="

if [ -d "$REPO_DIR/.git" ]; then
    echo "Repo found at $REPO_DIR"
    cd "$REPO_DIR"
    echo "Current: $(git log --oneline -1)"
    git fetch origin main
    git reset --hard origin/main
    echo "Updated to: $(git log --oneline -1)"
else
    echo "Cloning repo to $REPO_DIR..."
    git clone --depth 1 "$REPO_URL" "$REPO_DIR"
    cd "$REPO_DIR"
    echo "Cloned: $(git log --oneline -1)"
fi

echo ""
echo "=== bench-suite contents ==="
ls bench-suite/ 2>/dev/null || echo "No bench-suite directory"
echo ""
echo "=== Done ==="
