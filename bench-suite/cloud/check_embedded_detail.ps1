$Zone = "us-east1-b"
$VM = "velocity-embedded"

# Check build output and process tree
gcloud compute ssh $VM --zone=$Zone --quiet --command "echo '=== Process tree ==='; ps auxf | grep -E 'cargo|rustc' | grep -v grep; echo; echo '=== Disk activity ==='; ls -la ~/V.E.L.O.C.I.T.Y.-WorkFlow/target/release/build/ 2>/dev/null | tail -5; echo; echo '=== Cargo lock ==='; ls -la ~/V.E.L.O.C.I.T.Y.-WorkFlow/target/release/.cargo-lock 2>/dev/null; echo; echo '=== Recent target activity ==='; find ~/V.E.L.O.C.I.T.Y.-WorkFlow/target/ -newer /tmp/deploy.sh -type f 2>/dev/null | head -10 || echo NO_RECENT_FILES" 2>&1
