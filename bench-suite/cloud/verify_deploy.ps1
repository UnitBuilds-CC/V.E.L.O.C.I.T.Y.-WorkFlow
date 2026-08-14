$Zone = "us-east1-b"
$VM = "velocity-classic"

# Verify correct extraction
gcloud compute ssh $VM --zone=$Zone --quiet --command "echo '=== Cargo.toml head ==='; head -5 ~/V.E.L.O.C.I.T.Y.-WorkFlow/Cargo.toml; echo; echo '=== velocity-workflow-engine exists? ==='; ls ~/V.E.L.O.C.I.T.Y.-WorkFlow/velocity-workflow-engine/ 2>/dev/null && echo FOUND || echo MISSING; echo; echo '=== velocity-workflow-server exists? ==='; ls ~/V.E.L.O.C.I.T.Y.-WorkFlow/velocity-workflow-server/ 2>/dev/null && echo FOUND || echo MISSING; echo; echo '=== Total dirs ==='; ls ~/V.E.L.O.C.I.T.Y.-WorkFlow/ | head -20" 2>&1
