$Zone = "us-east1-b"
$VM = "velocity-classic"

gcloud compute ssh $VM --zone=$Zone --quiet --command "ls ~/V.E.L.O.C.I.T.Y.-WorkFlow/velocity-workflow-engine/ 2>/dev/null && echo FOUND || echo NOT_FOUND; head -5 ~/V.E.L.O.C.I.T.Y.-WorkFlow/Cargo.toml; echo ---; ls ~/V.E.L.O.C.I.T.Y.-WorkFlow/velocity-workflow-server/ 2>/dev/null || echo NO_SERVER_DIR" 2>&1
