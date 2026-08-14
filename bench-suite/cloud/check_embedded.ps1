$Zone = "us-east1-b"
$VM = "velocity-embedded"

gcloud compute ssh $VM --zone=$Zone --quiet --command "ls -lh ~/V.E.L.O.C.I.T.Y.-WorkFlow/target/release/velocity-server 2>/dev/null || echo SERVER_NOT_BUILT; ls -lh ~/V.E.L.O.C.I.T.Y.-WorkFlow/target/release/velocity-bench 2>/dev/null || echo BENCH_NOT_BUILT; ps aux | grep -E 'cargo|rustc' | grep -v grep | wc -l" 2>&1
