$Zone = "us-east1-b"

# Check velocity-bench help
gcloud compute ssh velocity-classic --zone=$Zone --quiet --command "~/V.E.L.O.C.I.T.Y.-WorkFlow/target/release/velocity-bench --help 2>&1" 2>&1
