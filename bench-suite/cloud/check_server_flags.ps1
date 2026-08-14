$Zone = "us-east1-b"

# Check velocity-server help on classic VM (already built there)
gcloud compute ssh velocity-classic --zone=$Zone --quiet --command "~/V.E.L.O.C.I.T.Y.-WorkFlow/target/release/velocity-server --help 2>&1" 2>&1
