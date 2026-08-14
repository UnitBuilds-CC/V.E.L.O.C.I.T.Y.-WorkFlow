gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command "ls -d ~/V* 2>/dev/null; git -C ~/V.E.L.O.C.I.T.Y.-WorkFlow log --oneline -1 2>/dev/null || echo REPO_NOT_FOUND"
