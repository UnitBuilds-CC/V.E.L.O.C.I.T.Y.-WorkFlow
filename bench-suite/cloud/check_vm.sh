#!/bin/bash
# Quick check: find repo directory on VM
gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command="find /home -maxdepth 3 -name '.git' -type d 2>/dev/null; ls -la ~/V.E.L.O.C.I.T.Y.-WorkFlow/.git 2>/dev/null || echo 'not at ~/V.E.L.O.C.I.T.Y.-WorkFlow'; ls -la ~/Velocity-workflow/.git 2>/dev/null || echo 'not at ~/Velocity-workflow'"
