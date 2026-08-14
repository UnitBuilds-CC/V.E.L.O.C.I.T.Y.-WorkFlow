$Zone = "us-east1-b"

# Simple CI check via curl on VM
gcloud compute ssh velocity-classic --zone=$Zone --quiet --command "curl -s 'https://api.github.com/repos/UnitBuilds/Velocity-workflow/actions/runs?per_page=5' | python3 -c 'import sys,json; data=json.load(sys.stdin); [print(r[\"name\"],r[\"status\"],r.get(\"conclusion\",\"N/A\"),r[\"created_at\"]) for r in data.get(\"workflow_runs\",[])[:5]]' 2>/dev/null || echo CURL_FAILED" 2>&1
