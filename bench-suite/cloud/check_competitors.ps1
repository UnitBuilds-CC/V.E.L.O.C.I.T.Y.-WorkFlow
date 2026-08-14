$Zone = "us-east1-b"

Write-Host "=== Checking Temporal VM ===" -ForegroundColor Cyan
gcloud compute ssh temporal-bench --zone=$Zone --quiet --command "docker ps 2>/dev/null; echo SEPARATOR; ls ~/temporal-production/ 2>/dev/null || echo NO_PROD_DIR; echo SEPARATOR; curl -s --max-time 3 http://localhost:8080/health 2>/dev/null || echo NO_SERVICE_8080; echo SEPARATOR; python3 -c 'import socket; s=socket.socket(); s.settimeout(2); s.connect((chr(108)+chr(111)+chr(99)+chr(97)+chr(108)+chr(104)+chr(111)+chr(115)+chr(116),7233)); print(chr(80)+chr(79)+chr(82)+chr(84)+chr(95)+chr(79)+chr(75)); s.close()' 2>/dev/null || echo PORT_7233_CLOSED" 2>&1

Write-Host "`n=== Checking DBOS VM ===" -ForegroundColor Cyan
gcloud compute ssh dbos-bench --zone=$Zone --quiet --command "ls ~/dbos-production/ 2>/dev/null || echo NO_PROD_DIR; echo SEPARATOR; systemctl is-active postgresql 2>/dev/null || echo PG_NOT_RUNNING; echo SEPARATOR; curl -s --max-time 3 http://localhost:8080/health 2>/dev/null || echo NO_SERVICE_8080" 2>&1

Write-Host "`n=== Checking Restate VM ===" -ForegroundColor Cyan
gcloud compute ssh restate-bench --zone=$Zone --quiet --command "docker ps 2>/dev/null; echo SEPARATOR; ls ~/restate-production/ 2>/dev/null || echo NO_PROD_DIR; echo SEPARATOR; curl -s --max-time 3 http://localhost:9080/restate/discover 2>/dev/null || echo NO_SERVICE_9080; echo SEPARATOR; curl -s --max-time 3 http://localhost:8082 2>/dev/null || echo NO_INGRESS_8082" 2>&1

Write-Host "`n=== All checks done ===" -ForegroundColor Green
