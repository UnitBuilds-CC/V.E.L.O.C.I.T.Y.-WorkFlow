$gcloud = 'C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
$cmd = @'
echo "Waiting for Temporal..."
for i in $(seq 1 30); do
  status=$(sudo docker inspect --format='{{.State.Health.Status}}' velocity-bench-temporal 2>/dev/null)
  echo "Attempt $i: $status"
  if [ "$status" = "healthy" ]; then
    echo "Temporal is healthy!"
    break
  fi
  sleep 5
done
'@
& $gcloud compute ssh velocity-classic --zone=us-east1-b --quiet --command=$cmd
