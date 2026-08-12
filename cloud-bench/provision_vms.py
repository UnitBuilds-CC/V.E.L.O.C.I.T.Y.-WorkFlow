"""Create benchmark VMs via GCP Compute Engine REST API."""
import json
import subprocess
import urllib.request
import urllib.error
import ssl
import time

PROJECT = "velocity-live-test-001"
ZONE = "us-east1-b"
MACHINE_TYPE = "e2-standard-4"
IMAGE_FAMILY = "ubuntu-2204-lts"
IMAGE_PROJECT = "ubuntu-os-cloud"
DISK_SIZE_GB = 50
TAGS = ["velocity-bench"]

VMS_TO_CREATE = [
    "velocity-runtime",
    "velocity-embedded",
    "temporal-bench",
    "restate-bench",
    "dbos-bench",
]

def get_token():
    r = subprocess.run(
        [r"C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd",
         "auth", "print-access-token"],
        capture_output=True, text=True
    )
    return r.stdout.strip()

def get_existing_vms():
    token = get_token()
    url = f"https://compute.googleapis.com/compute/v1/projects/{PROJECT}/zones/{ZONE}/instances"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, context=ctx) as resp:
            data = json.loads(resp.read())
            return {item["name"] for item in data.get("items", [])}
    except urllib.error.HTTPError as e:
        print(f"Error listing VMs: {e.code} {e.read().decode()}")
        return set()

def create_vm(name, token):
    url = f"https://compute.googleapis.com/compute/v1/projects/{PROJECT}/zones/{ZONE}/instances"
    body = {
        "name": name,
        "machineType": f"zones/{ZONE}/machineTypes/{MACHINE_TYPE}",
        "disks": [{
            "boot": True,
            "autoDelete": True,
            "initializeParams": {
                "sourceImage": f"projects/{IMAGE_PROJECT}/global/images/family/{IMAGE_FAMILY}",
                "diskSizeGb": str(DISK_SIZE_GB),
                "diskType": f"zones/{ZONE}/diskTypes/pd-ssd",
            }
        }],
        "networkInterfaces": [{
            "network": "global/networks/default",
            "accessConfigs": [{"type": "ONE_TO_ONE_NAT", "name": "External NAT"}]
        }],
        "tags": {"items": TAGS},
        "labels": {"flavor": name},
    }
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method="POST", headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    })
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, context=ctx) as resp:
            result = json.loads(resp.read())
            print(f"  Creating {name}... operation: {result.get('status', 'unknown')}")
            return True
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        if "alreadyExists" in err:
            print(f"  {name} already exists, skipping")
            return True
        print(f"  Error creating {name}: {e.code} {err[:200]}")
        return False

def wait_for_operations(token, names):
    """Poll until all VMs are RUNNING."""
    for attempt in range(60):
        all_running = True
        for name in names:
            url = f"https://compute.googleapis.com/compute/v1/projects/{PROJECT}/zones/{ZONE}/instances/{name}"
            req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
            ctx = ssl.create_default_context()
            try:
                with urllib.request.urlopen(req, context=ctx) as resp:
                    data = json.loads(resp.read())
                    status = data.get("status", "UNKNOWN")
                    if status != "RUNNING":
                        all_running = False
                        print(f"  {name}: {status}")
            except:
                all_running = False
                print(f"  {name}: not found yet")
        if all_running:
            print("All VMs are RUNNING!")
            return True
        time.sleep(10)
        token = get_token()  # refresh token
    print("Timeout waiting for VMs")
    return False

def get_vm_ips(token, names):
    """Get external IPs for all VMs."""
    ips = {}
    for name in names:
        url = f"https://compute.googleapis.com/compute/v1/projects/{PROJECT}/zones/{ZONE}/instances/{name}"
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        ctx = ssl.create_default_context()
        try:
            with urllib.request.urlopen(req, context=ctx) as resp:
                data = json.loads(resp.read())
                nics = data.get("networkInterfaces", [])
                if nics:
                    access = nics[0].get("accessConfigs", [])
                    if access:
                        ips[name] = access[0].get("natIP", "")
        except Exception as e:
            print(f"  Error getting IP for {name}: {e}")
    return ips

if __name__ == "__main__":
    print("=== VELOCITY 3-Flavor Cloud Benchmark VM Provisioning ===")
    print(f"Project: {PROJECT}, Zone: {ZONE}")
    
    # Check existing VMs
    existing = get_existing_vms()
    print(f"Existing VMs: {existing}")
    
    to_create = [vm for vm in VMS_TO_CREATE if vm not in existing]
    if not to_create:
        print("All VMs already exist!")
    else:
        print(f"Creating {len(to_create)} VMs: {to_create}")
        token = get_token()
        for vm in to_create:
            create_vm(vm, token)
            time.sleep(2)  # rate limit
    
    # Wait for all to be running
    all_vms = VMS_TO_CREATE + ["velocity-classic"]
    print("\nWaiting for all VMs to be RUNNING...")
    token = get_token()
    wait_for_operations(token, all_vms)
    
    # Get IPs
    print("\nVM External IPs:")
    token = get_token()
    ips = get_vm_ips(token, all_vms)
    for name, ip in sorted(ips.items()):
        print(f"  {name}: {ip}")
    
    # Save IPs for later use
    with open("cloud-bench/vm_ips.json", "w") as f:
        json.dump(ips, f, indent=2)
    print(f"\nIPs saved to cloud-bench/vm_ips.json")
