# Velocity — Quick Start Guide

Test all 3 flavors (Classic, Runtime, Embedded) on a live system.

## Option 1: Local Dev Server (Fastest, 0 config)

**Time: 2 minutes. Cost: Free.**

```powershell
# Start the in-memory dev server
cargo run --bin velocity-dev -- --port 7233 --grpc-port 7234 --ui-port 8233
```

Then test from another terminal:

```powershell
# Classic (Temporal-compatible)
cd velocity-classic-ts
npm test

# Runtime (Restate-compatible)
cd velocity-runtime-typescript
npm test

# Embedded (DBOS-compatible)
cd velocity-embedded-ts
npm test
```

Or run all tests at once:
```powershell
.\test-all-flavors.ps1
```

**Endpoints:**
- HTTP API: http://localhost:7233
- gRPC: http://localhost:7234
- Web UI: http://localhost:8233

---

## Option 2: Docker Compose (Full stack, local)

**Time: 5 minutes. Cost: Free.**

```powershell
# Build and start everything
docker compose up -d --build
```

**Services:**
| Service | Port | Description |
|---------|------|-------------|
| Velocity HTTP | 5000 | Classic REST API |
| Velocity gRPC | 50051 | Classic gRPC API |
| PostgreSQL | 5432 | Persistence |
| Prometheus | 9090 | Metrics |
| Grafana | 3000 | Dashboards (admin/admin) |
| Web UI | 8080 | Workflow visualizer |

**Stop:**
```powershell
docker compose down
```

---

## Option 3: Google Cloud GCE (Live cloud testing)

**Time: 10 minutes. Cost: ~$0.15/hour.**

### Prerequisites

1. Install gcloud CLI: https://cloud.google.com/sdk/docs/install
2. Authenticate:
   ```powershell
   gcloud auth login
   gcloud projects create velocity-test --name="Velocity Test"
   ```

### Deploy

```powershell
.\deploy-to-gce.ps1
```

Or manually:

```powershell
# 1. Create VM
gcloud compute instances create velocity-test `
  --zone=us-central1-a `
  --machine-type=e2-standard-4 `
  --image-family=ubuntu-2404-lts-amd64 `
  --image-project=ubuntu-os-cloud `
  --boot-disk-size=50GB `
  --tags=http-server

# 2. Open ports
gcloud compute firewall-rules create velocity-ports `
  --allow=tcp:5000,tcp:50051,tcp:3000,tcp:9090,tcp:8080

# 3. SSH in
gcloud compute ssh velocity-test --zone=us-central1-a

# 4. On the VM: Install Docker
sudo apt update && sudo apt install -y docker.io docker-compose-v2 git
sudo usermod -aG docker $USER && newgrp docker

# 5. Clone and deploy
git clone <your-repo-url> velocity-workflow
cd velocity-workflow
docker compose up -d --build
```

**Get the external IP:**
```powershell
gcloud compute instances describe velocity-test `
  --zone=us-central1-a `
  --format="value(networkInterfaces[0].accessConfigs[0].natIP)"
```

**Test all 3 flavors from your local machine:**

```powershell
$IP = "<VM_EXTERNAL_IP>"

# Classic
$env:VELOCITY_URL = "http://$IP`:5000"
cd velocity-classic-ts && npm test

# Runtime
$env:VELOCITY_ENGINE_URL = "http://$IP`:5000"
cd velocity-runtime-typescript && npm test

# Embedded
$env:DATABASE_URL = "postgres://velocity:velocity_secret@$IP`:5432/velocity"
cd velocity-embedded-ts && npm test
```

**Cleanup:**
```powershell
gcloud compute instances stop velocity-test --zone=us-central1-a
# Or delete:
gcloud compute instances delete velocity-test --zone=us-central1-a
```

---

## Option 4: Google Cloud GKE (Production-realistic)

**Time: 20 minutes. Cost: ~$0.50/hour.**

```powershell
# Create GKE cluster
gcloud container clusters create velocity-test `
  --zone=us-central1-a `
  --num-nodes=3 `
  --machine-type=e2-standard-4

# Deploy with Helm
helm install velocity ./deploy/helm/velocity --wait

# Check status
kubectl get pods
helm test velocity

# Get external IP
kubectl get service velocity-server -o jsonpath='{.status.loadBalancer.ingress[0].ip}'

# Cleanup
gcloud container clusters delete velocity-test --zone=us-central1-a
```

---

## Testing Each Flavor

### Classic (Temporal-compatible)

```typescript
import { Worker, Workflow, Activity } from 'velocity-classic-ts';

class MyWorkflow extends Workflow {
  async execute(input: { name: string }) {
    const greeting = await this.ctx.activity('greet', input);
    return greeting;
  }
}

const worker = await Worker.create({
  taskQueue: 'test-queue',
  workflowsPath: __dirname,
});
```

### Runtime (Restate-compatible)

```typescript
import { VirtualObject, Context, createApp } from 'velocity-runtime-typescript';

const cart = new VirtualObject('ShoppingCart')
  .addHandler('addItem', async (ctx: Context, item: string) => {
    const items = await ctx.get('items') || [];
    items.push(item);
    await ctx.set('items', items);
    return items;
  });

const app = createApp([cart]);
```

### Embedded (DBOS-compatible)

```typescript
import { Durable, Transaction, DurableContext, VelocityEmbedded } from 'velocity-embedded-ts';

@Durable()
class OrderWorkflow {
  @Transaction()
  async process(ctx: DurableContext, orderId: string) {
    const charge = await ctx.run('charge', () => chargeCard(orderId));
    const ship = await ctx.run('ship', () => shipOrder(orderId));
    return { charge, ship };
  }
}

const engine = new VelocityEmbedded({ databaseUrl: 'postgres://...' });
```

---

## Comparison Matrix

| Feature | Local Dev Server | Docker Compose | GCE VM | GKE Cluster |
|---------|:---:|:---:|:---:|:---:|
| Setup time | 2 min | 5 min | 10 min | 20 min |
| Cost | Free | Free | ~$0.15/hr | ~$0.50/hr |
| Persistence | In-memory | Postgres | Postgres | Postgres (HA) |
| Monitoring | None | Prometheus+Grafana | Prometheus+Grafana | Full stack |
| Multi-region | No | No | No | Yes |
| Production-realistic | No | No | Yes | Yes |
| All 3 flavors | Yes | Yes | Yes | Yes |

---

## Recommended Path

1. **Start local:** `cargo run --bin velocity-dev` — verify everything works
2. **Docker Compose:** `docker compose up -d` — test with Postgres persistence
3. **GCE VM:** `.\deploy-to-gce.ps1` — test on real cloud infrastructure
4. **GKE (optional):** For production-realistic Kubernetes deployment
