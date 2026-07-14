# Deploying Argus to Azure

One command deploys the full distributed stack to **Azure Container Apps**:

```bash
az login
./deploy/azure/deploy.sh argus-rg eastus demo    # synthetic manipulation tape
./deploy/azure/deploy.sh argus-rg eastus live    # real Binance + OKX feeds
```

The script prints the public dashboard URL when done. Teardown:

```bash
az group delete --name argus-rg --yes --no-wait
```

## What gets created ([main.bicep](main.bicep))

| Resource | Purpose |
|---|---|
| Container Apps Environment + Log Analytics | Serverless container runtime; every service's stdout/stderr queryable in one workspace |
| Azure Container Registry (Basic) | App images, built **cloud-side** with `az acr build` (no local Docker needed) |
| User-assigned Managed Identity + `AcrPull` | Image pulls without passwords — ACR admin user stays disabled |
| Storage Account + Azure Files share | The tamper-evident audit trail, mounted read-write into the detector and read-only-in-practice into the API |
| `argus-nats` | NATS broker, internal TCP ingress on 4222 (never exposed publicly) |
| `argus-detector` | Detection engine + audit writer. `maxReplicas: 1` **on purpose** — the hash chain is single-writer; scale out with additional detector apps using disjoint `-subjects` filters and separate audit dirs |
| `argus-api` | Public HTTPS dashboard; autoscales 1→3 on HTTP concurrency (KEDA) |
| `argus-ml` | Isolation Forest anomaly scorer |
| `argus-replay` *or* `argus-ingest-binance` + `argus-ingest-okx` | Feed source, selected by `feedMode` |

## Two-phase deployment

The app images live in the ACR the template creates, so `deploy.sh` runs:

1. `main.bicep` with `deployApps=false` → registry, environment, storage, identity
2. `az acr build` for both images (built in Azure, tagged per deploy)
3. `main.bicep` with `deployApps=true` → the container apps

Both phases are the same template — idempotent, re-runnable, no drift.

## CI/CD

[`deploy-azure.yml`](../../.github/workflows/deploy-azure.yml) runs the same
script from GitHub Actions via **OIDC federated credentials** (no long-lived
cloud secrets in the repo). Trigger it from the Actions tab with a resource
group, region, and feed mode.

## Operational notes

- **Logs**: `az containerapp logs show -n argus-detector -g argus-rg --follow`
- **Audit verification from anywhere**: the dashboard's *Verify Chain* button, or
  `GET https://<dashboard>/api/audit/verify`
- **Metrics**: each Go service exposes Prometheus `/metrics`; wire into Azure
  Monitor managed Prometheus by adding a scrape config on the environment.
- **Azure Files + the hash chain**: SMB supports the append+fsync pattern the
  audit log uses; at very high alert rates move the chain to Premium Files or a
  managed disk. The verifier is transport-agnostic — it only reads the files.
- **Live mode egress**: Container Apps have outbound internet by default; some
  exchange endpoints geo-restrict certain cloud regions. If the live feed won't
  connect, redeploy in another region or use `demo` mode.
