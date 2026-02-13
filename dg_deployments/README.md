# Dagster Deployment with Pooled Postgres Storage

Quick reference for deploying Dagster with custom pooled Postgres storage classes.

## Quick Start

### 1. Build Images

```bash
./dg_deployments/build-images.sh
```

### 2. Deploy with Helm

```bash
helm repo add dagster https://dagster-io.github.io/helm
helm repo update

helm upgrade --install dagster dagster/dagster \
  --namespace dagster \
  --create-namespace \
  --values dg_deployments/values.pooled.yaml
```

### 3. Verify Deployment

```bash
# Check pods are running
kubectl get pods -n dagster

# Check webserver logs for pooled storage
kubectl logs -n dagster -l component=dagster-webserver | grep "Pooled"

# Port-forward to access UI
kubectl port-forward -n dagster svc/dagster-webserver 8080:80
```

Open http://localhost:8080

## Files

| File | Purpose |
|------|---------|
| `Dockerfile.webserver` | Custom webserver image with ol-orchestrate-lib |
| `Dockerfile.daemon` | Custom daemon image with ol-orchestrate-lib |
| `build-images.sh` | Helper script to build and push images |
| `values.pooled.yaml` | Helm values with pooled storage configuration |
| `DEPLOYMENT.md` | Complete deployment documentation |

## Images

Built images extend `docker.io/dagster/dagster-k8s:1.12.14` with `ol-orchestrate-lib`.

**Default names:**
- `ol-data-platform/webserver:latest`
- `ol-data-platform/daemon:latest`

## Configuration

Pooled storage classes are configured in Helm values:

```yaml
dagsterInstanceConfig:
  run_storage:
    module: ol_orchestrate.lib.postgres
    class: PooledPostgresRunStorage
    config:
      postgres_db:
        username: {env: DAGSTER_PG_USERNAME}
        password: {env: DAGSTER_PG_PASSWORD}
        hostname: {env: DAGSTER_PG_HOST}
        db_name: {env: DAGSTER_PG_DB}
        port: 5432
      pool_size: 10
      max_overflow: 20
      pool_recycle: 3600
```

## Connection Limits

With default settings (2 webserver + 1 daemon pod):
- Max connections: (10 + 20) × 3 × 3 = **270 connections**
- Ensure Postgres `max_connections` > 270

## See Also

- **`DEPLOYMENT.md`** - Complete deployment guide
- **`../docs/POSTGRES_POOLING_README.md`** - Pooling overview
- **`../docs/POSTGRES_STORAGE_POOLING.md`** - Full documentation
