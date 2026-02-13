# Deploying Dagster with Pooled Postgres Storage

This directory contains Dockerfiles and configuration for deploying Dagster webserver and daemon with the custom pooled Postgres storage classes.

## Overview

The custom Docker images extend the official Dagster Helm chart images with the `ol-orchestrate-lib` package, enabling connection pooling for all Postgres storage components.

## Files

- **`Dockerfile.webserver`** - Custom webserver image with ol-orchestrate-lib
- **`Dockerfile.daemon`** - Custom daemon image with ol-orchestrate-lib
- **`build-images.sh`** - Helper script to build and push images
- **`dagster.yaml.pooled.example`** - Example configuration using pooled storage

## Quick Start

### 1. Build Images

From repository root:

```bash
# Build locally
./dg_deployments/build-images.sh

# Build and push to registry
./dg_deployments/build-images.sh --push --registry myregistry.io/dagster --tag v1.0.0
```

### 2. Configure Helm Chart

Create or update your Helm values file:

```yaml
# values.yaml

dagsterWebserver:
  image:
    repository: myregistry.io/dagster/webserver
    tag: v1.0.0
    pullPolicy: IfNotPresent

dagsterDaemon:
  image:
    repository: myregistry.io/dagster/daemon
    tag: v1.0.0
    pullPolicy: IfNotPresent

# Configure Postgres storage with pooling
runLauncher:
  type: K8sRunLauncher
  config:
    k8sRunLauncher:
      envConfigMaps:
        - dagster-postgres-config

# Dagster instance configuration
dagster:
  instance:
    config:
      run_storage:
        module: ol_orchestrate.lib.postgres
        class: PooledPostgresRunStorage
        config:
          postgres_db:
            username:
              env: DAGSTER_PG_USERNAME
            password:
              env: DAGSTER_PG_PASSWORD
            hostname:
              env: DAGSTER_PG_HOST
            db_name:
              env: DAGSTER_PG_DB
            port: 5432
          pool_size: 10
          max_overflow: 20
          pool_recycle: 3600

      event_log_storage:
        module: ol_orchestrate.lib.postgres
        class: PooledPostgresEventLogStorage
        config:
          postgres_db:
            username:
              env: DAGSTER_PG_USERNAME
            password:
              env: DAGSTER_PG_PASSWORD
            hostname:
              env: DAGSTER_PG_HOST
            db_name:
              env: DAGSTER_PG_DB
            port: 5432
          pool_size: 10
          max_overflow: 20
          pool_recycle: 3600

      schedule_storage:
        module: ol_orchestrate.lib.postgres
        class: PooledPostgresScheduleStorage
        config:
          postgres_db:
            username:
              env: DAGSTER_PG_USERNAME
            password:
              env: DAGSTER_PG_PASSWORD
            hostname:
              env: DAGSTER_PG_HOST
            db_name:
              env: DAGSTER_PG_DB
            port: 5432
          pool_size: 10
          max_overflow: 20
          pool_recycle: 3600
```

### 3. Deploy with Helm

```bash
helm repo add dagster https://dagster-io.github.io/helm
helm repo update

helm upgrade --install dagster dagster/dagster \
  --namespace dagster \
  --create-namespace \
  --values values.yaml
```

## Image Details

### Base Images

Both Dockerfiles extend `docker.io/dagster/dagster-k8s:1.12.14`, which is the default image used by the Dagster Helm chart for webserver and daemon components.

### What's Added

1. **uv package manager** - For fast dependency installation
2. **ol-orchestrate-lib** - Contains the pooled Postgres storage classes
3. **Verification** - Image build validates that pooled classes are importable

### Image Size

The additional layer adds approximately 10-20MB to the base Dagster image (primarily Python dependencies for ol-orchestrate-lib).

## Build Script Usage

### Basic Build

```bash
./dg_deployments/build-images.sh
```

Builds:
- `ol-data-platform/webserver:latest`
- `ol-data-platform/daemon:latest`

### Custom Registry and Tag

```bash
./dg_deployments/build-images.sh \
  --registry myregistry.io/dagster \
  --tag v1.2.3
```

Builds:
- `myregistry.io/dagster/webserver:v1.2.3`
- `myregistry.io/dagster/daemon:v1.2.3`

### Build and Push

```bash
./dg_deployments/build-images.sh \
  --push \
  --registry myregistry.io/dagster \
  --tag v1.2.3
```

Builds and pushes both images to the registry.

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Build Dagster Images

on:
  push:
    tags:
      - 'v*'

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Login to Registry
        uses: docker/login-action@v3
        with:
          registry: myregistry.io
          username: ${{ secrets.DOCKER_USERNAME }}
          password: ${{ secrets.DOCKER_PASSWORD }}

      - name: Build and Push Images
        run: |
          ./dg_deployments/build-images.sh \
            --push \
            --registry myregistry.io/dagster \
            --tag ${GITHUB_REF#refs/tags/}
```

## Connection Pool Sizing

For Kubernetes deployments, consider total connections:

```
Total connections = (pool_size + max_overflow) × 3 storage types × num_pods
```

**Example with 2 webserver pods + 1 daemon pod:**
- pool_size: 10, max_overflow: 20 per storage type
- Total: (10 + 20) × 3 × 3 = 270 max connections

Ensure Postgres `max_connections` setting accommodates this.

### Recommended Pod Settings

**Webserver** (multiple replicas for HA):
```yaml
dagsterWebserver:
  replicas: 2
```

**Daemon** (single replica):
```yaml
dagsterDaemon:
  replicas: 1
```

## Troubleshooting

### Import Error on Startup

**Error**: `ModuleNotFoundError: No module named 'ol_orchestrate'`

**Solution**: Ensure you're using the custom-built images, not the default Dagster images.

### Connection Pool Exhaustion

**Symptoms**: "Too many connections" errors in logs

**Solutions**:
1. Reduce `pool_size` or `max_overflow` in dagster.yaml
2. Increase Postgres `max_connections`
3. Scale down number of webserver/daemon pods

### Image Pull Errors

**Error**: `ImagePullBackOff` or `ErrImagePull`

**Solutions**:
1. Verify image exists in registry: `docker pull <image>`
2. Check imagePullSecrets are configured if using private registry
3. Verify image tag is correct in values.yaml

## Updating Dagster Version

To update to a newer Dagster version:

1. Update `FROM` line in both Dockerfiles:
   ```dockerfile
   FROM docker.io/dagster/dagster-k8s:1.13.0
   ```

2. Update `dagster~=1.12.0` in `dg_deployments/local/pyproject.toml`

3. Rebuild images:
   ```bash
   ./dg_deployments/build-images.sh --tag v1.13.0
   ```

## See Also

- **`../docs/POSTGRES_POOLING_README.md`** - Quick start guide
- **`../docs/POSTGRES_STORAGE_POOLING.md`** - Complete documentation
- **`dagster.yaml.pooled.example`** - Example configuration file
- [Dagster Helm Chart Documentation](https://docs.dagster.io/deployment/guides/kubernetes/deploying-with-helm)
