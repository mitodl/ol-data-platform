# Contributing to `delivery`

The DELIVERY code location owns the **push** axis of MIT Learn integration:
assets that send catalog metadata to MIT Learn over its webhook API.

| Pattern | Used by |
|---|---|
| Webhook push (REST API) | Sloan Executive Education, MIT Climate, MIT PE, OLL, MIT edX programs |
| Sensor-driven partitioned | OVS Videos (one asset execution per item ID) |

The **pull** axes are not owned by a code location. Warehouse-pull reads
StarRocks over the MySQL protocol, and MV-serve is built and refreshed in
WAREHOUSE; both are dbt-built surfaces governed by the contract docs. Do not
add pull-side assets here.

Extraction assets that predate the split (`sloan_course_metadata`, `video_api`,
`video_metadata`) still live here and move on to INGEST later.

## Prerequisites

- Python 3.14 (managed by `uv`)
- Docker Desktop (for `dagster dev` with dependent services)
- Access to `mitodl` GitHub org (for Vault in production)
- AWS credentials for local S3 access (optional — see Vault mock section below)

## Local Development Setup

```bash
# From the ol-data-platform repo root
cd dg_projects/delivery

# Create the virtual environment and install dependencies
uv sync

# Start the Dagster UI (no Vault credentials required — see below)
uv run dagster dev
```

Open http://localhost:3000. You should see assets for Sloan, OVS Videos, and
Open Learning Library. All assets load even without real credentials because the code
location uses **resilient loading** — failed Vault auth falls back to a mock
instance, and assets gracefully skip actual API calls when credentials are absent.

### Running without Vault credentials

Set the following environment variables before `dagster dev` to use mock/local
values and avoid Vault auth failures:

```bash
export DAGSTER_ENVIRONMENT=dev
export VAULT_ADDR=http://localhost:8200   # not contacted unless authenticated
export SKIP_VAULT=1                        # prevents blocking on Vault auth
```

If you need to test actual API calls, authenticate to Vault with the OIDC
browser flow — no token to request from the platform team:

```bash
export VAULT_ADDR=https://vault-qa.odl.mit.edu
bin/vault-login
```

### Running tests

```bash
uv run pytest delivery_tests/ -v
```

---

## Code Location Structure

```
delivery/
├── delivery/
│   ├── assets/             # One file per data source
│   │   ├── sloan_api.py         # Reference implementation — read this first
│   │   ├── ovs_videos.py        # Sensor-driven partitioned asset
│   │   └── open_learning_library.py  # Open Learning Library REST asset
│   ├── lib/                # Shared utilities (not assets)
│   ├── sensors/            # Discovery and cleanup sensors
│   └── definitions.py      # Wires everything together
├── delivery_tests/
├── pyproject.toml
└── uv.lock
```

---

## Pattern 1: Adding a REST API Webhook Asset

Follow the Sloan asset (`assets/sloan_api.py`) as the canonical reference.

### 1. Create the asset file

```python
# delivery/assets/my_source.py
from dagster import AssetExecutionContext, AssetOut, multi_asset, Output
from ol_orchestrate.resources.api_client_factory import ApiClientFactory

@multi_asset(
    group_name="my_source",
    outs={
        "course_metadata": AssetOut(
            description="Courses from My Source",
            io_manager_key="s3file_io_manager",
        ),
    },
)
def my_source_metadata(context: AssetExecutionContext, my_source_api: ApiClientFactory):
    courses = my_source_api.client.get("/api/courses")
    context.log.info("Fetched %d courses", len(courses))
    yield Output(courses, output_name="course_metadata")
```

### 2. Register the asset and resource in `definitions.py`

```python
from delivery.assets.my_source import my_source_metadata
from ol_orchestrate.resources.api_client_factory import ApiClientFactory

defs = Definitions(
    assets=[..., my_source_metadata],
    resources={
        ...,
        "my_source_api": ApiClientFactory(
            deployment="my-source",
            client_class="MySourceApiClient",
            mount_point="secret-data",
            config_path="pipelines/my-source",
            kv_version="1",
            vault=vault,
        ),
    },
)
```

### 3. Add a schedule

```python
ScheduleDefinition(
    name="my_source_daily_schedule",
    target=AssetSelection.assets(my_source_metadata),
    cron_schedule="@daily",
    execution_timezone="Etc/UTC",
)
```

### 4. Vault credentials (production)

Add a Vault secret at `secret-data/pipelines/my-source` containing the API
credentials that `ApiClientFactory` expects. See the platform team runbook for
the key/value format per client class.

---

## Pattern 2: Adding a Sensor-Driven Partitioned Asset

OVS Videos and Video Shorts use a **discovery sensor + partitioned asset** pattern
for processing one item (video) per partition. See `assets/ovs_videos.py` for the
full reference.

Key elements:

```python
# 1. Define a DynamicPartitionsDefinition — one entry per item ID
my_item_ids = DynamicPartitionsDefinition(name="my_item_ids")

# 2. Discovery sensor: watches for new items, adds partitions
@sensor(job=my_item_discovery_job)
def my_item_discovery_sensor(context: SensorEvaluationContext):
    ...
    return SensorResult(
        run_requests=[RunRequest(partition_key=item_id) for item_id in new_ids],
        dynamic_partitions_requests=[
            my_item_ids.build_add_request(new_ids),
        ],
    )

# 3. Partitioned asset: processes one item
@asset(partitions_def=my_item_ids)
def my_item_metadata(context: AssetExecutionContext, ...):
    item_id = context.partition_key
    ...
```

---

## Pattern 3: Adding a Trino-Pull Asset (new — OCW/OpenEdX migration)

For large, batch-oriented sources that are materialized as Iceberg tables via
dbt models, MIT Learn reads the StarRocks surface directly over the MySQL
protocol rather than having Dagster push it. That is the warehouse-pull axis and
it has no assets in this location. See `docs/learn_marts_contract.md`.

A placeholder component stub exists at `delivery/defs/` for future `dg`-component
based delivery assets.

---

## Adding a dlt Pipeline

If your source is better served by a dlt pipeline (e.g., RSS feeds, structured
file dumps, or sources with a dlt connector), add it to the `data_loading` code
location instead. See `dg_projects/data_loading/` and its `README`.

Use this code location (`delivery`) only for assets that push MIT Learn catalog
data (courses, programs, content files) over the webhook API.

---

## Conventions

| Convention | Example |
|---|---|
| Asset group name matches source | `group_name="sloan_executive_education"` |
| `io_manager_key` for large files | `"s3file_io_manager"` |
| `io_manager_key` for media/binary | `"yt_s3file_io_manager"` |
| Sensor names | `<source>_discovery_sensor`, `<source>_stale_cleanup_sensor` |
| Schedule names | `<source>_daily_schedule`, `<source>_api_schedule` |
| Asset file name | One file per source, snake_case matching the source name |

---

## Deployment

This code location deploys as a standalone Docker container (gRPC server on
port 4004 in the local workspace). The `Dockerfile` is at the root of this
directory and is built by the shared `build.yaml` pipeline.

For production deployments, see the platform team's Helm values in
`ol-infrastructure/src/ol_infrastructure/applications/dagster/`.
