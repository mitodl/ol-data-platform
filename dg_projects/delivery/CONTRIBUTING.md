# Contributing to `delivery`

The DELIVERY code location owns the **push** axis of MIT Learn integration:
assets that send catalog metadata to MIT Learn over its webhook API.

| Pattern | Used by |
|---|---|
| Webhook push (REST API) | MIT Climate, MIT PE, OLL, MIT edX programs |
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
export VAULT_OIDC_NONINTERACTIVE=1         # fail on a missing token, do not open a browser
```

If you need to test actual API calls, authenticate to Vault with the OIDC
browser flow — no token to request from the platform team:

```bash
export VAULT_ADDR=https://vault-qa.odl.mit.edu
bin/vault-login
```

### Delivering to a local MIT Learn

A webhook asset can run from your laptop against a MIT Learn that also runs on
your laptop, with no Vault token, VPN or AWS credentials. Two things stand in
for the deployed inputs, and both apply only when `DAGSTER_ENVIRONMENT` is `dev`
(the default):

- `MIT_LEARN_BASE_URL` and `MIT_LEARN_WEBHOOK_SECRET`, set together, configure
  `learn_api` without reading Vault. With only one of them set the secret is
  still read from Vault and that one value overrides it.
- `DBT_MODEL_FIXTURE_DIR` makes `get_dbt_model_as_dataframe` read
  `<dir>/<table_name>.jsonl` (one JSON row per line) and not the Glue table.
  `fixtures/` holds a canned batch for `mit_climate_webhook`. To run another
  asset, add a file named for the integrations table it reads.

```bash
cd dg_projects/delivery
export VAULT_OIDC_NONINTERACTIVE=1
export DBT_MODEL_FIXTURE_DIR="$PWD/fixtures"

# MIT Learn from ol-infrastructure/local-dev (k3d + Tilt). The secret is the
# WEBHOOK_SECRET in local-dev/apps/mit-learn/secrets.yaml.
export MIT_LEARN_BASE_URL=https://api.learn.mit.dev
export MIT_LEARN_WEBHOOK_SECRET=local-dev-mitlearn-insecure-webhook-secret  # pragma: allowlist secret

uv run dagster asset materialize -m delivery.definitions \
  --select "mit_learn_delivery/mit_climate_webhook"
```

For a MIT Learn started with its own `docker compose`, the base URL is
`http://localhost:8061`, the secret is the `WEBHOOK_SECRET` in its
`env/backend.env`. The same variables work with `uv run dagster dev`, where the
asset is materialized from the UI.

The HTTP client verifies TLS against the OS trust store, which is where
local-dev's `setup.sh` installs the mkcert root, so the local-dev certificate
needs no extra configuration.

MIT Learn answers `409` until the webhook owns the batch's
`(etl_source, resource_type)` pairs. A local database has no ownership rows, so
create one per pair first, in Django admin under "ETL source ownerships" or from
a shell in the MIT Learn web container:

```bash
python manage.py shell -c "
from learning_resources.models import ETLSourceOwnership
ETLSourceOwnership.objects.update_or_create(
    etl_source='mit_climate', resource_type='document',
    defaults={'owner': ETLSourceOwnership.Pipeline.WEBHOOK},
)"
```

Then confirm the delivery:

```bash
curl -s "$MIT_LEARN_BASE_URL/api/v1/learning_resources/?resource_type=document&platform=climate" \
  | jq '.results[] | {readable_id, title}'
```

A batch is a full sync. MIT Learn unpublishes every resource of the same
`etl_source` and `resource_type` that the batch leaves out, so a canned batch
should be for a source nothing else in your local database loads. Delivering
two rows of `mitxonline` courses would unpublish every other MITx Online course
you have.

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

### 3. Trigger it

An asset that fetches from an external API runs on a schedule:

```python
ScheduleDefinition(
    name="my_source_daily_schedule",
    target=AssetSelection.assets(my_source_metadata),
    cron_schedule="@daily",
    execution_timezone="Etc/UTC",
)
```

An asset that reads an `integrations__learn__*` model and POSTs it to MIT Learn
must not. A cron tick fires whether or not the model has been rebuilt that day,
and a full-sync batch built from stale data unpublishes whatever changed
upstream since. Declare its deps with the key lakehouse emits,
`AssetKey(["integrations", "<model name>"])`, and wrap it with
`deliver_after_upstream`, which runs it once a day after every dep has
materialized since the tick of the lakehouse job that stages the source's
inputs (`NON_AIRBYTE_STAGING_CRON` for dlt/Dagster-loaded sources; the
latest tick when inputs come from more than one job). Too late a tick and the
delivery never fires, because the models were rebuilt before it. Too early and
it fires after the first job's rebuild, on the other inputs' old data:

```python
my_source_webhook, my_source_delivery_sensor = deliver_after_upstream(
    my_source_webhook,
    "my_source_delivery_sensor",
    staging_cron=NON_AIRBYTE_STAGING_CRON,
)
```

Register the sensor in `Definitions(sensors=instigators_for_environment([...]))`
and give it an entry in `INSTIGATOR_ENVIRONMENTS`.

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
| Sensor names | `<source>_discovery_sensor`, `<source>_stale_cleanup_sensor`, `<source>_delivery_sensor` |
| Schedule names | `<source>_daily_schedule`, `<source>_api_schedule` |
| Asset file name | One file per source, snake_case matching the source name |

---

## Deployment

This code location deploys as a standalone Docker container (gRPC server on
port 4004 in the local workspace). The `Dockerfile` is at the root of this
directory and is built by the shared `build.yaml` pipeline.

For production deployments, see the platform team's Helm values in
`ol-infrastructure/src/ol_infrastructure/applications/dagster/`.
