# Data contracts

Each file here is the OpenMetadata data contract for one asset: the columns and
types consumers can rely on, the metadata rules the asset must satisfy (e.g. it
has an owner), and its refresh SLA. The files are the source of truth, and
OpenMetadata is made to match them.

## Format

```yaml
entity:
  type: table
  dbt_model: dim_user        # or dbt_source: <source_name>.<table_name>, or fqn: <OpenMetadata FQN>
contract:                    # an OpenMetadata CreateDataContract body, minus `entity`
  name: dim_user
  description: ...
  entityStatus: Approved
  owners: [{type: team, name: data-platform}]   # optional; resolved to ids on sync
  schema: [{name: user_pk, dataType: VARCHAR, constraint: PRIMARY_KEY}, ...]
  semantics: [{name: hasOwner, description: ..., rule: '<JsonLogic>', enabled: true}]
  sla: {refreshFrequency: {interval: 1, unit: day}, maxLatency: {value: 2, unit: hour}}
```

`contract` uses OpenMetadata's native shape rather than ODCS. OpenMetadata's
ODCS importer has no `semantics` field, and a `mode=replace` import clears them.
`schema` lists only the columns being promised, not every column. For a table
or dataset behind Superset dashboards, those are the columns the dashboards'
charts and native filters reference in `src/ol_superset/assets`. `dataType`
takes OpenMetadata type names (`VARCHAR`, `BIGINT`, `TIMESTAMP`, ...).

The `entity` binding says which asset the contract is for:

- `dbt_model` / `dbt_source`: a table built or read by dbt. Sync finds its
  OpenMetadata table from the relation in a production manifest, so the file
  names no environment. These are the only contracts CI can check.
- `fqn`: anything else OpenMetadata catalogs, e.g. a Superset dataset
  (`type: dashboardDataModel`, `fqn: Superset.model.41`) or a raw table dbt
  doesn't declare. Supported types are table, dashboardDataModel, topic,
  apiEndpoint and container, but only table and dashboardDataModel contracts
  may list a `schema`: OpenMetadata 2.0.5 checks column types for those two
  only (topics and API endpoints by name, containers not at all). Contracts
  on the other types carry semantics rules only.

## What is enforced where

| Part | Checked by | When |
| --- | --- | --- |
| `schema`, dbt bindings | `ol-dbt validate --only data_contract` (dbt PR CI) | every PR, before merge |
| `schema` columns, all bindings | OpenMetadata, against its catalog | after ingestion, on `ol-dbt contracts validate` or OpenMetadata's daily run |
| `schema` types, all bindings | `ol-dbt contracts sync` / `validate` only | same as above |
| `semantics` | OpenMetadata | same as above |
| `sla` shape | `ol-dbt validate --only data_contract` (dbt PR CI) | every PR, before merge |
| `sla`, `dbt_model` bindings | the asset's Dagster freshness check (lakehouse code location) | hourly, by `dbt_layer_freshness_sensor` |

OpenMetadata 2.0.5 reports a retyped column in `typeMismatchFields` but doesn't
count it as a failure, so the contract still shows Success in OpenMetadata
(`DataContractRepository.validateSchemaFieldsAgainstEntity`). The `ol-dbt
contracts` commands fail on it; OpenMetadata's own status and alerts won't.

OpenMetadata 2.0.5 stores `sla` but never evaluates it, so for a `dbt_model`
contract the lakehouse code location builds the model's Dagster freshness check
from it: the check fails once the table's last update is older than
`refreshFrequency` plus `maxLatency` (a month counts as 31 days, a year as 366).
That check replaces the 26-hour default the dimensional, mart and reporting
groups otherwise get, and a contracted model in any other group gets one too.
A `dbt_model` contract that sets `maxLatency` must also set `refreshFrequency`,
since the latency is grace on it. `sla` on `dbt_source` and `fqn` contracts is
recorded in OpenMetadata only, so either field may stand alone there.

The CI check fails when a contracted column is missing from the model or source
YAML, is no longer selected by the model SQL, has no `data_type`, or has a type
outside the contract type's family. It uses the same families OpenMetadata
uses, so `VARCHAR` → `STRING` passes and `VARCHAR` → `BIGINT` fails. Changing a
contracted column therefore means changing its contract in the same PR, where
the reviewer sees it.

Quality stays in dbt tests and Dagster asset checks, and contracts leave
`qualityExpectations` out. OpenMetadata 2.0.5 could carry the dbt and Dagster
test cases by reference: the contract's test-suite pipeline skips test cases
from other platforms, and on completion the contract takes each referenced
case's latest stored result. But the contract's quality would only update when
a validation runs (OpenMetadata's daily run or `ol-dbt contracts validate`),
those results have no age limit, each validation launches the contract's
test-suite ingestion pipeline as a pod, and `ol-dbt contracts validate` would
have to poll past the Running status that leaves the contract in. Dagster
already posts these failures to Slack as they happen (the run failure sensor
for dbt tests, `asset_check_failure_sensor` for freshness and native asset
checks).

## Commands

```bash
# Credential-free, what CI runs (needs target/manifest.json from `dbt parse`)
ol-dbt validate --only data_contract

# Needs OM_SERVER_URL (e.g. https://data.ol.mit.edu/api) and OM_BOT_JWT_TOKEN
ol-dbt contracts sync --service "Starburst Galaxy" --manifest <production manifest.json> --dry-run
ol-dbt contracts sync --service "Starburst Galaxy" --manifest <production manifest.json>
ol-dbt contracts validate --service "Starburst Galaxy" --manifest <production manifest.json>
```

`--service` is the OpenMetadata database service the warehouse tables are
catalogued under. It is a flag because the StarRocks migration will change it.
