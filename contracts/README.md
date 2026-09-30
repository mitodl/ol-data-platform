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
  owners: [{type: team, name: data-engineering}]   # optional; resolved to ids on sync
  schema: [{name: user_pk, dataType: VARCHAR, constraint: PRIMARY_KEY}, ...]
  semantics: [{name: hasOwner, description: ..., rule: '<JsonLogic>', enabled: true}]
  sla: {refreshFrequency: {interval: 1, unit: day}}
```

`contract` uses OpenMetadata's native shape rather than ODCS. OpenMetadata's
ODCS importer has no `semantics` field, and a `mode=replace` import clears them.
`schema` lists only the columns being promised, not every column. `dataType`
takes OpenMetadata type names (`VARCHAR`, `BIGINT`, `TIMESTAMP`, ...).

The `entity` binding says which asset the contract is for:

- `dbt_model` / `dbt_source`: a table built or read by dbt. Sync finds its
  OpenMetadata table from the relation in a production manifest, so the file
  names no environment. These are the only contracts CI can check.
- `fqn`: anything else OpenMetadata catalogs, e.g. a Superset dataset
  (`type: dashboardDataModel`, `fqn: Superset.model.41`) or a raw table dbt
  doesn't declare.

## What is enforced where

| Part | Checked by | When |
| --- | --- | --- |
| `schema`, dbt bindings | `ol-dbt validate --only data_contract` (dbt PR CI) | every PR, before merge |
| `schema` columns, all bindings | OpenMetadata, against its catalog | after ingestion, on `ol-dbt contracts validate` or OpenMetadata's daily run |
| `schema` types, all bindings | `ol-dbt contracts validate` / `sync --dry-run` only | same as above |
| `semantics` | OpenMetadata | same as above |
| `sla` | nothing in OpenMetadata 2.0.2 (stored, never evaluated) | Dagster freshness checks enforce freshness |

OpenMetadata 2.0.2 reports a retyped column in `typeMismatchFields` but doesn't
count it as a failure, so the contract still shows Success in OpenMetadata
(`DataContractRepository.validateSchemaFieldsAgainstEntity`). The `ol-dbt
contracts` commands fail on it; OpenMetadata's own status and alerts won't.

The CI check fails when a contracted column is missing from the model or source
YAML, is no longer selected by the model SQL, has no `data_type`, or has a type
outside the contract type's family. It uses the same families OpenMetadata
uses, so `VARCHAR` → `STRING` passes and `VARCHAR` → `BIGINT` fails. Changing a
contracted column therefore means changing its contract in the same PR, where
the reviewer sees it.

Quality stays in dbt tests and Dagster asset checks. Contracts don't use
`qualityExpectations`.

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
