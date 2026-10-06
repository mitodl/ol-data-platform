# Business metrics

Each file here defines one business metric and names the columns that carry it.
The column is the definition: the file gives it a name, a description and an
owner, and says where to find it. The files are the source of truth, and
OpenMetadata is made to match them.

A metric gets a file once its column exists. A metric that is still computed
several ways has no entry until one column carries it.

## Format

```yaml
metric:                         # an OpenMetadata CreateMetric body
  name: learner_completion_status
  displayName: Course completion status
  description: >-
    Where a learner stands in a course run: certified, passed, in_progress or
    not_started.
  metricType: OTHER
  owners: [{type: team, name: data-engineering}]   # optional; resolved to ids on sync
  reviewers: [{type: team, name: data-engineering}]
  tags:
  - {tagFQN: <glossary>.<term>, source: Glossary, labelType: Manual, state: Confirmed}
status: Approved                # entityStatus; applied by PATCH, not part of CreateMetric
implemented_by:
- dbt_model: afact_learner_courserun_progress
  columns: [completion_status]
- dbt_model: mv_b2b_learner_enrollment
  columns: [completion_status]
```

`metric` uses OpenMetadata's field names, so the file needs no translation. The
fields it may set, their shapes and their enum values (`metricType`,
`unitOfMeasurement`, `granularity`, `status`, ...) are the server's own: see
`CreateMetric` in
`src/ol_dbt_cli/ol_dbt_cli/lib/openmetadata_metric_schema.json`, a snapshot of
the request schema our OpenMetadata publishes. Two differences from that
schema:

- `owners` and `reviewers` are `{type: team | user, name: ...}`. OpenMetadata
  takes an id, which only exists in the live catalog, so sync resolves the name.
- `id` and `fullyQualifiedName` belong to sync.

A glossary term is a `tags` entry with `source: Glossary`. `metricExpression`
is documentation only.

`implemented_by` is ours. Each entry is a `dbt_model` and the columns that
carry the metric. The first entry is the defining implementation; later entries
are places it is served from, e.g. a StarRocks view that selects the column.
An entry may instead give an `fqn` (with a `type`, default `table`) for an
OpenMetadata entity dbt does not build. CI cannot check those.

## Naming

OpenMetadata's fully qualified name for a metric is the bare name, so names are
global. Use `<subject>_<measure>` in snake case with no double underscore, e.g.
`learner_completion_status`, `contract_seats_used`,
`learner_needs_attention_since`. Name the file after the metric.

## What CI checks

`ol-dbt validate --only metric_registry` runs in dbt PR CI on every PR that
touches `metrics/`, the dbt project or the CLI. It fails when:

- an `implemented_by` model has no `.sql` file in the project;
- a listed column is not declared in the model's YAML, or is not selected by
  its SQL (skipped when the SQL is a `SELECT *` that could not be expanded);
- two files declare the same metric, the name breaks the convention, or the
  file is not named after the metric;
- `metric` sets a field `CreateMetric` does not have;
- a value in `metric`, or `status`, does not fit the schema snapshot (a wrong
  type, a value outside an enum, a nested object missing a required key).

Changing or removing a column that carries a metric therefore means changing
the metric file in the same PR, where the reviewer sees it.

Models are looked up in the project's YAML and SQL files, not the manifest.
PR CI parses with the DuckDB target, where the StarRocks views in
`b2b_analytics` and `b2b_learner_records` are disabled, so the manifest does
not contain them.

A file with no `metric.name`, no `implemented_by`, a malformed entry, or a key
it does not take (e.g. `Status` for `status`) stops the command with a message
naming the file.

## Commands

```bash
# Credential-free, what CI runs. Needs no manifest and no network.
ol-dbt validate --only metric_registry

# After an OpenMetadata upgrade: rewrite the schema snapshot from the server's
# published OpenAPI document (no token needed), then commit the diff.
ol-dbt metrics refresh-schema --server-url https://data.ol.mit.edu/api
```

The snapshot is committed instead of fetched in CI so that a PR check does not
depend on the catalog being reachable, and so that an upgrade changes what a
metric file may contain in a reviewed commit.

Publishing these files to OpenMetadata (`ol-dbt metrics sync`) is not built yet.
