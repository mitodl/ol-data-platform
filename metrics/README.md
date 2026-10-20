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
  owners: [{type: team, name: data-platform}]   # optional; resolved to ids on sync
  reviewers: [{type: team, name: <business team>}]
  tags:
  - {tagFQN: <glossary>.<term>, source: Glossary, labelType: Manual, state: Confirmed}
status: Draft                   # entityStatus; applied by PATCH, not part of CreateMetric
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
  An empty list (`reviewers: []`) clears them; leaving the key out keeps what
  OpenMetadata has.
- `id` and `fullyQualifiedName` belong to sync.
- `assets` is not allowed: it takes catalog ids a file cannot know. The tables
  under `implemented_by` become lineage instead.

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

## Owners, reviewers and status

`data-platform` owns every metric. The business team that uses a metric
reviews it and is listed in `reviewers`. A metric whose business team does not
exist in OpenMetadata yet has no `reviewers` until it does. Approval happens on
the pull request that changes the file. A new metric starts as `Draft` and
moves to `Approved` in a later change, once a sync has written it.

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

# Publish to OpenMetadata. Needs OM_SERVER_URL, OM_BOT_JWT_TOKEN and a
# production manifest. --dry-run reads the catalog and writes nothing.
ol-dbt metrics sync --service "Starburst Galaxy" --manifest prod-manifest.json --dry-run
```

The snapshot is committed instead of fetched in CI so that a PR check does not
depend on the catalog being reachable, and so that an upgrade changes what a
metric file may contain in a reviewed commit.

## Sync to OpenMetadata

`ol-dbt metrics sync` makes each Metric entity in OpenMetadata match its file
and adds a lineage edge from each `implemented_by` table to it. It stops before
the first file when the server's version is not the one stamped in the schema
snapshot. For each file it:

1. resolves `owners`, `reviewers`, glossary and classification `tags` and the
   `implemented_by` tables by name, and writes nothing for that metric when one
   is missing;
2. reads the live metric and checks for drift: when someone other than the sync
   bot changed the description, reviewers or status since the bot last wrote
   the metric (the newest version in its history whose `updatedBy` is the bot),
   and the change differs from the file, it reports them and exits non-zero
   without writing, unless `--force`. The edit then belongs in a pull request.
   A file change to a field nobody edited in OpenMetadata syncs whatever else
   was edited there;
3. PUTs the `metric` body only to create the metric or to change one of its own
   fields (type, unit, expression, ...);
4. PATCHes description, owners, reviewers and `status` where they still differ.
   A bot's PUT keeps a stored description and existing owners and resets the
   status, so these land only by PATCH;
5. PUTs a table-to-metric lineage edge per `implemented_by` table. OpenMetadata
   stores no column lineage for a Metric, so the columns stay in this file.

An unchanged file writes only the lineage edges, which OpenMetadata upserts.
OpenMetadata stores descriptions HTML-encoded (`learner's` as `learner&#39;s`),
so sync compares them with the encoding undone. Markup the server's sanitizer
removes cannot compare equal, and sync warns that the file needs fixing.
Fields the file does not set are left as OpenMetadata has them, and a PUT
never removes a tag, so deleting a tag from a file does not remove it from the
catalog. Lineage from a StarRocks view is skipped with a warning until
OpenMetadata has a StarRocks service, and an edge whose `implemented_by` entry
was removed is not deleted.
