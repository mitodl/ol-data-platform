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
status: Draft                   # required: Draft or Approved; applied by PATCH, not part of CreateMetric
implemented_by:
- dbt_model: afact_learner_courserun_progress
  columns: [completion_status]
- dbt_model: mv_b2b_learner_enrollment
  columns: [completion_status]
```

`metric` uses OpenMetadata's field names, so the file needs no translation. The
fields it may set, their shapes and their enum values (`metricType`,
`unitOfMeasurement`, `granularity`, ...) are the server's own: see
`CreateMetric` in
`src/ol_dbt_cli/ol_dbt_cli/lib/openmetadata_metric_schema.json`, a snapshot of
the request schema our OpenMetadata publishes. The differences from that
schema:

- `owners` and `reviewers` are `{type: team | user, name: ...}`. OpenMetadata
  takes an id, which only exists in the live catalog, so sync resolves the name.
  An empty list (`reviewers: []`) clears them; leaving the key out keeps what
  OpenMetadata has.
- `id` and `fullyQualifiedName` belong to sync.
- `assets` is not allowed: it takes catalog ids a file cannot know. The tables
  under `implemented_by` become lineage instead. Sync keeps the assets a
  metric has in OpenMetadata.
- `domains`, `dataProducts` and `extension` are not allowed: a bot's PUT does
  not change them on an existing metric, so a file change would never land.
- `lifeCycle` is not allowed: OpenMetadata keeps it current, and a PUT keeps
  any live timestamp newer than the file's, so the file would never match.
- `displayName` lands only when sync creates the metric or OpenMetadata has
  none. Every bot is denied EditDisplayName (DefaultBotPolicy), and
  OpenMetadata keeps a stored display name against a bot's change
  (`EntityUpdater.updateDisplayName`). So when the file and OpenMetadata
  differ, sync prints a `stale` line, syncs the rest of the metric and counts
  it as failed. Change the display name in OpenMetadata to match the file.
- `status` is required and takes only `Draft` or `Approved` of the server's
  statuses (see "Status and approval"). A PUT resets the status in
  OpenMetadata, so sync always restores it from the file.

A glossary term is a `tags` entry with `source: Glossary`. `metricExpression`
is documentation only.

`implemented_by` is ours. Each entry is a `dbt_model` and the columns that
carry the metric. The first entry is the defining implementation; later entries
are places it is served from, e.g. a StarRocks view that selects the column.
An entry may instead give an `fqn` (with a `type`, default `table`) for an
OpenMetadata entity dbt does not build. CI cannot check those.

## Status and approval

`status` is `Draft` or `Approved`, nothing else. Approval is the pull request
that changes the file: a reviewer who approves that PR approves the metric, and
sync then sets `Approved` in OpenMetadata. In Review, Rejected, Deprecated,
Archived and Unprocessed are not set from files.

The sync bot is not a metric reviewer, and should not be made one. The rules in
this paragraph and the next are read from the OpenMetadata 2.0.5 source and
have not been observed on a server. Only a move from In Review to Approved or
Rejected checks the updater, and only when the metric has reviewers: the
updater must then be one of them, or OpenMetadata answers 403 "User '<bot>' is not a reviewer"
(`EntityUpdater.updateEntityStatus`, `checkUpdatedByReviewer`). Draft to
Approved, and any move out of Unprocessed (where a sync PUT leaves the status,
`setDefaultStatus`), are not checked, so the bot can apply a file's status
without being a reviewer. Reviewers are either all users or exactly one team
(`validateReviewers`), so the bot could not sit beside a reviewing team anyway.

Sync does not override a review in progress or a reviewer's rejection. When a
metric is In Review or Rejected in OpenMetadata, sync writes nothing for it (no
PUT, no PATCH, no lineage edge), with or without `--force` and whether or not
the file changed a structural field. It prints one line for that metric,
counts it as failed, carries on with the others, and exits non-zero.
`--dry-run` reports the same. Skipping only the status PATCH would not be
enough: going by the same source, a structural PUT alone resets the status to
Unprocessed, which would end a review without a reviewer or undo a rejection.

- In Review clears when a reviewer approves or rejects the metric in
  OpenMetadata, or someone moves it out of In Review.
- Rejected means a reviewer turned the definition down. Sync reads only the
  live status, so moving the metric out of Rejected in OpenMetadata releases
  it. The expected process is to change the metric file in a pull request as
  well (e.g. back to `Draft` with the definition fixed), but sync does not
  check for that. Moved out with no file change, a file that says `Approved`
  differs from the live status and from the bot's last write, so sync reports
  status drift and writes nothing, and `--force` would approve it again. A
  file that says `Draft`, with the metric moved to Draft, syncs.

Once the hold clears, the next sync proceeds, reporting drift if the status
differs from the file.

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
- a value in `metric` does not fit the schema snapshot (a wrong type, a value
  outside an enum, a nested object missing a required key);
- `status` is missing, or is anything other than `Draft` or `Approved`.

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
   bot changed the description, owners, reviewers or status since the bot last
   wrote the metric (the newest version in its history whose `updatedBy` is the
   bot), and the change differs from the file, it reports them and exits
   non-zero without writing, unless `--force`. The edit then belongs in a pull request.
   A metric that is In Review or Rejected is left alone even with `--force` (see
   "Status and approval").
   A file change to a field nobody edited in OpenMetadata syncs whatever else
   was edited there;
3. PUTs the `metric` body only to create the metric or to change one of its own
   fields (type, unit, expression, ...);
4. PATCHes description, owners, reviewers and `status` where they still differ,
   and the file's digest when it changed (below). A bot's PUT keeps a stored
   description, existing owners and custom properties and resets the status,
   so these land only by PATCH;
5. PUTs a table-to-metric lineage edge per `implemented_by` table. OpenMetadata
   stores no column lineage for a Metric, so the columns stay in this file.

Sync records a SHA-256 of what the file declares (`metric`, `status` and
`implemented_by`) in the metric's `metricFileSha256` custom property, and
PATCHes it whenever the file changed, even when no other field did. That
write makes the bot the author of the newest version. Without it, a file that
adopts an edit made in OpenMetadata (e.g. copies a description someone wrote
there) would write nothing, the bot's last version would keep the old value,
and the next file change to that field would read as drift. The property is a
string custom property on the metric type, defined by ol-infrastructure
(`om_server_config`). When it is not defined, OpenMetadata refuses the PATCH
and sync fails that metric, saying so. `--dry-run` lists it among the fields
it would PATCH. It is not part of the drift check, and a file cannot set it
(`extension` is not allowed).

An unchanged file writes only the lineage edges, which OpenMetadata upserts.
OpenMetadata stores descriptions HTML-encoded (`learner's` as `learner&#39;s`),
so sync compares them with the encoding undone. Markup the server's sanitizer
removes cannot compare equal, and sync warns that the file needs fixing.
Fields the file does not set are left as OpenMetadata has them, except
`displayName`: a file without one, for a metric that has one in OpenMetadata,
is reported as `stale` like any other difference (see above). Deleting a tag
from a file removes it from the metric only if sync applied it (the tag's
`appliedBy` is the bot); a tag someone added in OpenMetadata stays, even if the
file listed it too. Lineage from a StarRocks view is skipped with a warning until
OpenMetadata has a StarRocks service, and an edge whose `implemented_by` entry
was removed is not deleted.
