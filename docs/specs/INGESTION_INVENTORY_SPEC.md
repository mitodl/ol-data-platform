# Ingestion Inventory & Airbyte Config-as-Code — Spec

**Status:** Spec (accepted direction; implements RFC 12319 step 1 and unblocks RFC 12711 step 2)
**Project:** `wp-airbyte-dlt-migration-programmatic-airbyte-manag-14e813`
**Task:** `tk-airbyte-config-as-code-terraform-provider-via-pu-d71323`
**RFC:** [mitodl/hq#12319](https://github.com/mitodl/hq/discussions/12319) — Airbyte → dlt migration
and programmatic Airbyte management (**Accepted 2026-08-13**)
**Related RFC:** [mitodl/hq#12711](https://github.com/mitodl/hq/discussions/12711) §3 —
fixes the `(deployment, layer)` entry shape this spec extends
(`docs/specs/QA_DATA_TOPOLOGY_SPEC.md`)
**Code pinned at:** `1ff30dcae0a1b8a8562244f67f5946facb3ac332` (ol-data-platform),
`3ab56f5d3` (ol-infrastructure)

RFC 12319 decided *that* the interim Airbyte setup gets managed as code, that a YAML inventory
is the human source of truth, and that the inventory is a normal PR-reviewed file with a CI
check. RFC 12711 §3 fixed the key and the per-environment `strategies` map. This document
specifies the parts neither settled: what an entry contains below the unit level, where the
file lives, how it crosses the repo boundary into Pulumi, what generates what, and how the
first version of the file gets built without hand-transcribing 372 table declarations.

It does not revisit those decisions, and it does not cover the dlt cutover itself (RFC 12319
phase 2, strictly sequenced after this lands).

---

## 1. Four measured findings that shape the schema

Each of these invalidates an obvious design, so they come before the schema rather than after.

### 1.1 Raw table names are not positionally parseable

The natural shortcut — derive `(deployment, layer)` from the `raw__<deployment>__<layer>__…`
table name — does not work. Across the 372 raw tables declared in `src/ol_dbt/models/**/_*sources.yml`
there are 23 distinct two-segment prefixes with at least four incompatible shapes:

| Prefix | Shape | Tables |
|---|---|---|
| `raw__mitx__openedx__mysql__*` | deployment, layer, system, table | 54 |
| `raw__ovs__postgres__ui_video` | deployment, **system**, table (no layer) | 6 |
| `raw__thirdparty__zendesk_support__tickets` | pseudo-deployment, vendor, table | 7 |
| `raw__thirdparty__salesforce___destination_v2__Opportunity` | vendor + Airbyte destination artifact, **mixed case**, triple underscore | 2 |
| `raw__edxorg__s3__tables__auth_user` | deployment, system, sub-namespace, table | 18 |

**Consequence:** each unit declares its `table_prefix` explicitly. The inventory maps
prefix → unit; nothing parses names. A validator rule asserts prefixes are non-overlapping,
which is what makes the mapping total and unambiguous.

**Confirmed against the live workspace (2026-08-14, §8.1).** There are exactly two naming
mechanisms, and neither is a positional parse:

- 31 connections set an Airbyte `prefix` (`raw__mitxonline__app__postgres__`) and carry short
  stream names (`users_user`). Raw table = prefix + stream name.
- 12 connections set **no prefix at all** and carry the fully-qualified name in the stream
  itself (`raw__mitx__openedx__tracking_logs`, `raw__edxorg__s3__program_course`). These are
  the S3/file and tracking-log sources.

So a unit's `table_prefix` is the connection's `prefix` where one exists, and the common
`raw__…__` head of its stream names where none does.

### 1.2 `loader:` in the dbt sources YAML is already wrong

`src/ol_dbt/models/staging/edxorg/_edxorg_sources.yml` declares `loader: airbyte`, but
`raw__edxorg__s3__tables__*` is produced by dlt — `src/ol_dlt/ol_dlt/sources/edxorg_s3/__init__.py:180`
builds exactly that resource name. edxorg is the one *completed* Airbyte→dlt migration and the
dbt metadata still says Airbyte. Eighteen `raw__edxorg__s3__*` tables sit under that wrong label,
and four other files already say `loader: dlt` (`mitpe`, `oll`, `mit_climate`,
`edxorg__discovery`), so the field is neither reliably right nor reliably absent.

**Consequence:** `loader` is an inventory field, and `ol-dbt generate` emits it rather than
`_adjust_source_schema_pattern` hard-coding `loader: airbyte`
(`src/ol_dbt_cli/ol_dbt_cli/commands/generate.py:91`). It also means the loader-agnostic dedup
macro (`tk-loader-agnostic-dbt-raw-metadata-macro-confirm-t-eec760`) must resolve through the
inventory, not through `source.loader` — today's values would route 18 dlt tables to the
Airbyte branch.

### 1.3 Connection names are load-bearing in Dagster, character for character

`dg_projects/lakehouse/lakehouse/definitions.py:197-199` selects connections with
`conn.name.lower().endswith("s3 data lake")`. `OLAirbyteTranslator`
(`definitions.py:164-182`) derives each asset's Dagster group name from the connection name by
stripping the U+2192 arrow and non-alphanumerics, and `group_name_to_interval`
(`definitions.py:219-254`) keys 32 sync cadences off those derived strings, defaulting to 24 h
for anything unlisted (`definitions.py:266`).

So a config-as-code rollout that "cleans up" connection names would silently re-group every
asset, orphan 32 schedule entries, and downgrade 6- and 12-hour syncs to daily — with no error
anywhere.

**Consequence:** import fidelity is a hard requirement, not a nicety. The generated
`airbyte_connection.name` must reproduce today's name byte-for-byte, arrow included. The
inventory carries the connection name as data, and a validator rule asserts every unit's name
still ends with `s3 data lake`. Renaming becomes a deliberate, separately reviewed change that
must move `group_name_to_interval` in the same commit — and step 5 below removes that hazard
entirely by generating the interval map from the inventory.

### 1.4 dbt sources are a projection of ingestion, not a census of it

The 17 sources files declare 372 raw tables. Production raw holds ~2,090 (2026-08-05 audit;
QA holds 2,738). The inventory is a statement about *what is loaded*, so it is roughly 5×
larger than what dbt declares, and generating dbt source YAML for all of it would bury the
curated column descriptions and trip the undocumented-column gate added in #2555.

**Consequence:** each table entry carries `modeled: true|false` (default `false`). Only
`modeled: true` tables are emitted into dbt sources YAML, and generation merges rather than
overwrites — `_merge_sources_content` (`generate.py:119-188`) already preserves existing
descriptions and does the right thing. The gap between loaded and modeled becomes a report
(`ol-dbt inventory report --unmodeled`), which is the constructive form of RFC 12711's open
"QA raw cleanup" question: a table loaded for two years and never modeled is a candidate for
retirement, and now it is countable.

---

## 2. The seam: what the inventory owns and what it does not

The inventory lives in ol-data-platform and is owned by data engineering. Connection *identity*
is infrastructure and stays in ol-infrastructure. The join key is the unit key.

| Fact | Owner | Why |
|---|---|---|
| Which deployments/layers we load | inventory (ol-data-platform) | It is the analytics contract |
| Which tables/streams, their cursor field, PK, sync mode | inventory | Changes with dbt models, reviewed by the same people |
| Which tables are modeled in dbt | inventory | Generates the sources YAML |
| Per-environment `strategies` (qa/local) | inventory | RFC 12711 §3 |
| Sync cadence | inventory | Today split between Dagster and Airbyte; §5 unifies it |
| DB host, port, SSL, Vault path, credentials | ol-infrastructure | Stack references and secrets; must never be in a data repo |
| Airbyte workspace ID, destination config, connector definition/version pins | ol-infrastructure | Airbyte-implementation detail, disposable with Airbyte |
| Airbyte source/destination/connection UUIDs | Pulumi state | Machine identity, not a human decision |

The practical test: **anything that would have to be rewritten when a source moves to dlt does
not belong in the inventory.** Host and credentials get rewritten (dlt reads them from Vault at
connect time — see `pat-reusable-ol-dlt-database-source-databasesourcesp-7cd605`); table lists,
cursors and primary keys do not. That is what makes the inventory survive the migration and
makes the later cutover a backend swap.

---

## 3. Inventory schema v1

### 3.1 Physical layout

```
ingestion/
  inventory/
    units/<deployment>__<layer>.yml     # one file per unit; ~40 files
    schema/unit.schema.json             # JSON Schema, version-stamped
    retired.yml                         # graveyard, see §7.2
```

One file per unit rather than one big file: units are the review granularity (a PR adds tables
to one layer), the largest unit is ~55 tables, and per-file ownership keeps merge conflicts off
the critical path when several source migrations run in sequence.

### 3.2 Unit entry

RFC 12711 §3's shape is the head of the entry and is reproduced verbatim; everything from
`loader:` down is new here.

```yaml
schema_version: 1

deployment: mitxonline            # mitx | mitxonline | xpro | mitlearn | edxorg | ...
layer: app_postgres               # see §3.7 — RFC 12711's six values are not enough
scope: scoped                     # scoped | singleton
strategies:
  qa: ingest                      # ingest | mirror | omit
  local: fixture                  # ingest | fixture | omit
# mirror_max_age_days: 30         # required iff any strategy is `mirror`

loader: airbyte                   # airbyte | dlt
table_prefix: raw__mitxonline__app__postgres__   # §1.1; must be unique across all units

airbyte:                          # required iff loader == airbyte; dropped at cutover
  connections:                    # a LIST — one unit can have several (§3.5)
    - name: "MITx Online Open edX DB → S3 Data Lake"   # §1.3, byte-exact
      status: active              # active | inactive — §3.6
      sync_interval_hours: 12     # per connection, not per unit (§3.5)
      streams: [assessment_assessment, …]             # which tables this one carries
  source_kind: source-postgres
  replication_method: xmin        # xmin | cursor | cdc  — see §3.4

dlt:                              # required iff loader == dlt
  source_module: ol_dlt.sources.edxorg_s3
  write_disposition: merge        # merge | replace | append

tables:
  - name: ecommerce_basketdiscount        # stream name at the source
    raw_table: raw__mitxonline__app__postgres__ecommerce_basketdiscount
    sync_mode: incremental_append         # provider enum, §6.2
    cursor_field: [updated_on]            # required iff sync_mode starts with `incremental`
    primary_key: [id]
    modeled: true                         # §1.4; default false
    # excluded_columns: [password]        # optional; enforced by both backends
    # renamed_from: ecommerce_basket_discount   # §7.2
```

### 3.3 Rules the validator enforces

Carried over from RFC 12711 §3, unchanged:

1. `local: mirror` is rejected by the schema (the enum omits the value).
2. `local: ingest` requires `loader: dlt` — Airbyte cannot run in k3d.
3. `mirror_max_age_days` is required iff any strategy is `mirror`, with no default.

New here:

4. `table_prefix` values are pairwise non-overlapping **across units**, and every
   `tables[].raw_table` starts with its unit's prefix. This is what makes prefix → unit total
   (§1.1). Note the direction: prefix → unit must be a function; connection → prefix need not
   be injective, because a unit legitimately has several connections (§3.5).
5. `cursor_field` is required iff `sync_mode` is incremental, and must name a column the
   table actually has once §8's warehouse reconcile runs.
6. Every `airbyte.connections[].name` ends with `s3 data lake` (case-insensitive) — the
   Dagster selector's precondition (§1.3) — or the unit is marked `dagster_visible: false`,
   which is an assertion that no dbt model depends on it (§8.1 finding C).
7. `airbyte:` is present iff `loader: airbyte`; `dlt:` is present iff `loader: dlt`.
8. Every `raw_table` is globally unique across units, and appears in exactly one connection's
   `streams` within its unit.

### 3.4 `replication_method` is deliberately recorded, and is not decorative

`airbyte.replication_method` records how the Postgres source detects change: `xmin`, an
explicit cursor column, or CDC. Two open questions read straight off it once the inventory is
populated:

- `tk-determine-per-source-incremental-cursor-viabilit-51f299` — which connections use xmin,
  and therefore which need a replacement cursor column chosen before dlt can take over. Today
  that answer requires crawling the Airbyte UI; after this lands it is `rg replication_method: xmin`.
- The Airbyte-side deadline: source-postgres 3.8+ refuses xmin mode outright on any database
  that has ever exceeded 2^32 lifetime transactions
  (`les-airbyte-source-postgres-3-8-refuses-xmin-mode-on-a5438b`). That deadline is independent
  of the dlt migration, and the inventory is where its blast radius becomes enumerable.

RFC 12319's resolution needs one correction of fact, which does not change its conclusion.
CDC *is* in use: the three MongoDB forum connections (`mongodb-v2`, which is change-stream
based by design) carry `_ab_cdc_cursor` on 13 streams. All three connections are **inactive**
and the source is not wanted, so the migration remains delete-capture-neutral in practice —
but the RFC's blanket "no Airbyte connection uses CDC" was established by grepping dbt models,
which cannot see a cursor that is never modelled. State it as "the only CDC connections are
the three MongoDB forum ones, all paused".

### 3.5 A unit has connections, plural — and cadence lives on the connection

Four prefixes are shared by two connections each, and all four are the same deliberate
pattern: one enormous table split into its own connection so it can run on a different
schedule.

| Unit prefix | Bulk connection | Split-out connection |
|---|---|---|
| `raw__mitx__openedx__mysql__` | MITx Residential Open edX DB (70) | …Studentmodule History (1) |
| `raw__mitxonline__openedx__mysql__` | MITx Online Open edX DB (64) | …Student Module History (1) |
| `raw__xpro__openedx__mysql__` | xPro Open edX DB (66) | …Studentmodule History (1) |
| `raw__irx__edxorg__bigquery__` | IRx BigQuery (3) | IRx BigQuery - Email Opt In (1) |

In three of the four the split-out stream is
`coursewarehistoryextended_studentmodulehistoryextended` — the largest table in Open edX.

This is not a defect to normalize away; it is the reason cadence cannot sit on the unit. The
interval map runs the bulk Open edX connections at 12h and the history ones separately, so
`sync_interval_hours` belongs on `airbyte.connections[]`, and the unit's `tables` are
partitioned across them by the connection's `streams` list. **This closes open question 3**,
which asked whether cadence was a unit or table property: it is neither — it is a property of
the connection, and the connection is the thing config-as-code creates.

### 3.6 Paused connections are inventory data, not absences

12 of 43 connections are `inactive`, and Dagster builds assets for 8 of them — including
Open Discussions (93 streams) and both HubSpot connections. A config-as-code import that
ignored `status` would either resurrect paused ingestion on apply or silently drop it from
the inventory, and both are wrong. `status` is therefore a required field on each connection,
and pausing something becomes a reviewable one-line diff rather than a UI click.

### 3.7 The `(deployment, layer)` vocabulary — resolved 2026-08-14

**Decision: vendor SaaS sources become deployments in their own right, with the existing
`api` layer, and `thirdparty` stops being a pseudo-deployment.** So `(salesforce, api)` and
`(zendesk, api)` are distinct keys instead of colliding under `(thirdparty, api)`.

That leaves five values to add rather than eleven: `hubspot`, `bigquery`, `google_sheets`,
`s3`, `openedx_notes`.

Two things dissolved on inspection rather than needing a decision. `raw__ovs__postgres__` and
`raw__ocw__studio__postgres__` do not need a `postgres` or `studio_postgres` layer, and do not
need renaming: §1.1 already established that names are never parsed and `table_prefix` is
declared, so the prefix and the layer are simply allowed to disagree. Both units are
`app_postgres` with their existing prefixes untouched.

`openedx_notes` is the one that had to be added rather than absorbed: MITx Online's edX Notes
is a different MySQL database from its Open edX one, so `mysql` cannot name both. The draft
renderer demonstrated the failure by folding 8 edX Notes tables into `mitxonline__mysql` —
RFC 12711 §3's own warning about source-level keys, one level down.

The vocabulary lives in `ingestion/inventory/vocabulary.yml` as **data, not code**: adding a
source is an edit there plus a unit file, with no change to `ol_dbt_cli`. The validator checks
membership; the JSON Schema only checks the shape, because a closed enum in the schema would
put the vocabulary in two places.

**Still to do:** raise the amendment on RFC 12711, since the key is shared and the
per-model `meta.qa_branches` contract in that RFC's step 4 names the same pairs.

### 3.7.1 The original finding, for the record

The six values (`mysql`, `mongodb`, `api`, `tracking_logs`, `fastly`, `app_postgres`) were
derived from the Open edX and app-database deployments. Against the real connection set, 20 of
35 draft units have no legal `layer`. The missing values are all vendor/SaaS or
second-database layers:

`hubspot`, `salesforce`, `zendesk`, `mailgun`, `github`, `bigquery`, `google_sheets`, `s3`,
`studio_postgres` (OCW), `postgres` (ODL Video Service, which has no `app` segment), and
`openedx_notes` (MITx Online's edX Notes MySQL, a distinct database from the Open edX one).

Two of those deserve attention rather than a mechanical addition: `postgres` for ODL Video
Service is the same thing `app_postgres` names elsewhere and should probably be renamed at the
source; and `openedx_notes` is genuinely a separate layer that a `mysql`-only enum silently
merges into the Open edX database — the draft renderer did exactly that, folding 8 edX Notes
tables into `mitxonline__mysql`, which is the failure mode RFC 12711 §3 warns about applied to
layers instead of deployments.

**This is a change to RFC 12711's schema, not just this spec's**, since the key is shared.
Raise it on 12711 before the inventory is populated (step 3), not after.

---

## 4. Crossing the repo boundary: a committed, generated JSON

The Pulumi program in ol-infrastructure must not import a Python package from ol-data-platform,
and Pulumi must not parse the YAML dialect directly. The contract between the repos is a
**rendered JSON document**, produced by `ol-dbt inventory render airbyte`, carrying
`schema_version` and only the Airbyte-relevant fields.

Two reasons this is a JSON render and not the YAML file:

1. The YAML is a source-of-truth format that will keep growing dlt-shaped fields; the render is
   narrow and stable, so ol-infrastructure does not break when the inventory schema grows.
2. The render is produced by a validated command. A malformed inventory fails in ol-data-platform
   CI, not halfway through a Pulumi apply against production Airbyte.

**The rendered JSON is committed into ol-infrastructure**, beside the stack that consumes it,
with a header naming its source and forbidding hand edits. It is not fetched, and no pipeline
renders it.

### Why there is no cross-repo pipeline

An earlier draft of this spec specified a Concourse job that watched
`ingestion/inventory/` in ol-data-platform, rendered the JSON and applied Pulumi. That was
machinery for an event that does not happen often enough to need it, and it duplicated
something that already exists.

`applications/airbyte` is already one of the 33 applications managed by the **`simple_pulumi`
meta-pipeline** (`src/ol_concourse/pipelines/infrastructure/simple_pulumi/`) — the pattern for
stacks with no build steps, "triggered solely by infrastructure code changes".
`applications/airbyte_connections` registers there like any other stack, and a change to the
committed JSON *is* an infrastructure code change, so it triggers with no new plumbing.

The cost is one extra pull request:

1. Inventory PR in ol-data-platform; CI validates it (§7.2).
2. Run the render; open a small PR in ol-infrastructure with the regenerated JSON.
3. The standard stack pipeline applies it on merge.

That is a fair trade for a file that changes a handful of times a year, and it puts the diff in
front of the people reviewing infrastructure changes, in the repo where it takes effect — which
is where an Airbyte connection change most wants a second pair of eyes. During the phase-2
migration, when units flip `loader: airbyte → dlt` one at a time, that review is the point.

### The automation that does earn its place: drift detection

Static configuration that silently stops describing reality is the failure this whole project
exists to end, and an auto-apply does nothing about it — the divergence it cannot catch is
someone editing a connection in the Airbyte UI.

So the recurring job runs the *other* direction: on a schedule, dump the live workspace, diff it
against the inventory, and report any difference. Same read-only path `bin/airbyte-inventory.py`
already uses. This is what makes the inventory trustworthy between changes, and it is the only
part of the Airbyte-as-code story that needs to run on a timer.

---

## 5. What the inventory generates

| Target | Command | Notes |
|---|---|---|
| dbt sources YAML | `ol-dbt generate sources --from-inventory` | Merges into existing files; emits `loader` from the unit (§1.2); only `modeled: true` tables (§1.4) |
| Airbyte config | `ol-dbt inventory render airbyte` → committed JSON → Pulumi | §4, §6 |
| Dagster sync cadence | `ol-dbt inventory render dagster-intervals` | Replaces the hand-maintained `group_name_to_interval` literal (`definitions.py:219-254`) |
| dlt source specs | `ol-dbt inventory render dlt` | Phase 2 only; emits `DatabaseSourceSpec`/`DatabaseTable` inputs (`src/ol_dlt/ol_dlt/database.py`) |

### Where this lives: `ol-dbt inventory`, not a new package

The commands go under the existing CLI as an `inventory` sub-app, and the schema, loader and
validation rules go in a module beside them.

The case for a separate `ol_ingest` package rested on two consumers that would not want dbt in
their dependency tree. §4 removed the first — there is no Concourse task rendering JSON, so
nothing at the repo boundary needs a light install. The second is real but not yet here:
phase 2 builds dlt sources in `dg_projects/data_loading`, a separately packaged Dagster code
location, and making it depend on `ol_dbt_cli` (duckdb, dbt-core, dbt-trino) to read a YAML file
would be wrong — `src/ol_dlt` is already excluded from the workspace for exactly that reason.

Against that: `ol-dbt validate` already has the `Severity` enum, the error/warning/info split,
`--format json` for CI, and baseline load/write plumbing (`validate.py:1291-1374`), all of which
§7.2's check wants. Building a second CLI to reuse none of it is the more expensive mistake
today.

So: land it in `ol_dbt_cli`, keep the inventory module free of dbt and duckdb imports so the
split stays cheap, and split it out when phase 2 actually needs it — not before.

**Warehouse introspection does not disappear — it changes role.** `ol-dbt generate sources`
today discovers tables *downstream* of ingestion, from Trino or the local DuckDB registry
(`generate.py:196-216`, `380-395`). That path becomes `ol-dbt inventory reconcile`: a check that
compares what the warehouse actually holds against what the inventory says should be there,
reported in three buckets — *in inventory, missing from warehouse* (ingestion is broken),
*in warehouse, missing from inventory* (undeclared drift), *in both* (fine). It stays useful
precisely because it is an independent observation; making it the source of truth is the
current defect.

The Dagster interval map is worth calling out as the immediate win: it is a hand-maintained
32-entry dict of strings that must match names Airbyte generates, with a silent 24-hour default
for typos (§1.3). Generating it removes an entire class of "why did this source go stale"
incident.

---

## 6. Airbyte-as-code mechanics

### 6.1 Provider, edition, auth

The official `airbytehq/airbyte` Terraform provider (v1.3.0, released 2026-08-11) supports OSS
self-managed instances and accepts **HTTP Basic** (`username` / `password`) alongside OAuth and
bearer auth. That matches the existing APISIX basic-auth API route Dagster already uses —
`server_url = https://api-airbyte.odl.mit.edu/api/public/v1`, credentials from the same Vault
KV v1 secret Dagster reads (`definitions.py:139-154`, path `dagster-http-auth-password`, mount
`secret-data`). No new ingress, no OIDC dance for machine access.

Consumed via Pulumi with `pulumi package add terraform-provider airbytehq/airbyte`, generating
a local SDK under `sdks/airbyte/` and a workspace member entry — the pattern already
established by `sdks/rootly` and `sdks/qdrant-cloud` (`pyproject.toml:45-46,323-327`).

### 6.2 Use the generic resources from day one

Typed connector resources (`airbyte_source_postgres`, …) were deprecated in provider 1.0 and
**removed in 1.1**. Since we have no existing Terraform state, there is nothing to migrate and
no reason to adopt a removed API: use `airbyte_source` / `airbyte_destination` with the
`airbyte_connector_configuration` data source, which resolves `definition_id` from a connector
name, validates configuration against the connector's JSONSchema at plan time, and splits
sensitive from non-sensitive values so diffs stay readable (the whole `configuration` attribute
is otherwise marked sensitive and shows as an opaque blob).

Pin `connector_version` explicitly. An unpinned data source resolves "latest", which silently
couples every plan to whatever Airbyte published that morning — and connector upgrades are
exactly the kind of change that must be a reviewed diff here, given 3.8's xmin refusal (§3.4).

Connection streams map onto the inventory almost one-to-one: `configurations.streams[]` takes
`name`, `sync_mode` (one of `full_refresh_overwrite`, `full_refresh_append`,
`incremental_append`, `incremental_deduped_history`, …), `cursor_field` (list),
`primary_key` (list of lists), and `selected_fields`. The inventory's `excluded_columns` is
rendered as the complement into `selected_fields`.

### 6.3 The replacement hazard, and the apply gate

`source_id` and `destination_id` on `airbyte_connection`, and `workspace_id` / `definition_id`
on `airbyte_source`, all **require replacement if changed**. A replaced connection is a new
connection: its sync state — the cursor position Airbyte has been advancing for years — is
gone, and the next sync either re-reads everything or, worse, starts from empty state on an
append destination.

This is the single largest operational risk in the whole config-as-code exercise, and it is
entirely a preview-time-detectable condition. Therefore:

**The apply job fails if the Pulumi preview contains any delete or replace of an
`airbyte_source`, `airbyte_destination`, or `airbyte_connection`.** Deliberate replacements are
performed by a human running the apply with an explicit override flag, never by the pipeline.
This gate goes in before the first import, not after.

`configuration` is *not* replacement-forcing, which is what makes §7's credential rotation an
in-place update.

### 6.4 Import, not recreate

Every existing source, destination and connection is imported into Pulumi state by UUID
(the provider supports `terraform import` / `import` blocks for all three; Pulumi's `import`
resource option carries the same). The acceptance test for the import phase is a **clean
preview**: after importing, `pulumi preview` shows zero changes of any kind. Any diff at that
point is a fidelity bug in the rendered config — a stream ordering, a namespace format, a
schedule type — and must be fixed in the renderer, not applied away.

Airbyte's connection schedules are expected to be manual (Dagster triggers syncs; the group
jobs and schedules are built at `definitions.py:259-283`). This must be confirmed per
connection during import: if any connection carries its own Airbyte-side cron, it is
double-scheduled today, and the inventory's `sync_interval_hours` has to reconcile the two
rather than silently pick one.

### 6.5 A separate, disposable stack

The connection config goes in a **new** Pulumi project, `applications/airbyte_connections`, not
into `applications/airbyte` (which deploys the server itself). The whole point is that this
stack is disposable: as each source migrates to dlt, its unit flips `loader: dlt`, the renderer
stops emitting it, and Pulumi removes the connection. When the last one goes, the stack is
destroyed and the Airbyte server stack outlives it only as long as it takes to shut down.

---

## 7. Two things that make the file honest

### 7.1 Credentials: Vault static roles

Airbyte stores connector config once and never renews a lease, so Vault *dynamic* database
roles structurally cannot work — the lease expires and the connection breaks. Switch source
read-replica credentials to Vault **static** roles: stable username, Vault-rotated password
(`tk-switch-source-read-replica-creds-from-vault-dyna-812a53`).

Because `configuration` is not replacement-forcing (§6.3), reconciling a rotated password is an
in-place update of `airbyte_source` — cheap and non-destructive. But it is not automatic:
Airbyte keeps using the old password until an apply runs. So the rotation period and the apply
cadence are one decision, not two. Specify them together in the same PR: a scheduled apply job
whose period is strictly shorter than the static role's rotation period, so a rotation is always
picked up before the next one lands.

Note what Airbyte's own external-secret-manager setting does *not* do: it governs where Airbyte
persists a secret it already holds. It does not consume Vault leases and does not rotate
database credentials. It is not an alternative to this.

### 7.2 Removals are acknowledged in the file, not in a review comment

RFC 12319's resolution requires CI to fail on any unacknowledged table removal or rename,
because the failure mode is silent: a dropped entry means the loader simply stops loading, with
no error anywhere and a dbt model that quietly goes stale.

Mechanism, following the ratchet pattern this repo already runs for dimensional layering
(`src/ol_dbt_cli/ol_dbt_cli/commands/validate.py:568-619`):

- The check diffs the inventory against the PR's merge base.
- Any `(unit, raw_table)` that disappears must appear either in `ingestion/inventory/retired.yml`
  with a date and a reason, or as `renamed_from:` on another entry in the same unit.
- Unacknowledged disappearance is an `ERROR` and is **not** baselineable — like RFC 12711 §2's
  declaration-contradicts-declaration finding, it is always fixable by editing text and has no
  legitimate upstream cause.
- `retired.yml` entries are never deleted. The graveyard is the record of what we used to load,
  and it is what makes "when did this table stop arriving" answerable.

Renames are the subtle half: a rename looks like a delete plus an add, and without
`renamed_from:` it passes an add-only check while silently orphaning every downstream model.

---

## 8. Steps and acceptance criteria

Strictly sequential, per RFC 12319's resolution 3. Steps 1–3 are ol-data-platform; 4–6 are
ol-infrastructure; 7 closes the loop.

| # | Step | Done when |
|---|---|---|
| 1 | `ol-dbt inventory` sub-app (§5): JSON Schema, dbt-free loader, `validate` | `ol-dbt inventory validate` passes on a hand-written two-unit fixture; all eight §3.3 rules have a failing test |
| 2 | Dump the live workspace and derive the findings — **`bin/airbyte-inventory.py` already does this**, and has been run (§8.1); folding it in is a move, not a rewrite | Generated inventory validates; connection names byte-identical to the API's (§1.3); `replication_method` captured per Postgres source |
| 3 | `ol-dbt inventory reconcile` — three-way diff of inventory vs warehouse vs dbt sources; land the reconciled inventory as a reviewed PR | The three buckets of §5 are reported; every one of the 374 dbt-declared raw tables maps to exactly one unit; unmapped tables are explained, not deleted |
| 4 | CI: schema validation + §7.2 removal/rename check on every PR touching `ingestion/inventory/` | A PR deleting a table entry fails; the same PR with a `retired.yml` entry passes |
| 5 | Pulumi `applications/airbyte_connections` + `sdks/airbyte`, provider pinned, **preview-gate first** (§6.3), then import every existing source/destination/connection | `pulumi preview` is empty after import — zero creates, zero updates, zero replacements |
| 6 | Commit the rendered JSON into ol-infrastructure and register the stack in `simple_pulumi`'s `pipeline_params` + `meta.py` (§4) | Changing the committed JSON triggers the stack's own pipeline; no new pipeline is written |
| 7 | Flip generation: `ol-dbt generate sources --from-inventory`, generate `group_name_to_interval` (§5) | Regenerating dbt sources from the inventory is a no-op diff except the corrected `loader:` values (§1.2) |
| 8 | Scheduled drift check: dump the live workspace, diff against the inventory, report differences (§4) | A connection edited in the UI is reported within a day |

Steps 1–4 unblock `tk-step-2-extend-the-rfc-12319-ingestion-inventory--5a2841` (RFC 12711's
critical path) — that step needs the schema and the file, not the Pulumi half. Do not hold it
for step 6.

Step 8 is the one piece that runs on a timer, and it is deliberately last: it compares live
Airbyte against the inventory, so it only means something once the inventory is real.

### Step 2 is already runnable: `bin/airbyte-inventory.py`

Reading the live workspace does not need the `inventory` sub-app to exist first, and the
findings are what tell us whether the schema above survives contact with production. The
script is read-only — every call is a GET — and takes the basic-auth credentials Dagster
already uses:

```shell
export AIRBYTE_PASSWORD="$(vault kv get -mount=secret-data \
    -field=dagster_unhashed_password dagster-http-auth-password)"

uv run python bin/airbyte-inventory.py all --username dagster
#   → airbyte-snapshot.json   redacted JSON snapshot
#   → airbyte-findings.md     findings A–F below
#   → ingestion/inventory/units/*.yml   draft units, TODO-marked
```

`report` and `render` re-run offline against a saved snapshot, so iterating on the derivation
costs nothing and needs no further credentials. Findings produced:

| | Finding | Answers |
|---|---|---|
| A | Replication method per source | which connections are on xmin (§3.4, `tk-…-51f299`) |
| B | Sync modes, cursor fields, primary keys | which incremental streams have no explicit cursor, and which dedup streams have no PK to merge on |
| C | Dagster coupling | connections the selector drops, groups silently on the 24h default, dead interval-map entries (§1.3) |
| D | Schedules | connections carrying their own Airbyte cron, i.e. double-scheduled (§6.4) |
| E | dbt reconcile | loaded-but-unmodeled and modeled-but-unloaded tables (§1.4) |
| F | Table prefixes | the authoritative `prefix` per connection, and any prefix shared by two connections (§1.1, §3.3 rule 4) |

The draft units are for review, not for merging: `scope`, both `strategies`, and any layer the
prefix does not determine are emitted as `TODO`, and a unit fed by more than one connection
carries a `_todo` naming the conflict rather than silently keeping one connection's metadata.

### 8.1 Measured state, 2026-08-14

The first clean run against production. These numbers are the baseline the inventory has to
reproduce, and several of them changed this spec (§1.1, §3.4–§3.7).

| | |
|---|---:|
| Connections | 43 (31 active, **12 paused**) |
| Configured streams | 1,518 (**723 on active connections**) |
| Sources | 50 — of which several have no connection at all |
| Destinations | 4 (one live `s3-data-lake`, three legacy `s3`/`s3-glue`) |
| Sources on `xmin` | 11, covering 560 streams |
| Incremental streams with no explicit cursor | 514 — i.e. riding xmin |
| Distinct explicit cursor fields | 22, dominated by Salesforce's `SystemModstamp` (1,176) |
| Connections carrying their own Airbyte cron | **0** — Dagster is the sole trigger, as assumed |

Findings that are work items rather than schema changes:

- **Bootcamps ingestion is gone, deliberately** — the app is no longer deployed (confirmed
  2026-08-14). The source exists with zero connections, its interval-map entry
  `bootcamps_production_app_db__s3_data_lake` matches nothing, and dbt still models 19
  `raw__bootcamps__app__postgres__*` tables against data that stopped arriving. Nothing was
  lost, but nothing was cleaned up either, and the tables read as current to anyone who does
  not already know. This is the case `retired.yml` exists for (§7.2) and it is the natural
  first entry: the check would not have prevented the retirement, it would have forced the
  decommission to be written down where a reader of the raw layer can see it.
- **9 connection groups fall through to the 24h default** because their derived name is absent
  from `group_name_to_interval` — including Zendesk, Open Discussions, GitHub, both HubSpots,
  and xPro's Studentmodule History. Generating the map from the inventory (step 7) removes the
  whole class.
- **2 interval-map entries are dead** (`bootcamps_production_app_db`, `edxorg_production_course_tables`).
- **4 connections are invisible to Dagster** — all four point at the legacy S3-Glue
  destinations and all four are paused, so this is dead config rather than a gap.
- **Both Salesforce connections are paused deliberately** — the data was not being used and
  keeping the connection active cost a seat licence (confirmed 2026-08-14). Salesforce is
  therefore the worked example of the other lifecycle: `status: inactive`, kept in the
  inventory rather than retired, because the decision is reversible and the reason is
  economic. It is also why paused units cannot simply be omitted (§3.6) — Salesforce still
  has a live interval-map entry, dbt models, and 568 configured streams, and all of that is
  correct and should stay, just not running.
- **1,185 loaded-but-unmodeled tables** against 374 modelled — the §1.4 ratio, now measured.

---

## 9. Open questions

None blocking. Three worth deciding as the steps that surface them land:

1. **Salesforce's `_destination_v2` table names** (§1.1) embed an Airbyte destination artifact
   and mixed case in the raw table name. Migrating that source to dlt cannot reproduce the
   name, so it is a rename with downstream model edits. Decide during step 3 whether to
   normalize now (while it is still Airbyte-owned and the rename is one PR) or at cutover.
2. **~1,700 loaded-but-unmodeled tables** (§1.4). The report exists after step 3; whether
   unmodeled tables get retired, and on what evidence, is RFC 12711's open QA-cleanup question
   and should be answered with the numbers in hand.
3. **Whether `sync_interval_hours` belongs on the unit or the table.** Today's cadence is per
   Airbyte connection, and connections are per unit, so unit-level is correct now. A dlt source
   can schedule per resource, so this may want to move later. Not worth pre-building.
