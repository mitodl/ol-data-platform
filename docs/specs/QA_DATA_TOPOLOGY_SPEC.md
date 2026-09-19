# QA/Production Data Topology — Spec Resolution

**Status:** Spec (accepted — resolves the RFC's blocking open questions; RFC moved Draft → Accepted 2026-08-05)
**Project:** `wp-qa-production-data-topology-per-layer-scoping-an-eaa435`
**Epic:** `tk-epic-rfc-12711-qa-production-data-topology-imple-cab58f`
**RFC:** [mitodl/hq#12711](https://github.com/mitodl/hq/discussions/12711) — QA/Production data
topology, per-layer source scoping and union-completeness contracts
**Related RFC:** [mitodl/hq#12319](https://github.com/mitodl/hq/discussions/12319) — the YAML
ingestion inventory this spec extends
**Code pinned at:** `0ce61fb8cbc0ab51547980e263f9e9da23333cbe`

RFC 12711 chose Option 3 (per-layer scoping + declared union-completeness contracts) but left
two questions marked **Blocking**, one gating step 5 and one gating step 6. This document
resolves both, specifies the Local-1 per-environment strategy schema, and records a third
config defect of the same class that the RFC audit did not capture.

It does not revisit the Option 3 decision.

---

## 1. Resolved: mirror freshness semantics (RFC open question 1 — blocking step 6)

> *Does a mirrored singleton track production on a schedule, or is it pinned to a snapshot
> that changes only on explicit refresh? A moving mirror makes QA results irreproducible; a
> pinned one goes stale in the same way the current lake did.*

**Decision: the mirror is a materialized copy, refreshed only on explicit request, with a
declared maximum age that the union-completeness check asserts.**

### The RFC's dichotomy is false

"Pinned to a snapshot" is not an available option in the sense the question implies. Production
Iceberg snapshots are expired on a schedule — `RAW_LAYER_GROUP_CONFIGS` in
`packages/ol-orchestrate-lib/src/ol_orchestrate/lib/iceberg_maintenance.py:119-128` sets
3 days for the `raw__*__app` groups, 14 for `raw__thirdparty__salesforce` and
`raw__thirdparty__zendesk_support`, 7 by default, and `src/ol_dbt/dbt_project.yml:198-203`
sets `snapshot_retention_days: 7` for dbt-built models. A QA reference held against a pinned
production snapshot ID therefore dangles after 3–14 days. Iceberg time travel cannot express
the pin the question is asking for.

A physical CTAS copy into `ol_data_lake_qa` — which is what RFC step 6 already specifies — **is**
the pin, and it is the only durable one. So the real question is not "pinned or moving" but
"what refreshes the copy, and what makes its age visible".

### Refresh on request, not on a schedule

Three reasons, in priority order:

1. **Reproducibility.** A scheduled mirror means a QA result cannot be re-derived after the
   fact — the same query against the same declared topology returns different rows next week
   with nothing recording why. This is the failure the RFC names, and it is the more damaging
   one because it undermines the reason QA exists.
2. **PII surface.** Every refresh is another wholesale copy of production PII into a cluster
   with broader access. The RFC already calls the column allowlist "load-bearing for that risk,
   not optional"; minimizing copy *events* is the same argument applied to time. An
   on-request refresh is attributable to a person and a reason. A cron refresh is attributable
   to nobody.
3. **Nothing is bought.** The `singleton` set is precisely the slow-changing sources —
   `edxorg` (frozen), `emeritus` and `global_alumni` (quarterly-ish BigQuery drops),
   `salesforce`, `zendesk`. Production ingests them daily at best: every Airbyte
   connection group gets a schedule whose interval defaults to 24 hours
   (`dg_projects/lakehouse/lakehouse/definitions.py:258`), and only app-database and
   Open edX groups are given a faster override (`definitions.py:213-246` — `emeritus`
   and `ol_salesforce` appear there at 24h; `zendesk` and `global_alumni` are not
   listed at all and so take the default). A QA mirror tracking them hourly, or even
   daily, tracks noise.

### Staleness is answered by declaration, not by a schedule

The counter-risk is real — undetected staleness is exactly how the current QA lake died. But
the RFC's own thesis is that the fix for silent staleness is *declaring* the expectation and
*asserting* it, not adding a cron job that hides it. Applying that thesis to the mirror itself:

- The inventory entry for a `mirror` unit carries **`mirror_max_age_days`** (required; no default,
  so it is a decision per unit rather than an inherited accident).
- The mirror asset records copy-completion time as table metadata on the QA copy.
- The union-completeness check (§2) reads that stamp and reports a mirror older than its
  declared maximum as a **stale-mirror** finding, at the same severity as an empty branch.

A stale mirror thus fails the same way a missing branch fails — loudly, at build time, naming
the unit — rather than silently, which is the whole point of the RFC.

**Consequence for step 6:** the mirror asset is a plain manually-triggered Dagster asset. It
takes no partition definition and no schedule. `ol_data_lake_production` → `ol_data_lake_qa`
CTAS with the per-unit column allowlist, then a metadata stamp. Do not add
`AutomationCondition` to it.

---

## 2. Resolved: contract enforcement strength (RFC open question 5 — blocking step 5)

> *Should a declared-but-empty QA branch fail the build, or warn? Failing is safer but will
> block QA builds whenever any single upstream layer lapses, which given the observed failure
> rate may be frequent enough to encourage bypassing.*

**Decision: neither. A baselined ratchet — the pattern this repo already runs.**

`ol-dbt validate` has solved this exact tension once already.
`_check_dimensional_layering` (`src/ol_dbt_cli/ol_dbt_cli/commands/validate.py:568-619`)
treats new violations as `ERROR`, collapses known ones listed in
`dimensional_layering_baseline.txt` into a single `INFO` summary, and surfaces baseline
entries that no longer occur at `INFO` so the baseline gets shrunk. `--update-baseline`
(`validate.py:997-1004`) rewrites the file. `_check_pk_test_coverage` documents the same
intent explicitly: warn now, "promote to ERROR once the outstanding models are covered
(mirrors the dimensional_layering baseline approach)".

The RFC's worry — that hard failure "may encourage bypassing" — is precisely what the baseline
defuses. A bypass becomes a reviewable line in a committed text file rather than a habit of
skipping the check.

### Two findings, deliberately different severities

The question conflates two things that fail for different reasons and deserve different
treatment:

| Finding | Cause | Severity | Baselineable |
|---|---|---|---|
| Model declares a `qa_branch` absent from the inventory, or whose `strategies.qa` is `omit` | Declaration contradicts declaration | `ERROR` | **No** |
| Model declares a `qa_branch` whose strategy is `ingest`/`mirror`, but the unit is empty or the mirror is past `mirror_max_age_days` | Operational lapse upstream | `ERROR` if new, `INFO` if baselined | **Yes** |

The first is a spec bug. It is always fixable by editing text, needs no ingestion work, and
cannot be caused by an upstream outage — so there is no legitimate reason to tolerate it and
no bypass to accommodate. Making it unbaselineable keeps the two declarations honest against
each other, which the RFC identifies as "the actual deliverable".

The second is the case the RFC is worried about. Baselining it means a lapsed layer is
acknowledged in `qa_branch_baseline.txt` with a reviewable diff, QA builds keep running, and
the resolved-entry `INFO` nags the baseline back down as layers are repaired.

### Where it lives

A new `qa_branch_contract` check in `ol-dbt validate`, alongside checks 1–5. It already has
the `Severity` enum (`validate.py:58-61`), the error/warning/info split, `--format json` for
CI, and baseline load/write plumbing (`validate.py:1291-1374`). Step 5 wires a new check into
existing machinery rather than building a CI step from scratch.

**Consequence for step 4:** the per-model contract needs both halves of the distinction, so
`meta.qa_branches` entries name `(deployment, layer)` pairs — matching the inventory's key
exactly — and `meta.qa_buildable: false` suppresses the check entirely for models that have no
QA-buildable form. A model that is `qa_buildable: false` must not also declare `qa_branches`;
that combination is itself an unbaselineable `ERROR`.

### Step 4 as built (2026-09-16)

The keys live in `config.meta`. A model is a union, and so must declare one of them, when it
sits outside `staging/` and its manifest lineage reaches more than one inventory unit (a source
maps to the unit declaring its `raw_table`; retired and dbt-built sources map to none). The
`qa_branch_contract` check in `ol-dbt validate` enforces that, plus the shape rules above and one
more: a declared branch must be upstream of the model. It runs globally in `dbt_pr_ci.yaml`,
because a union gains a branch through an edit to an ancestor that `--changed-only` never selects.
Checking declarations against `strategies.qa` and against what QA holds is still step 5's, and
extends the same check.

The initial 168 declarations list the `scoped` units that can reach each model's output,
singletons excluded. Table lineage alone over-counts: `dim_contract` reads `dim_organization`
but keeps only `platform = 'mitxonline'`, so xPro rows never reach it. Each model's SQL was
read for its union arms, platform filters and platform-keyed joins (including joins on
platform-hashed keys such as `course_pk` and `instructor_pk`), and those facts were propagated
per platform through the DAG. That narrowed 25 models. A unit that only enriches columns still
counts: MicroMasters exam runs set semester and passing grade on MITx Online rows of
`dim_course_run`, so an empty MicroMasters branch changes that model's output. Joins the review
could not show to be platform-scoped were kept, so the lists err toward over-declaring. This is
the RFC's intended QA topology, not the measured one. Nearly every unit is still
`strategies.qa: omit`, so most declarations contradict the inventory today, on purpose: step 5's
unbaselineable finding is what forces step 3 to decide each branch, either by marking the unit
`ingest`/`mirror` from measured state or by dropping the branch from the models that declare it.

`irx/bigquery` was reclassified `scoped` → `singleton` in the same change. Its tables are
`raw__irx__edxorg__bigquery__*`, staged under `staging/edxorg`: edX.org data delivered through
IRx's BigQuery, with no QA counterpart. Left `scoped`, it would have been declared a QA branch
of 135 models.

### Step 5 as built (2026-09-18)

Both rows of the table above are now part of `qa_branch_contract`.

The first row needs only text. A declared branch that no unit declares, or whose unit is
`strategies.qa: omit`, is an ERROR on every model that declares it. After step 3 no declaration
hits this. Flipping `mitlearn/app_postgres` to `omit` fails 44 models.

The second row needs to know what QA holds, and the CI job has no AWS credentials. So
`ol-dbt inventory observe` reads the QA Glue database and each table's current Iceberg snapshot
(the same method as §7) for every table of an `ingest` or `mirror` unit, and writes
`ingestion/inventory/qa_observation.json`. That file is committed and CI reads it. A table counts
as empty when it is absent, not Iceberg, has no current snapshot, or has zero rows. A `mirror`
table is stale when its snapshot is older than `mirror_max_age_days` at observation time. Staleness
is measured against the observation time, not the CI run time, so an old observation can't
invent staleness. An observation older than 30 days is a WARNING. An observation of any database
other than `ol_warehouse_qa_raw` is an ERROR, because it would hide every gap.

A declared table the observation doesn't cover is an `unobserved` gap, not a warning. That is
what a branch looks like when a PR newly declares it, or flips its unit from `omit` to `ingest`,
which is when QA is least likely to hold it. The PR either refreshes the observation or baselines
the gap, and both are visible in review.

Only tables that a declaring model reads count, through manifest lineage. The baseline
(`ingestion/inventory/qa_branch_baseline.txt`) is keyed per table, not per branch: a model that
starts reading an empty table of an already-baselined branch is a new finding. New findings are
reported as one ERROR per branch. `ol-dbt validate --update-qa-baseline` rewrites the baseline.

The first observation (2026-09-18) gives 48 baselined tables across 14 branches, all `empty`.
No `stale` finding can fire yet, because step 4 declared only scoped units and no model declares
a mirror branch. That changes once step 6 lands mirrors and models start declaring them.

The observation is refreshed by hand for now. A refresh that shows a table has emptied fails the
refresh PR, which is where the lapse should surface.

---

## 3. Specified: per-environment strategy map (task Local-1)

Amends RFC step 2 before the inventory schema is finalized, so `local` is a first-class
environment rather than a retrofit. Per-entry shape:

```yaml
- deployment: mitxonline          # mitx | mitxonline | xpro | mitlearn | edxorg | ...
  layer: app_postgres             # mysql | mongodb | api | tracking_logs | fastly | app_postgres
  scope: scoped                   # scoped | singleton
  strategies:
    qa: ingest                    # ingest | mirror | omit
    local: fixture                # ingest | fixture | omit
  mirror_max_age_days: 30         # required iff any strategy is `mirror`
```

Three schema rules, each resolving a question Local-1 left open:

**`mirror` is not a legal `local` strategy** — rejected by the schema, not merely discouraged.
A laptop has no Vault path and no VPN route to a production read replica, and there should not
be one, so `local: mirror` is unimplementable rather than merely unwise. Encoding it as an
enum that omits the value answers the question once, in one place, instead of re-litigating it
per entry in review.

**`local: ingest` requires the unit to be dlt-backed** — a validator rule, not a Step 5
cross-check. Airbyte cannot run in k3d, so `local: ingest` on an Airbyte-only unit is false on
its face. This belongs at inventory-parse time because it is a property of the unit itself; it
should fail even when no model references the unit yet. Step 5's cross-check answers a
different question (does a *model's* declaration match the inventory).

**Singletons get `fixture` locally, and that is the intended forcing function.** With `mirror`
illegal and `ingest` impossible for a source with no local counterpart, `edxorg`, `emeritus`,
`global_alumni`, `salesforce`, and `zendesk` have exactly one legal local strategy. Local is a
cleaner forcing function for declaring seed data than QA was, because it has no production
escape hatch to fall back on.

---

## 4. New finding: the QA/production seam is a substring test on a target name

The RFC records two config defects. There is a third of the same class, and it widens defect 1
beyond what the RFC states.

`src/ol_dbt/models/b2b_analytics/_b2b_analytics__sources.yml:9-12` selects its source catalog
and schema with `{{ 'ol_data_lake_qa' if 'qa' in target.name else 'ol_data_lake_production' }}`
— a substring test. `STARROCKS_DBT_TARGET_MAP` maps **`"dev": "starrocks_qa_vault"`**
(`dg_projects/lakehouse/lakehouse/assets/lakehouse/dbt_starrocks.py:35-42`), and
`"starrocks_qa_vault"` contains `"qa"`. So a developer's local b2b build also reads the empty
`ol_data_lake_qa`, not just the QA deployment. The B2B models cannot be developed locally today
for the same reason they 500 in RC.

The root cause is that one string is being asked to answer two independent questions:

- **Which cluster do I connect to?** (`dev` → the QA StarRocks cluster, via port-forward)
- **Which catalog do I read?** (`dev` → production data, per the RFC's own observation that
  developing against production is the sanctioned working practice)

These genuinely differ for `dev`, which is why the substring test produces a wrong answer there.

**Consequence for step 1:** reconciling the two target maps is necessary but not sufficient.
Step 1 must also replace the substring test with an explicit variable — the source catalog
becomes a dbt var (e.g. `data_lake_env`) set per target, rather than inferred from the target's
name. Reconciling the maps while leaving `'qa' in target.name` in place fixes the QA deployment
and leaves `dev` broken.

### Direction for step 1

Map `qa` explicitly on both sides, to QA:

- `dbt.py:49-53` — add `"qa": "qa"` to the map and **remove the `default=` fall-through**,
  raising on an unrecognized `DAGSTER_ENVIRONMENT` instead. The fall-through is the actual
  defect; a future environment must not silently inherit production the way `qa` did.
- `dbt_starrocks.py:35-42` — already maps `"qa": "starrocks_qa_vault"`; apply the same
  no-default treatment, and set `data_lake_env` per target so `dev` reads production.

Two things to confirm before landing, neither visible from the repo:

1. `dbt_automation_sensor` (`dg_projects/lakehouse/lakehouse/definitions.py:441-453`) is
   defined with **no environment gate**. Combined with today's `default="production"`, a QA
   lakehouse code location running that sensor materializes dbt assets against the production
   Trino warehouse. Whether the sensor is actually running in the QA Dagster deployment is an
   instance setting and must be checked in the UI — if it is, this is a live production-write
   path from QA, and it is more urgent than the B2B failure that prompted the RFC.
2. QA dbt builds will produce empty or partial output until step 8 re-establishes the QA
   `app_postgres` layer. That is expected and is exactly the state the §2 contract is designed
   to make loud. Land step 1 with the baseline seeded from the measured gaps, not with QA
   builds expected to be green.

---

## 5. Remaining open questions (all non-blocking, unchanged)

RFC 12711's other three open questions are not resolved here and do not gate any step:

- **QA raw cleanup** (2,738 QA tables vs production's 2,090) — determines whether the inventory
  can be read as a completeness statement. Recommend deciding it during step 3, when the
  measured state is being transcribed anyway.
- **Per-user development schemas** (~40 `ol_warehouse_production_<username>_*`) — §3's
  `fixture`-only local topology is the constructive alternative, so this is best revisited
  after the Local-1..Local-5 chain lands rather than now.
- **`mitxresidential` scoping** — untouched by the pilot; needs a data-representativeness
  check on the QA Open edX deployment before `mitx` units can be marked `qa: ingest`.

---

## 6. Step status after this spec

| Step | Gate | Status |
|---|---|---|
| 1 — reconcile dbt targets | none | Ready; scope widened by §4 |
| 2 — inventory schema | RFC 12319 Terraform provider task | Blocked; §3 amends its schema |
| 3 — populate inventory | step 2 | Blocked |
| 4 — per-model contract | step 2 | Blocked; shape fixed by §2 |
| 5 — cross-check in CI | **was** open question 5 | **Unblocked by §2** |
| 6 — singleton mirror asset | **was** open question 1 | **Unblocked by §1** |
| 7 — narrow StarRocks grant | step 6 | Blocked |
| 8 — B2B pilot | step 1 | Blocked on step 1 only |
| 9 — engagement path | steps 5, 8 | Blocked |

---

## 7. Measured QA state and the strategies it set (step 3, 2026-09-17)

Every unit's `strategies.qa` now follows one rule:

- `ingest`: the unit is `scoped` and at least one model declares it in `meta.qa_branches`.
  keycloak is `ingest` too, since a dlt loader already writes it in QA.
- `mirror` with `mirror_max_age_days: 90`: the unit is a `singleton` with modeled tables. 90 days
  fits the quarterly partner drops; edxorg is frozen.
- `omit`: every other unit. No union model declares these, so they can't break a contract.

`ol-dbt inventory validate` now rejects `scoped` + `mirror` and `singleton` + `ingest`.

Most `ingest` and every `mirror` unit is empty or stale in QA. That's the gap step 5 baselines
(§2's second row), not a reason to mark the unit `omit`. Marking it `omit` would turn the `qa_branches`
declarations on 165 models into unbaselineable errors.

### How it was measured

- **Presence:** `aws glue get-tables` on `ol_warehouse_qa_raw` (2,766 tables) and
  `ol_warehouse_production_raw` (2,087), joined case-insensitively to each unit's
  `tables[].raw_table`. Glue stores names lowercased, and salesforce's inventory names are not.
- **Non-empty and snapshot date:** the current snapshot in each table's Iceberg
  `metadata_location` file, using `total-records` and `timestamp-ms`. Glue `UpdateTime` is
  useless here. 2,354 QA tables carry a 2026-09-08 `UpdateTime`, the date of the QA
  JSONL→Iceberg conversion (step 8), and 190 more carry 09-11 or 09-14. The conversion also wrote
  snapshot dates for the tables that held data. So a 2026-09-08 to 09-14 snapshot on
  a converted table dates the conversion, not the data. Converted empty shells have no snapshot
  at all.
- **QA loaders:** `bin/airbyte-inventory.py dump --environment qa`. The workspace has 28
  connections, 9 marked active. The public jobs API returns no job history for any of them. Only
  five target the Iceberg `S3 Data Lake` destination (xPro app DB, Bootcamps app DB, and the
  edxorg/Open edX course XML and API connections). The rest write the legacy JSONL Glue
  destination that neither StarRocks nor dlt can read. The QA Dagster sync schedules are stopped
  (step 8). So the only QA writers are the two dlt units.

### `ingest` (18)

"Present" counts tables in QA Glue out of those declared. "Non-empty" counts tables with a snapshot holding rows.

| Unit | Present | Non-empty | Modeled non-empty | QA loader | Gap |
|---|---|---|---|---|---|
| keycloak/app_postgres | 14/14 | 14 | — | dlt, writing 2026-09-17 | none |
| mitxonline/app_postgres | 64/64 | 64 | 50/50 | dlt, run by hand 2026-09-08; schedule stopped | not scheduled; unit still says `loader: airbyte` |
| micromasters/app_postgres | 39/39 | 39 | 28/28 | Airbyte, inactive, legacy destination | converted legacy data, no loader |
| xpro/app_postgres | 178/178 | 159 | 54/55 | Airbyte, active, Iceberg destination, no jobs | newest snapshot 2025-02-25 |
| ocw/app_postgres | 32/34 | 20 | 2/3 | Airbyte, inactive, legacy destination | converted legacy data, no loader |
| ovs/app_postgres | 25/26 | 0 | 0/6 | Airbyte, inactive, legacy destination | empty |
| mitx/mysql | 56/74 | 8 | 7/61 | Airbyte, inactive, legacy destination | nearly empty |
| mitxonline/mysql | 55/68 | 0 | 0/56 | Airbyte, active, legacy destination, no jobs | empty |
| xpro/mysql | 57/70 | 0 | 0/54 | Airbyte, inactive, legacy destination | empty |
| mitx/tracking_logs | 1/1 | not Iceberg | 0/1 | Airbyte, inactive | legacy JSON, data 2024-08-28 |
| mitxonline/tracking_logs | 1/1 | not Iceberg | 0/1 | Airbyte, inactive | legacy JSON, data 2024-08-28 |
| xpro/tracking_logs | 1/1 | not Iceberg | 0/1 | Airbyte, inactive | legacy JSON, data 2024-08-27 |
| mitx/api | 0/2 | 0 | 0/1 | none | never ingested |
| mitxonline/api | 0/2 | 0 | 0/2 | none | never ingested |
| xpro/api | 0/2 | 0 | 0/2 | none | never ingested |
| mitlearn/app_postgres | 0/98 | 0 | 0/9 | none | never ingested |
| learn_ai/app_postgres | 0/28 | 0 | 0/5 | none | never ingested |
| openedx/s3 | 0/1 | 0 | 0/1 | none | also absent from production raw |

### `mirror`, 90 days (11)

The step 6 mirror asset doesn't exist yet, so none of these has a mirror. What QA holds for
edxorg/s3 (2/6, non-empty) and irx/bigquery (4/4, legacy JSON) are pre-existing copies, not mirrors.

| Unit | Production present | Production newest snapshot |
|---|---|---|
| edxorg/api | 0/1 | absent |
| edxorg/course_structure | 4/5 | 2026-08-14 |
| edxorg/google_sheets | 1/1 | 2026-09-17 |
| edxorg/mysql | 3/7 | 2026-09-17 |
| edxorg/s3 | 6/6 | 2026-09-17 |
| edxorg/tracking_logs | 1/1 | 2026-09-14 |
| emeritus/bigquery | 1/1 | 2026-09-17 |
| global_alumni/bigquery | 1/1 | 2026-05-04 |
| irx/bigquery | 4/4 | 2026-09-17 |
| salesforce/api | 3/3 | 2026-06-16 |
| zendesk/api | 26/26 | 2026-09-17 |

A mirror copies production, so `mirror_max_age_days` measures time since the copy, not source
freshness. global_alumni and salesforce are already months stale in production.

### `omit` (14)

bootcamps/hubspot, mailgun/api, mit_climate/api, mitpe/api, mitx/mongodb, mitxonline/mongodb,
xpro/mongodb, mitxonline/hubspot, xpro/hubspot, mitxonline/openedx_notes, oll/google_sheets,
open_discussions/app_postgres, podcast/rss, posthog/s3.

QA holds converted forum data for the three mongodb units (3 non-empty tables each). Nothing
reads it. Scope for mailgun, posthog, podcast, oll, mit_climate, mitpe and bootcamps/hubspot is
still unaudited. It doesn't affect anything until a model declares one of them.

### QA raw cleanup (§5)

The 43 units own 538 of QA raw's 2,766 tables. The other 2,228 belong to no unit, so no
`qa_branches` contract can depend on them. Whether a dbt source or anything outside dbt still
reads them was not checked here. That check is what the cleanup decision still needs.

---

## 8. The singleton mirror (step 6, 2026-09-18)

### It runs from production, not QA

The RFC assumed the QA StarRocks cluster would read production through the catalog it already
carries. That is no longer true. Since ol-infrastructure #5472 (2026-08-17) and #5670
(2026-08-31), the QA StarRocks IRSA role carries
`data-lake-cross-environment-glue-denial-policy-qa`. On 2026-09-18 the IAM policy simulator
returned `explicitDeny` for `glue:GetTable` on `ol_warehouse_production_raw` tables from
`data-qa-starrocks-lakehouse-trust-role`. The production role was allowed to read production
and to create and drop tables in `ol_warehouse_qa_raw`.

So the mirror runs on production StarRocks, from the production lakehouse code location. The
assets are registered only when `DAGSTER_ENV == "production"`. Production pushes an allowlisted
subset into QA, and QA never reads production. That changes step 7. The QA cluster has no
mirror role to narrow a grant to, and its production catalog is already unusable at the IAM
layer. What is left is to drop `ol_data_lake_production` from the QA cluster's
`_DATA_LAKE_ENVS`, so the SQL grants stop advertising access IAM denies.

### The declaration

Each mirrored table carries a `mirror:` block in its unit file:

```yaml
- name: api_enrollments
  raw_table: raw__emeritus__bigquery__api_enrollments
  mirror:
    columns:
      _airbyte_extracted_at: copy
      email: hash
      first_name: redact
      batch_id: copy
    where: "..."        # optional
```

`columns` is the allowlist. A production column it does not name is not copied. The modes:

- `copy` keeps the value.
- `hash` writes `sha2(nullif(value, ''), 256)`. It keeps a key distinct and joinable within
  mirrored data. It is pseudonymization, not anonymization: an unsalted digest of a known email
  can be matched, and a digest of a first name is trivial to reverse. Blanks become NULL, or
  every blank username would share one digest.
- `redact` writes the literal `'redacted'` where the value is not NULL. It carries no
  information and keeps the column's NULLs where production has them, so a `not_null` test
  fails in QA exactly when it would in production.
- `nullify` keeps the column with every value NULL, typed through a dead `CASE` branch so QA
  gets production's type. For non-string and JSON-shaped columns, where a literal would break a
  cast or a JSON parse.

`hash` and `redact` take string columns only, so a cast in a staging model never meets a hex
digest or the word `redacted`. Staging models select PII columns by name, so a dropped column
fails the QA build. A nullified column builds, but fails any `not_null` test on it, which is
why string PII uses `redact`.

`where` is one predicate. `{source}` stands for the production relation, so a filter can be
measured from the table's own newest row. A paused feed then still copies rows, which a filter
on `now()` would not.

`ol-dbt inventory validate` rejects a `mirror:` block on a unit whose `strategies.qa` is not
`mirror`, a `where` containing `;` or a set operator (a `UNION` there could read production
columns the allowlist leaves out), and an allowlist without the table's resolved raw metadata
column. The dedup macro reads that column through the inventory, so no analysis of the model's
SQL sees the read.

The asset checks every table's declaration in the unit against `DESCRIBE` of the production
table before it drops any QA copy. An allowlisted column that production lacks, or `hash` or
`redact` on a non-string column, fails the run with the whole unit's QA copies still in place.
A `where` or CTAS that fails at run time is different: it fails after that table's `DROP`, so
the unit is left partly refreshed and the failed table absent.

### What was not built

A static check that no model reads a column the mirror drops. The sqlglot scope resolution in
`ol-dbt validate` expands `select *` to the whole schema it is given, so it cannot see a
dropped column. Strict qualification against the mirrored schema fails on 29 of the 32 models
that read a mirrored table even when given the full production schema, because it cannot see
through the dedup macro's CTE or other macro calls. A dropped column instead fails the QA dbt
build of that staging model with the column's name, which is loud and in the right place.

### The allowlists

25 tables across 9 units. Each allowlist is the columns the reading models name, found by
text-matching the production column list against each model and the macros it calls, plus the
raw metadata column. Unread columns are dropped, which is how `mitx_person_course`'s `ip`,
`city`, `postalcode` and coordinates never reach QA. Read columns that identify a person are
masked:

- `hash`: emails, usernames, tracking-log session ids, and the certificate key and uuids in
  `mitx_user_info_combo` (edX's public certificate pages, keyed by those, show the learner's
  name).
- `redact`: names, street address, city, zip code, job title and company, phone, alias,
  signature and zendesk user `details`, profile goals and mailing address, certificate name,
  zendesk organization and user notes, salesforce `nextstep` and line-item `description`.
- `nullify`: IP, year of birth, certificate download URLs, and the JSON-shaped `profile_meta`,
  zendesk user `photo` and `user_fields`.

Two tables are filtered:

- `raw__edxorg__program_learner_report`: 14.4B rows, 760 GB. Airbyte re-reads the same report
  files every day (their mtimes stop at 2025-03-13) and appends about 15M rows per sync. The
  mirror keeps the last day of syncs by `_airbyte_extracted_at` (epoch milliseconds). That is
  one full report while syncs run a day apart, and two if a gap is shorter. The staging model
  dedupes on user, course run and program either way.
- `raw__edxorg__s3__tracking_logs`: 2.2B rows, 245 GB. The mirror keeps 30 days of syncs and
  drops `edx.user.settings.changed` events.

Tracking-log `event` and `context` payloads are copied as they are, because the staging model
parses them. `edx.user.settings.changed` is excluded because edx-platform logs the old and new
email, name and address in its payload, and no model reads it. Forum events carry post bodies
and are kept, because `tfact_discussion_events` reads them. That text is copied into QA. The
30-day filter bounds how much lands, but no column mode can mask inside a JSON payload.

Not declared, so not mirrored:

- edxorg/mysql's `auth_user`, `certificates_generatedcertificate` and
  `grades_persistentcoursegrade`. They resolve to `_file_modified_at`, which production does
  not have yet (ol-data-platform#2443).
- Tables absent from production raw: `raw__edxorg__discovery__api__programs`,
  `raw__edxorg__s3__course_xml_blocks`, and edxorg/mysql's `auth_userprofile`,
  `courseware_studentmodule`, `student_courseenrollment` and `student_courseaccessrole`.
- Unmodeled tables (most of zendesk, salesforce `Account`), which nothing reads.
- zendesk `tickets` and `ticket_comments`. Their `via` JSON holds the requester's email at
  `$.source.from.address`, and the staging models parse `$.channel` from it into a column with
  a `not_null` test. `copy` leaks the email, and `redact` or `nullify` fail the test. A mode that
  keeps named JSON keys would fix both, and is left as a follow-up.

The salesforce `Opportunity` and `OpportunityLineItem` tables declared `_airbyte_emitted_at`
as their raw metadata column, as if they were still on Airbyte's v1 destination. Production
Glue shows the v2 columns and no `_airbyte_emitted_at`. The override is removed. Neither staging
model read it, since both order by `systemmodstamp`.

### Refresh and staleness

Each unit is one asset, `qa_mirror/<deployment>/<layer>`, materialized by hand. There is no
schedule, no partitions and no `AutomationCondition` (§1). A refresh drops each QA table with
`FORCE`, which deletes its data files, then runs the CTAS. StarRocks cannot rename an Iceberg
table, so there is no swap. A CTAS that fails inside StarRocks drops the table it created
(`StmtExecutor.handleCreateTableAsSelectStmt` on branch-4.1), and the next QA observation
reports it as empty. The CTAS is never retried by the StarRocks
resource: an FE lost mid-statement can leave the table behind, and a retry would fail on
"already exists" and hide the real error. The next manual run's `DROP` clears it.

The `DROP` goes through the Iceberg catalog, so it cannot remove a Glue entry that is not an
Iceberg table, and the CTAS then fails on the name. One mirrored name had such an entry in QA:
`raw__irx__edxorg__bigquery__email_opt_in`, a legacy JSON table last written 2024-08-26. Its Glue
entry was deleted on 2026-09-19. The JSON files under
`s3://ol-data-lake-raw-qa/raw/irx/edxorg/bigquery/email_opt_in/` were left in place.

§1 called for a copy-time stamp in table metadata. The CTAS writes a single snapshot, and
`ol-dbt inventory observe` already reads that snapshot's time as the copy time. So no separate
property is written.

### Checked on QA StarRocks (2026-09-19)

The rendered SQL was run QA-to-QA, from `raw__edxorg__s3__mitx_course` into a scratch table,
through the `admin` Vault role the production resource uses:

- `DESCRIBE` on an Iceberg table returns `Field` and `Type`, which the asset reads.
- The CTAS with the `SET_VAR(query_timeout, insert_timeout)` hint and a `{source}` subquery in
  its `WHERE` created the table and copied all 448 rows as one `append` snapshot.
- A nullified `BIGINT` column stayed `BIGINT` with no non-NULL values, and a hashed column was
  64-character hex in every row.
- `DROP TABLE ... FORCE` removed the Glue entry and the data files. It left a zero-byte
  `data/load_spill/` marker, and because `ol-data-lake-raw-qa` is versioned the dropped files
  remain as noncurrent versions until the bucket's 90-day `expire-noncurrent-versions` rule
  removes them. So each refresh keeps the previous copy billed for up to 90 days.
