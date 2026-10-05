# Business metrics layer: technical spec

**Status:** Spec (draft)
**Project:** `wp-business-metrics-consolidated-definitions-and-me-cdf8b8`
**Decision record:** `tk-decide-the-metrics-layer-shape-and-where-metric--0de542` (closed 2026-10-05)
**Repos:** ol-data-platform (definitions, registry, tooling), ol-analytics-api (consumer),
ol-infrastructure (OpenMetadata)

---

## 1. Problem and goal

The same business metric is computed in several places, with nothing keeping the copies
in step. Some already disagree:

- "Certified learners" is `certificate_count > 0` from the administration report in
  `mv_b2b_monthly_engagement_trend`, and `certificate_is_revoked = false` from
  `tfact_certificate` in `mv_b2b_enrollment_completion_funnel`.
- Completion rate is over enrolled learners in `mv_b2b_enrollment_completion_funnel` and
  over seats consumed in `mv_b2b_mit_admin_contract_health`.

Others agree today only because they were copied by hand. The completion-status rule is
written three times: twice in ol-analytics-api (`tenants/b2b_dashboard/learner_queries.py`,
`tenants/b2b_learner_records/queries.py`) and once as the `courses_in_progress` predicate
in `mv_b2b_learner.sql`. The needs-attention rule and its 30-day constant exist only in
API Python.

The discovery inventory (2026-10-05, recorded in the project's tasks listed in §7) also
found four predicates for "certificate earned" across the dbt project, a different
"passing" rule per platform, and 69 distinct ad hoc SQL metric expressions in Superset
charts. OpenMetadata had no Metric or glossary entities when measured on the same day.

Goal: each business metric has one definition, computed in dbt, that every consumer reads,
and one registry entry that names it, describes it and points at the column that
implements it.

### Non-goals

- No query-time semantic engine. MetricFlow and Cube were considered and rejected (§2).
- No change to ol-analytics-api's tenant auth, consent handling or k-anonymity suppression.
- No new data quality tooling. dbt tests and dbt unit tests cover verification.
- The OpenMetadata governance model (teams, reviewers, domains) is decided in
  `wp-openmetadata-governance-data-contracts-and-platf-7c77ee`, not here.

---

## 2. Decisions

| Decision | Reason |
| --- | --- |
| Definitions are materialized dbt columns, not generated SQL | The consumers are a StarRocks-backed API, Superset on Trino and ad hoc SQL. Tables are the only thing all three share. |
| Row-level rules live in the dimensional Iceberg layer | Trino builds it and StarRocks reads it through `source('dimensional', ...)`, so both engines see one value. |
| Time-relative rules are published as a threshold date | A boolean computed at build time freezes "today" at the last refresh. The consumer compares the date against its own today. |
| The registry is `metrics/*.yaml`, not dbt `semantic_models` / `metrics` | See below. |
| Definitions are reviewed in the pull request; OpenMetadata displays the result | Git stays authoritative, the same as `contracts/`. |

Why not dbt's metric YAML. Tested on dbt-core 1.12.5 with a one-metric project:
`dbt parse` fails without a time spine model, fails unless every measure has an
aggregation time dimension, and rejects metric names containing `__`. A metric YAML
mistake is a parse error, which stops every dbt command. It cannot describe a metric
implemented by a StarRocks view column. Read from the OpenMetadata 2.0.2 source, not
tested: its dbt ingestion writes metrics with a bot PUT and the server keeps an existing
non-empty description (`EntityRepository.updateDescription`), so a description changed in
git would not reach OpenMetadata after the first ingest.

Why not MetricFlow with a StarRocks renderer. The renderer is a small patch (the Trino one
in metricflow 0.213.0 is 157 lines) but open-source MetricFlow is a CLI and library. We
would still have to embed it in the API to generate SQL per request and find a separate
route for Superset.

---

## 3. Shape

```
staging / intermediate
        |
dimensional (Iceberg, built by Trino)
  tfact_*, dim_*, bridge_*           existing facts
  afact_learner_courserun_progress   NEW: row-level outcome rules as columns
        |
        +-- Trino / Superset / marts read the columns directly
        |
        +-- StarRocks views (b2b_analytics, b2b_learner_records) select the columns
                |
                +-- ol-analytics-api reads the views, restates no rule

metrics/*.yaml  --validate (PR CI)-->  project YAML and SQL
                --sync (post-build)-->  OpenMetadata Metric + lineage
```

Three kinds of definition, and where each goes:

- Row-level rule (a status or flag on one entity): a column on a dimensional model.
- Aggregate (a count or rate at some grain): a dbt model or StarRocks view that aggregates
  the row-level columns. It may filter its population (e.g. active enrollments only) but
  does not restate an outcome rule.
- Time-relative rule: a threshold date column, named `<rule>_since`.

---

## 4. Phase 1: learner course-run progress

This phase needs no registry and no OpenMetadata work.

### 4.1 New model `afact_learner_courserun_progress`

Location `src/ol_dbt/models/dimensional/`. Grain: one row per (user, course run).

Sources: `tfact_enrollment`, `tfact_grade`, `tfact_certificate`,
`afact_learner_courserun_daily_activity`, `dim_date`.

Population: `tfact_enrollment` rows with `enrollment_type = 'course'` and non-null
`user_fk` and `courserun_fk`. The fact's own grain is one row per enrollment, tested
unique on (enrollment_id, platform, enrollment_type), and both foreign keys are nullable,
so the model must pick one enrollment per (user, course run) when more than one exists.
The pick rule is an open question (§9).

Certificates: `tfact_certificate` already keeps one row per (user, course run, scope) per
build when both keys resolve (`cross_source_deduped`). The model joins it on
`certificate_scope = 'course'` without reducing it again. Two limits follow, both owned by
the certificate task in §7 and not fixed here: when a learner has a revoked and a reissued
certificate on one platform, the surviving row is arbitrary; and incremental runs can leave
more than one row, which this model's uniqueness test will surface.

| Column | Definition |
| --- | --- |
| `user_fk`, `courserun_fk`, `platform` | Keys, from `tfact_enrollment`. |
| `enrollment_created_on`, `enrollment_updated_on`, `enrollment_is_active`, `enrollment_mode`, `enrollment_status` | Carried from `tfact_enrollment`. |
| `is_passing`, `grade_value`, `letter_grade`, `grade_updated_on` | Carried from `tfact_grade`. |
| `certificate_is_revoked`, `certificate_issued_on`, `certificate_updated_on` | Carried from `tfact_certificate` unchanged, revoked or not. `certificate_is_revoked` stays three-valued: null means no certificate. |
| `is_certified` | `coalesce(certificate_is_revoked = false, false)`. Never null. |
| `last_active_on` | DATE of the latest row in `afact_learner_courserun_daily_activity`. |
| `completion_status` | `certified` when `is_certified`; else `passed` when `is_passing`; else `in_progress` when `grade_value > 0` or `last_active_on` is not null; else `not_started`. |
| `is_in_progress`, `is_not_started` | `completion_status` equals that value. Never null. |
| `needs_attention_since` | DATE. See 4.2. |

`completion_status` is the API's current `_COMPLETION_STATUS` CASE unchanged, including
that an unrevoked certificate is `certified` without requiring `is_passing`
(ol-data-platform#2669) and that `in_progress` needs a nonzero grade or tracked activity
(ol-data-platform#2693).

There is no `is_passed` flag. `is_passing` (the grade's own flag, true for certified
learners too) is what `courses_passed` and `passing_learners` count today, and a second
flag meaning "passing and not certified" would invite using the wrong one. The `passed`
status bucket is `completion_status = 'passed'`.

Platform scope: the model covers every platform in `tfact_enrollment`, with one rule for
all of them. The rule was written for MITx Online B2B and its inputs are weaker elsewhere.
The model's YAML description states these gaps:

| Platform | Grades | Certificates | Activity | Reachable statuses |
| --- | --- | --- | --- | --- |
| mitxonline, mitxpro | yes | yes | yes | all four |
| edxorg | yes | yes, never marked revoked | yes | all four |
| residential | no | no | yes | `in_progress`, `not_started` |
| bootcamps | no | yes | no | `certified`, `not_started` |

Every uncertified bootcamps enrollment is therefore `not_started` and gets a
`needs_attention_since`. The gaps are closed in the inputs by the passing and certificate
tasks in §7, not by platform branches in this model.

Access: the model keeps the `dimensional` layer's grants. Its inputs (enrollments, grades,
certificates, `dim_user`) are already readable by those roles, so it exposes no new source
data, only the derived status and needs-attention date. Consent is not applied in the
dimensional layer. It is applied where the model is joined to consent for a consumer: the
mart, reporting and integration layers. The B2B path already works this way: the StarRocks
views join consent and emit `outcomes_shared`, and the API withholds outcomes on it. A new
consumer-facing model built on this one must carry or apply consent itself.

Tests: `unique_combination_of_columns` on (user_fk, courserun_fk), `not_null` on both,
`accepted_values` on `completion_status`, and dbt unit tests (run by `ol-dbt unit-test`)
covering each status branch, a revoked certificate with and without a passing grade, and
each row of the `needs_attention_since` table.

### 4.2 `needs_attention_since`

The rule is the one in ol-analytics-api PR #87: a learner needs attention if they never
started, or if they are in progress and their last recorded activity was at least N days
ago. A learner who is currently `passed` or `certified` never needs attention, because
going quiet after finishing is expected. (API `main` applies staleness to every status;
that version is not carried over.)

| `completion_status` | `needs_attention_since` |
| --- | --- |
| `not_started` | enrollment date, or `1970-01-01` when the enrollment has no created date |
| `in_progress` with a `last_active_on` | `last_active_on` + N days |
| `in_progress` with a grade but no tracked activity | null |
| `passed`, `certified` | null |

The consumer's predicate is `coalesce(needs_attention_since <= <today>, false)`. N is a dbt
var, `needs_attention_quiet_days`, default 30, replacing `NEEDS_ATTENTION_QUIET_DAYS`.

#87 flags `not_started` unconditionally. The fallback date keeps that true when
`enrollment_created_on` is null, which the fact allows. Which "today" the consumer uses
also matters for this row and is an open question (§9).

The column follows the row's current status, as #87 does. A learner whose certificate is
revoked and who is not passing is `in_progress` again on the next build and gets a
threshold date again.

### 4.3 StarRocks views

No existing view column is removed or changes definition. The change adds columns and
moves where the inputs are read from.

- Add `afact_learner_courserun_progress` to the `dimensional` source in
  `b2b_analytics/_b2b_analytics__sources.yml`.
- `mv_b2b_learner_enrollment` replaces its `tfact_enrollment`, `tfact_grade`,
  `tfact_certificate` and last-active joins with one join to the progress model. It keeps
  every column it has today, including `certificate_is_revoked` and `certificate_issued_on`,
  which the API publishes, and adds `completion_status`, `is_certified`, `is_in_progress`,
  `is_not_started` and `needs_attention_since`.
- `record_updated_on` stays computed in the view, from the model's enrollment, grade and
  certificate timestamps plus `consent_modified_at`. It is the API's `updated_since`
  cursor, and consent is at the view's (learner, contract) grain, not the model's.
- The engagement counters (`days_active`, `videos_played`, ...) stay on the activity fact
  until the engagement task consolidates them.
- `mv_b2b_learner` reads the same model. Both views read Iceberg, so the name-order refresh
  constraint that stops one view selecting from the other does not apply. Its counters keep
  their current meaning: `courses_passed` counts `is_passing`, `courses_certified` counts
  `is_certified`, and `courses_in_progress` counts `enrollment_is_active and is_in_progress`.
- `mv_b2b_enrollment_completion_funnel` moves `passing_learners` to `is_passing` and
  `certified_learners` to `is_certified` in the same change. The views that take certified
  counts from the administration report move with the engagement task, since that is where
  their numbers change.

Build order: the views read the new table through the Iceberg source, and adding view
columns makes the Dagster asset escalate the StarRocks build to `--full-refresh`, which
recreates the views (`dbt_project.yml`, `b2b_analytics` block). The Trino build of the
progress model must therefore have run in a lake before the view change reaches the
StarRocks build that reads that lake. That holds for QA as well as production, because the
`ci` environment reads the QA lake. Ship the model in one pull request and the view change
in a second, merged after the model exists in both lakes.

### 4.4 ol-analytics-api

After the views are rebuilt in production:

- Delete both `_COMPLETION_STATUS` definitions and select the column.
- `_needs_attention(cutoff)` becomes `COALESCE(needs_attention_since <= <cutoff>, FALSE)`.
  `NEEDS_ATTENTION_QUIET_DAYS` and the `DATE_SUB` in `NEEDS_ATTENTION_CUTOFF_QUERY` are
  removed; the query resolves today's date only.
- `_recomputed_learners` keeps its shape and its `include_inactive` / `contract_id`
  filters, and counts the flags in place of the status CASE.

The API must deploy after the view build, or its SELECT fails on an unknown column. The
view change only adds columns, so the old API keeps working against the new views.

### 4.5 Verification

- Before switching the views: on QA StarRocks, compare `completion_status` from the new
  model against the API's CASE evaluated over the current `mv_b2b_learner_enrollment`, row
  by row on (user, course run). Any difference blocks the change until explained.
- For the counters: before the view change deploys, copy `mv_b2b_learner` and
  `mv_b2b_enrollment_completion_funnel` to scratch tables on QA StarRocks, then compare
  the rebuilt views to the copies with SQL. `ol-dbt diff` runs on DuckDB, where these
  models are disabled, so it does not apply here.
- The API's `test_needs_attention_boundary_is_computed_from_real_rows` is rewritten against
  the new column and must still pin the 30th day as included.

---

## 5. Phase 2: metric registry and PR validation

### 5.1 File format

One file per metric under `metrics/`, mirroring `contracts/`:

```yaml
metric:                         # OpenMetadata CreateMetric body
  name: learner_completion_status
  displayName: Course completion status
  description: >
    Where a learner stands in a course run: certified, passed, in_progress or not_started.
  metricType: OTHER
  owners: [{type: team, name: <team>}]        # names come from the governance decision
  reviewers: [{type: team, name: <team>}]
  glossaryTerms: [<glossary>.<term>]
status: Approved                # entityStatus; applied by PATCH, not part of CreateMetric
implemented_by:
  - dbt_model: afact_learner_courserun_progress
    columns: [completion_status]
  - dbt_model: mv_b2b_learner_enrollment
    columns: [completion_status]
```

- `metric` uses OpenMetadata's field names so the file needs no translation layer.
  `metricType`, `unitOfMeasurement` and `granularity` take OpenMetadata's enum values.
- `implemented_by` is ours. Each entry is a `dbt_model` (or `fqn` for an entity dbt does
  not build) and the columns that carry the metric. The first entry is the defining
  implementation; later entries are places it is served from.
- `metricExpression` is optional and is documentation only. The column is the definition.

### 5.2 Naming

OpenMetadata's Metric fully qualified name is the bare name, so names are global. Names are
`<subject>_<measure>` in snake case with no double underscore, e.g.
`learner_completion_status`, `contract_seats_used`, `learner_needs_attention_since`. The
file is named after the metric.

### 5.3 `ol-dbt validate --only metric_registry`

Added to `ol_dbt_cli.lib` beside `data_contracts.py`, reusing its binding parser, and added
to the global-gates step in `dbt_pr_ci.yaml` and to the `--skip` list of the changed-models
step. `metrics/**` joins that workflow's path filter. It needs no credentials. Errors:

- an `implemented_by` model does not exist in the project, or a listed column is not
  declared in the model's YAML or not selected by its SQL;
- two files declare the same metric name, or a name breaks the convention;
- `metricType`, `unitOfMeasurement` or `granularity` is not an OpenMetadata enum value;
- the file sets a field sync owns (`id`, `fullyQualifiedName`).

Bindings are resolved against the project's model YAML and SQL files, not against manifest
nodes. PR CI parses with the DuckDB target, where `b2b_analytics` and
`b2b_learner_records` are disabled, so a manifest lookup would reject every binding to a
StarRocks view.

### 5.4 First entries

The five metrics with competing definitions today: completion status, certified learners,
completion rate, monthly active learners, seats used. The first two are implemented by
phase 1. The others get a registry entry when their consolidation task lands, not before:
an entry must point at a column that exists.

---

## 6. Phase 3: sync to OpenMetadata

Blocked on the governance decision for team and reviewer names
(`tk-decide-the-openmetadata-governance-model-and-whe-14f5a6`).

`ol-dbt metrics sync --service <name> [--dry-run]`, reusing `OpenMetadataClient` from
`commands/contracts.py` (extracted to `lib/` on this second use). For each file:

1. Resolve owners, reviewers and glossary terms to references.
2. `PUT /v1/metrics` with the `metric` body. This creates the entity or updates its
   structural fields.
3. `PATCH /v1/metrics/{id}` for description, owners, reviewers and status. A bot PUT does
   not overwrite an existing description, so git wins only through PATCH.
4. For each `implemented_by` entry, resolve the table from a production manifest and
   `PUT /v1/lineage` with a table-to-metric edge and column lineage.

Drift: before step 3, read the live entity's `updatedBy`. If the last change was not made
by the sync's bot and description, reviewers or status differ from the file, report the
difference and exit non-zero without overwriting, unless `--force`. The edit then becomes
a pull request. This needs no stored state.

Where it runs: `ol-dbt contracts sync` has no scheduled caller in this repo today. Both
syncs should run from the same place after the production dbt build. Choosing that place
(a Dagster asset in the lakehouse code location vs. a Concourse step) belongs to the
governance project's config-as-code decision.

Not covered: lineage to StarRocks views. OpenMetadata has no StarRocks service, so an
`implemented_by` entry naming a view is checked in CI but skipped by sync, with a warning,
until that service exists.

---

## 7. Phase 4: per-metric consolidation

Each is an existing task. Each follows the phase 1 pattern: define the row-level columns
once, move aggregates onto them, add the registry entry, compare before and after, tell
the dashboard owners which numbers change.

| Metric area | Task |
| --- | --- |
| Certificates: four "earned" predicates, two revoked conventions | `tk-course-certificate-earned-four-predicates-and-tw-6badf1` |
| Engagement and "active": one activity fact | `tk-engagement-metrics-one-activity-fact-as-the-sour-9491c3` |
| Passing rule per platform | `tk-passing-has-a-different-rule-per-platform-and-tf-ffcbcd` |
| Enrollment populations and counts | `tk-enrollment-metrics-population-differs-between-tf-a15f8b` |
| Program completion | `tk-program-completion-certificate-url-rule-vs-edx-r-a485f6` |
| Commerce and net revenue | `tk-commerce-metrics-discount-amount-type-which-orde-154836` |
| Superset chart expressions | `tk-move-the-69-distinct-ad-hoc-sql-metric-expressio-f03265` |

The certificate and passing tasks change inputs of `afact_learner_courserun_progress`.
Phase 1 ships with today's inputs; those tasks then change `is_certified` and `is_passing`
in one place.

Cohort metadata for the API's k-anonymity policy (column `meta` on `b2b_analytics` plus a
dbt exposure per API tenant) is `tk-carry-the-api-s-cohort-policy-and-column-contrac-2aa49d`.
It is independent of the registry: it describes how columns relate for suppression, and
the API's CI reads it from the manifest.

---

## 8. Order and dependencies

1. Phase 1 dbt model (ol-data-platform), built by Trino in QA and production.
2. Phase 1 view change (ol-data-platform). After 1 exists in both lakes.
3. Phase 1 API change (ol-analytics-api). After 2 is built in production.
4. Phase 2 registry format and CI check. Independent of 1 to 3, but its first entries need
   1 and 2.
5. Phase 3 sync. After 4 and the governance decision.
6. Phase 4 tasks. Any order after 1; each adds registry entries once 4 exists.

---

## 9. Open questions and unverified points

Blocking for phase 1:

- Which enrollment represents a (user, course run) when `tfact_enrollment` has more than
  one. Not measured how often that happens.
- Which "today" the consumer compares against. The API uses the StarRocks cluster's
  `CURRENT_DATE()`; the enrollment date and `last_active_on` are UTC dates. If the cluster
  is not on UTC, a learner who enrolls late in the UTC day is not flagged until the cluster
  date catches up. The cluster's timezone has not been checked. Comparing against the UTC
  date in the API would remove the question.

Non-blocking:

- Order against API PRs #87 and #89, both open: either they merge first and the API change
  in 4.4 replaces the merged rule with the column, or they are rebased onto the column.
  The rule is the same either way. Needs the PR author. #89's distinct-learner count stays
  in the API as a distinct count over `needs_attention_since <= <cutoff>`.
- Whether Dagster orders the StarRocks build after the Trino build of a newly added
  dimensional model has not been traced. 4.3 avoids depending on it by splitting the pull
  requests.
- Materialization: table vs. incremental for the progress model. Start as a table and
  measure the build.
- Whether the existing `data_contract` check already resolves bindings from project files
  in a way 5.3 can reuse for target-disabled models. Not checked.
- Reviewers on re-sync: whether a bot PUT clears reviewers set in the OpenMetadata UI is
  not verified. Test on QA before writing the drift check.
- Creating a Metric with reviewers may start OpenMetadata's approval workflow and set the
  status itself. Not verified; affects step 3 of sync.
