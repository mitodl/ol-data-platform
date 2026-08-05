# QA/Production Data Topology — Spec Resolution

**Status:** Spec (resolves the RFC's blocking open questions; RFC ready to move Draft → Accepted)
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
   `salesforce`, `zendesk`. Production already ingests these on 24-hour schedules
   (`dg_projects/lakehouse/lakehouse/definitions.py:218-224`). A QA mirror tracking them
   hourly, or even daily, tracks noise.

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
| Model declares a `qa_branch` absent from the inventory, or whose `qa_strategy` is `omit` | Declaration contradicts declaration | `ERROR` | **No** |
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
