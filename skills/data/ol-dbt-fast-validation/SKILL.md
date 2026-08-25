---
name: ol-dbt-fast-validation
description: >
  Drive the ol-dbt CLI (validate, impact, diff) to QA dbt-warehouse changes fast
  and credential-free before opening or reviewing a PR. Use this skill whenever
  you edit models under src/ol_dbt/ (especially dimensional, mart, or reporting
  models) and need to check SQL/YAML consistency, column-level downstream blast
  radius, or whether a migrated model still matches its predecessor — all on the
  local DuckDB target, no Trino/warehouse credentials required.
license: BSD-3-Clause
metadata:
  category: data
---

# Fast dbt validation with `ol-dbt`

The `ol-dbt` CLI (`src/ol_dbt_cli`) already emits CI-ready JSON. Reach for it
**before** committing or when reviewing a dbt change — it catches broken
refs, column drift, and downstream breakage without a warehouse round-trip.

All commands run against the local **`dev_local`** target (DuckDB reading
Iceberg directly) by default, so they need **no network or credentials**. Run
them from anywhere in the repo; the CLI locates `src/ol_dbt` automatically.

## The three checks (run in this order)

### 1. `ol-dbt validate` — SQL/YAML consistency
Confirms model SQL and its `.yml` schema agree (column sync, broken `ref()`s,
`SELECT *`, doc coverage).

```bash
ol-dbt validate                      # all models
ol-dbt validate --changed-only       # only models changed vs origin/main (-c)
ol-dbt validate --model staging      # scope to a path/name
ol-dbt validate --format json        # machine-readable, for CI
```
Exit code is non-zero when any `ERROR`-severity issue is found. Use
`--only <check>` / `--skip <check>` to focus or suppress specific checks.

### 2. `ol-dbt impact` — column-level downstream blast radius
Diffs each changed model's output columns vs the base ref and walks forward
lineage to flag downstream models that a removed/renamed column will break.

```bash
ol-dbt impact                        # all changed models vs origin/main
ol-dbt impact --model dim_user       # one model
ol-dbt impact --format json          # BREAKING/WARNING/INFO alerts for CI
ol-dbt impact --auto-compile         # dbt compile on dev_local first for precise columns
```
Alert levels: 🔴 `BREAKING` (removed/renamed column is consumed downstream) ·
⚠️ `WARNING` (impact indeterminate) · ℹ️ `INFO` (additive only). Exits non-zero
on any `BREAKING`. For the most accurate lineage, run `dbt parse` (or
`--auto-compile`) first so a `manifest.json` exists.

### 3. `ol-dbt diff` — same-engine row/column comparison
Compares an old model against its migrated replacement on the **same engine**
(so dialect differences cancel). Use it to QA dimensional/mart/reporting
migrations (epic #2072).

```bash
ol-dbt diff --old dim_user_old --new dim_user --primary-key user_pk
ol-dbt diff --old m_old --new m_new -k user_email,exam_created_on   # composite key
ol-dbt diff --old m_old --new m_new -k id --exclude-columns _loaded_at --format json
ol-dbt diff --old m_old --new m_new --auto-build   # build both sides first
```

To QA an *in-place* edit to one model, freeze a baseline first so upstream data
drift can't masquerade as a code-change diff:

```bash
ol-dbt local snapshot my_model --as my_model_baseline   # before the change
# ...edit the SQL...
ol-dbt run --select my_model --target dev_local
ol-dbt diff --target dev_local \
    --old my_model_baseline --old-raw \
    --new my_model --primary-key my_model_pk
```
- `--old-raw`/`--new-raw` mark that side as a literal existing table rather than a
  dbt model — **required** for a snapshot table or any relation dbt doesn't know
  via `ref()`. Pair with `--old-schema`/`--new-schema` to compare the same model
  across schemas (e.g. your dev schema vs. production) on one Trino target.
- **Always pass `--primary-key`** for a meaningful per-column comparison, and
  for models with known surrogate-key non-determinism (e.g. `dim_user.user_pk`
  is email-keyed and collapses NULL emails — a naive full-row compare shows
  spurious mismatches).
- **Use the model's real grain, including composite keys.** Pass a composite key
  comma-separated (`-k a,b,c`) or by repeating the flag (`-k a -k b -k c`); the two
  are equivalent. `-k a b c` does *not* work — only the first token is taken as a
  key and the rest are read as positional args, producing a confusing `--dbt-dir`
  error. A non-unique key pairs rows many-to-many and inflates the mismatch and
  "missing" counts with join artifacts rather than real differences, so a
  single-column key on a composite-grain model actively misleads. If you don't know
  the grain, check the model for a
  `dbt_expectations_expect_compound_columns_to_be_unique` test — its column list is
  the key.
- Use `--exclude-columns` for non-deterministic columns (load timestamps, etc.).
- Column sets are reconciled first: a schema mismatch is reported as
  `schema_divergence`, not a raw SQL error.
- Exits non-zero on any divergence (`mismatch` or `schema_divergence`), so it is
  gate-able in CI. `--limit` caps sample mismatched rows (default 20).

Requires the `dbt_audit_helper` package: run `dbt deps` once if you see it
missing.

## Rules

- Prefer `--format json` when a machine (CI, another agent) consumes the output;
  the human-readable text form is the default.
- These checks are **fast and free** — run `validate` + `impact` on every
  dbt change before committing; run `diff` whenever you migrate or refactor a
  mart/reporting/dimensional model.
- Non-zero exit = blocking. Do not declare a dbt change done while `validate`
  reports errors or `impact` reports `BREAKING` without a reviewed reason.
- If lineage looks incomplete, you likely need a fresh manifest — run
  `dbt parse -t dev_local` (or pass `--auto-compile`) and re-run.

See `docs/specs/DBT_WAREHOUSE_CI_QA_SPEC.md` for how these commands slot into the
phased CI/QA plan. For building models locally to diff against, see the
`ol-dbt-local-dev` skill.
