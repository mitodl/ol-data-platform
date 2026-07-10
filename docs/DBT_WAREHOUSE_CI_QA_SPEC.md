# dbt Warehouse CI/QA Hardening & Data-Trust — Technical Spec

**Status:** Spec (accepted, ready for implementation)
**Project:** `wp-dbt-warehouse-ci-qa-hardening-data-trust-021724`
**Epic:** `tk-epic-dbt-warehouse-ci-qa-hardening-data-trust-9bf237`
**Origin issue:** [#2407](https://github.com/mitodl/ol-data-platform/issues/2407) — "Add ol-dbt diff command to QA migrated mart/reporting models"
**Related epic:** #2072 (dimensional-layer migration)
**Gap analysis:** witan memory `pf-dbt-warehouse-ci-qa-gap-analysis-phased-hardenin-4a7ae1`

---

## 1. Problem & goal

We want humans and agents to confidently ship changes to the dbt-managed warehouse
(`src/ol_dbt`), especially dimensional-model changes and their downstream impacts, and to
build org-level trust in the platform.

**Headline finding:** the repo has strong test *content* (~3,100 dbt tests, heavy
`dbt_expectations`) and a purpose-built `ol-dbt` CLI whose `impact` and `validate`
commands already emit `--format json` "for CI pipelines" — but **essentially nothing runs
on PRs**. Today's only PR-time dbt guardrail is pre-commit `sqlfluff`. `.github/workflows/`
contains `pytest.yaml` (runs `packages/` only, and only on `packages/**` changes) and
`publish_dbt_docs.yaml` (dbt compile+docs, push-to-`main` only). No dbt build/test, no
`ol-dbt validate`/`impact`, no dimensional-layering enforcement, and the CLI's own pytest
suite is un-gated.

**The biggest confidence win is wiring tools that already exist.** This spec sequences that
wiring into four credential/engine-gated phases.

### Non-goals
- No new diff engine (Datafold, etc.). DuckDB `dev_local` reads Iceberg directly and a
  same-engine diff cancels dialect differences — see §3.
- No copying of prod data. The zero-copy substrate (`ol-dbt local register` + Glue/Iceberg
  DuckDB views) already exists and is reused.
- No changes to test *content* in this project; we gate and surface the tests that exist.

---

## 2. System facts the spec relies on (grounded)

- **CLI shape** (`src/ol_dbt_cli/`): `cyclopts` app. `impact` and `validate` are bare
  module-level functions registered in `cli.py` via `app.command(impact, name="impact")` /
  `app.command(validate, name="validate")`. `local`/`run`/`generate`/`starrocks` are
  `cyclopts.App` sub-apps.
- **Arg convention:** params are `Annotated[T, Parameter(name=[...])]` keyword flags.
  `--format` is currently an **unconstrained `str`** defaulting to `"text"`; code branches on
  `output_format == "json"`. `--target/-t` defaults to `"dev_local"`.
- **JSON output convention:** build a flat `list[dict]`, `print(json.dumps(data, indent=2))`
  (plain `print`, never rich). Text mode uses `console.print` + a trailing rich `Summary:`
  line. Dual consoles: `console = Console()`, `err_console = Console(stderr=True)`.
- **Severity enums:** `impact` uses `AlertLevel(StrEnum) = BREAKING|WARNING|INFO`; `validate`
  uses `Severity(StrEnum) = ERROR|WARNING|INFO`. Exit `1` when the top severity is present,
  bare `return` for the clean/no-op case.
- **dbt invocation:** always `subprocess.run([...], cwd=str(dbt_dir))`, argv list, no
  `dbtRunner`. `run.py` passes `--profiles-dir str(dbt_dir)`; `impact`/`validate` rely on
  cwd. `DBT_PROFILES_DIR` is not set by the CLI.
- **Reusable libs:**
  - `lib/sql_parser.py`: `ParsedModel` dataclass (`output_columns: set[str]`, `refs`,
    `source_refs`, `has_star`, …); `parse_model_file(path, compiled_dir=None)`,
    `parse_model_sql_at_content(name, sql)` (for `git show` content),
    `get_columns_read_from_ref(downstream, upstream_name) -> set[str] | None`,
    `find_compiled_dir(dbt_dir)`.
  - `lib/yaml_registry.py`: `build_yaml_registry(models_dir)`; `YamlModel.column_names`,
    `file_to_models`, `get_source_columns(...)`.
  - `lib/manifest.py`: `find_manifest(dbt_dir)` (checks `target/manifest.json` then
    `manifest.json`), `load_manifest(path) -> ManifestRegistry`; `ManifestModel` exposes
    `.database`, `.schema`, `.name`, `.resource_type`, `.columns`, `.depends_on`,
    `.column_names`, `.is_model`; `get_children`, `get_all_descendants`,
    `column_consumers(unique_id, column)`. **Note:** no `identifier`/`relation` field — a
    fully-qualified relation must be built from `.database` + `.schema` + `.name`.
- **Zero-copy substrate:** `ol-dbt local register` mounts prod Glue/Iceberg tables as DuckDB
  views named `glue__<db>__<table>` (`iceberg_scan`). `macros/override_ref.sql` (`ref`) and
  `macros/override_source.sql` (`source`) fall back to these views for unbuilt models on the
  `dev_local` target; `macros/duckdb_glue_integration.sql` holds `duckdb_init` /
  `iceberg_source`. This lets old+new build side-by-side against real prod data, no copy.
- **Slim-CI backbone:** `ol-dbt run` builds `dbt <sub> --select "state:modified+
  result:error+ result:fail+" --state <.dbt-state> --defer`. Saved-state dir default is
  `<dbt_project>/.dbt-state/` (`manifest.json`, `run_results.json`). The **prod manifest is
  already published to S3** (`DbtS3ArtifactsResource`, prefix `openmetadata/dbt-artifacts`) —
  a ready-made deferral/`--state` baseline.
- **Namespaced schemas:** Trino `dev_qa`/`dev_production` targets use
  `schema: ol_warehouse_{qa,production}{{ "_{}".format(var('schema_suffix')) }}`;
  `schema_suffix` default is `dev` (`dbt_project.yml:37`). `ol-dbt local` has a
  `PROTECTED_SCHEMAS` guard + dev-schema teardown.
- **StarRocks:** reads the SAME Iceberg tables via external catalog `ol_data_lake_iceberg`,
  so Trino/DuckDB QA is representative — EXCEPT the `b2b_analytics` layer
  (`+enabled: target.type == 'starrocks'`), materialized natively in StarRocks, which must be
  QA'd on StarRocks. Its profiles (`profiles.yml` `starrocks_qa`/`starrocks_qa_vault`/
  `starrocks_production`) hardcode `schema: '{{ env_var("DBT_STARROCKS_SCHEMA",
  "b2b_analytics") }}'` — **no `schema_suffix`**, so dev/CI builds collide.

---

## 3. Phase 0 — Ship `ol-dbt diff` (#2407) — DETAILED

**Task:** `tk-p0-ship-ol-dbt-diff-command-2407-12ae21`

### 3.1 Purpose
A by-hand, same-engine row/column diff of two model relations (typically an old vs migrated
mart/reporting model) built on the same engine so dialect differences cancel. Must be
driveable **both by hand and by CI** (Phase 2 auto-diff vs base ref).

### 3.2 Package dependency
Add to `src/ol_dbt/packages.yml`:
```yaml
- package: dbt-labs/dbt_audit_helper
  version: [">=0.12.0", "<0.13.0"]   # pin to the current 1.x-compatible release at impl time
```
Wrap `audit_helper.compare_relations` / `compare_column_values` / `compare_row_counts`.
(Current packages: `codegen`, `metaplane/dbt_expectations`, `dbt_utils`,
`starburstdata/trino_utils`.)

### 3.3 CLI surface
New file `src/ol_dbt_cli/ol_dbt_cli/commands/diff.py`; register in `cli.py` via
`app.command(diff, name="diff")`. Mirror the `impact`/`validate` bare-function style.

```python
def diff(
    old: Annotated[str, Parameter(name=["--old"], help="Baseline model/relation name.")],
    new: Annotated[str, Parameter(name=["--new"], help="Candidate model/relation name.")],
    dbt_dir_path: Annotated[str | None, Parameter(name=["--dbt-dir", "-d"])] = None,
    target: Annotated[str, Parameter(name=["--target", "-t"])] = "dev_local",
    primary_key: Annotated[tuple[str, ...], Parameter(name=["--primary-key", "-k"])] = (),
    exclude_columns: Annotated[tuple[str, ...], Parameter(name=["--exclude-columns"])] = (),
    output_format: Annotated[str, Parameter(name=["--format", "-f"])] = "text",
    limit: Annotated[int, Parameter(name=["--limit"])] = 20,   # cap sample rows
    auto_build: Annotated[bool, Parameter(name=["--auto-build"])] = False,
) -> None: ...
```
Recommendation: type `--format` as `Literal["text", "json"]` (stricter than the existing
`impact`/`validate` `str`, consistent with `run`/`local` literals) so an invalid value fails
loudly instead of silently falling to text.

### 3.4 Algorithm
1. **Resolve dbt project** (shared preamble from `impact.py`/`validate.py`): resolve
   `dbt_dir` → assert `dbt_project.yml` → `find_compiled_dir` → `find_manifest` +
   `load_manifest` → `build_yaml_registry`.
2. **Reconcile column sets BEFORE diffing.** Using `sql_parser` (`ParsedModel.output_columns`
   from the compiled SQL, cross-checked against `yaml_registry` / manifest `column_names`),
   compute `columns(old)` vs `columns(new)`. If they differ, emit a clear structured message
   (`only_in_old`, `only_in_new`) instead of letting a raw SQL error surface from
   `audit_helper`. Diff proceeds on the **intersection minus `--exclude-columns`**.
3. **Primary key / non-determinism handling.** If `--primary-key` given, pass it to
   `compare_relations` as the join key. Document the canonical trap: `dim_user.user_pk` is an
   email-keyed surrogate that collapses NULL emails — a naive full-row compare shows spurious
   mismatches; callers should pass `--primary-key` and/or `--exclude-columns` for known
   unstable columns.
4. **Run the comparison.** Render an `audit_helper` operation via a small analysis/macro
   invocation and execute it with `dbt`. On `dev_local`, unbuilt sides resolve to the Glue
   DuckDB views through `override_ref`/`override_source` (zero-copy). `--auto-build` may first
   `ol-dbt run --select <old> <new>` (or `dbt build --select`) so both relations exist.
   Invoke dbt as `subprocess.run([...], cwd=str(dbt_dir), capture_output=True, text=True)`.
5. **Summarize.** Compute row-count delta, per-column mismatch rate, and the first `--limit`
   mismatched rows.

### 3.5 Output
**JSON** (`--format json`) — flat, `print(json.dumps(..., indent=2))`:
```json
{
  "old": "dim_user_old", "new": "dim_user", "target": "dev_local",
  "primary_key": ["user_pk"], "excluded_columns": ["_loaded_at"],
  "column_reconciliation": {"only_in_old": [], "only_in_new": ["new_col"], "compared": ["..."]},
  "row_counts": {"old": 12345, "new": 12345, "delta": 0},
  "column_mismatches": [{"column": "email", "mismatch_rate": 0.001, "mismatched_rows": 12}],
  "sample_mismatches": [{"...": "capped at --limit rows"}],
  "verdict": "match | mismatch | schema_divergence"
}
```
**Text** — rich table of the above + a trailing `Summary:` line.

### 3.6 Exit codes
- `0`: relations match within the compared column set (and no schema divergence, or
  divergence is empty).
- `1`: any row-count delta, column mismatch, or unreconcilable schema divergence (`verdict !=
  "match"`). Wire this so CI can gate on it.
- `1` + `err_console` message on operational failure (no project, dbt error, relation not
  found).

### 3.7 Tests — `src/ol_dbt_cli/tests/test_diff.py`
- Column reconciliation: identical / `only_in_new` / `only_in_old` sets.
- Exclude-columns and primary-key plumbing into the comparison args.
- JSON schema shape + `--limit` capping.
- Exit-code matrix (match→0, mismatch→1, schema divergence→1, missing project→1).
- dbt invocation mocked (`subprocess.run` patched) — no live warehouse in unit tests.

### 3.8 Acceptance criteria
- [ ] `ol-dbt diff --old A --new B` runs on `dev_local` with zero cloud creds and prints a
      capped human summary.
- [ ] `--format json` emits the schema in §3.5; `--primary-key`/`--exclude-columns` respected.
- [ ] Schema mismatch yields a structured message, not a raw SQL error.
- [ ] Exit code 1 on any mismatch (CI-gateable).
- [ ] `dbt_audit_helper` added to `packages.yml`; `test_diff.py` added; `cli.py` registers it.

---

## 4. Phase 1 — Fast PR CI (GitHub Actions, no creds) — DETAILED

**Task:** `tk-p1-fast-pr-ci-for-dbt-github-actions-no-creds-de9076`
Highest ROI: mostly wiring tools that already emit CI-ready JSON.

New workflow `.github/workflows/dbt_pr_ci.yaml`, `on: pull_request` with
`paths: [src/ol_dbt/**, src/ol_dbt_cli/**]` (+ `workflow_dispatch`). Jobs (all credential-free;
run `dbt parse`/`compile` against `dev_local`, no warehouse network):

1. **dbt-parse** — `uv sync`; `dbt deps`; `dbt parse -t dev_local` (produces `manifest.json`
   for the JSON-emitting steps). `working-directory: src/ol_dbt`.
2. **validate** — `ol-dbt validate --format json`; fail on any `ERROR`. Upload JSON artifact.
3. **impact** — `ol-dbt impact --format json --base-ref origin/${{ github.base_ref }}`; post
   the BREAKING/WARNING column-level blast radius as a **PR comment** (create-or-update a
   single sticky comment). BREAKING → non-zero (or convert to a required-review signal — decide
   at impl; default: annotate, do not hard-fail, to avoid blocking legitimate breaking changes
   that are reviewed).
4. **dimensional-layering-lint** — see §5 (runs as `ol-dbt validate --only dimensional_layering`
   or a dedicated flag).
5. **sqlfluff-lint** — run the same sqlfluff config used in pre-commit (today pre-commit only).
   Fix: `ci.skip` behavior so it runs in CI too.
6. **pytest** — the repo-wide pytest suites. **Superseded/expanded by**
   `tk-expand-pytest-yaml-to-run-all-repo-pytest-suites-0996f5`: `pytest.yaml` currently runs
   `packages/` only; it must run all five suites (`packages/ol-orchestrate-lib`, `src/ol_dbt`,
   `src/ol_dbt_cli`, `src/ol_dlt`, `src/ol_superset`). Note `src/ol_dlt` is a **standalone uv
   project** (excluded from the root workspace) and needs its own `cd src/ol_dlt && uv sync`.

### Acceptance criteria
- [ ] A PR touching `src/ol_dbt/**` triggers the workflow; a PR touching only docs does not.
- [ ] `validate` failures block; `impact` posts a single updating PR comment.
- [ ] Dimensional-layering lint runs and reports violations (gating policy per §5.4).
- [ ] `src/ol_dbt_cli` tests run in CI (via the pytest-expansion task).
- [ ] All jobs complete with no warehouse credentials.

---

## 5. Phase 1a — Dimensional-layering lint (enforce #2072 DoD) — DETAILED

**Task:** `tk-p1a-dimensional-layering-lint-enforce-2072-dod-a76da5`

### 5.1 Rule (from `models/dimensional/README.md`)
Marts (`models/marts/`) and reporting (`models/reporting/`) models must reference ONLY
dimensional models (`dim_*`/`tfact_*`/`afact_*`/`bridge_*`) or other mart/reporting models —
never `staging/` (`stg__*`) or `intermediate/` (`int__*`) directly. This is #2072's DoD,
currently unenforced prose, violated by 26/28 marts + 13/25 reporting (~100+ `int__` refs).

### 5.2 Implementation
Add a check to `ol-dbt validate` (new `ValidationIssue` check name, e.g.
`dimensional_layering`), selectable via the existing `--only`/`--skip` machinery, emitted in
both text and `--format json`. Operate over the **manifest** (authoritative
parent/child/resource_type + directory), not string-matching SQL:
- For each node whose `original_file_path` is under `models/marts/` or `models/reporting/`,
  inspect `depends_on` parents via `manifest.get_node`.
- Flag any parent whose path is under `models/staging/` or `models/intermediate/`.
- `ValidationIssue(check="dimensional_layering", severity=ERROR, model=<child>,
  message="marts/reporting model refs staging/intermediate directly",
  detail="<child> -> <offending parent> (int__/stg__)")`.

### 5.3 Baseline / migration-in-flight handling
Because ~100+ violations exist today, a hard ERROR would red-wall every PR. Ship with an
**allowlist baseline** (checked-in `dimensional_layering_baseline.txt` of known
`child -> parent` pairs). The lint fails only on **new** violations not in the baseline; the
#2072 migration shrinks the baseline over time. Provide `ol-dbt validate --update-baseline` (or
document the regeneration command).

### 5.4 Acceptance criteria
- [ ] Manifest-based check; correct on the current 39 known violators.
- [ ] Baseline lets the check pass CI today while blocking *new* `int__`/`stg__` refs.
- [ ] `--format json` includes `dimensional_layering` issues; wired into the P1 workflow.

---

## 6. Phase 2 — Slim data CI (Concourse, credentialed) — DESIGN

**Depends on:** Phase 0 (`diff`), Phase 1 (green fast CI). Concourse (in-cluster) is required
because this needs Trino OAuth/LDAP + Vault + `kubectl port-forward` that GitHub Actions
cannot hold.

Pipeline outline:
1. Download the prod manifest from S3 (`DbtS3ArtifactsResource`, prefix
   `openmetadata/dbt-artifacts`) → use as `--state` baseline.
2. `ol-dbt run` builds `--select state:modified+ result:error+ result:fail+ --defer` into a
   **per-PR namespaced schema** `ol_warehouse_qa_ci_pr_<n>` (via `schema_suffix` var =
   `ci_pr_<n>`).
3. `dbt test` on the built + deferred selection.
4. Auto-run `ol-dbt diff` for each changed mart/reporting model vs the base ref (reusing the
   Phase 0 command, `--format json`, gating on exit 1).
5. Teardown: existing `ol-dbt local cleanup` + `PROTECTED_SCHEMAS` guard drops
   `ol_warehouse_qa_ci_pr_<n>`.
6. StarRocks `b2b_analytics` changes build against a namespaced StarRocks schema — **requires
   Phase 2a**.

### Acceptance criteria (high level)
- [ ] Per-PR schema build+test+diff+teardown, no collisions between concurrent PRs.
- [ ] Deferral against prod manifest so only modified+ is built.

---

## 7. Phase 2a — StarRocks `schema_suffix` namespacing — DESIGN

**Task:** `tk-p2a-add-schema-suffix-namespacing-for-starrocks--6e38f8`. Prereq for StarRocks
coverage in Phase 2.

Today (`src/ol_dbt/profiles.yml`, targets `starrocks_qa`, `starrocks_qa_vault`,
`starrocks_production`): `schema: '{{ env_var("DBT_STARROCKS_SCHEMA", "b2b_analytics") }}'` —
no suffix, so two dev/CI builds collide. Mirror the Trino
`ol_warehouse_{env}{{ "_{}".format(var('schema_suffix')) }}` pattern, e.g.:
```yaml
schema: '{{ env_var("DBT_STARROCKS_SCHEMA", "b2b_analytics") }}{{ "_" ~ var("schema_suffix") if var("schema_suffix", "") not in ["", "dev"] else "" }}'
```
**Design decision for implementer:** reconcile `env_var` (set by the Dagster StarRocks
resource) vs `var('schema_suffix')` (the dbt-native pattern). Preserve current default behavior
(bare `b2b_analytics` when no suffix) so production is unaffected; append `_<suffix>` only when a
non-default suffix is supplied.

### Acceptance criteria
- [ ] Two concurrent StarRocks builds with different `schema_suffix` land in distinct schemas.
- [ ] Default (no suffix) still resolves to `b2b_analytics`; production unchanged.

---

## 8. Phase 3 — Runtime + org-level trust — DESIGN

**Task:** `tk-p3-runtime-org-level-trust-dagster-asset-checks--23d637`. Complementary to
PR-time CI.

1. **Dagster asset checks** (currently ZERO exist) on the dimensional assets
   (`full_dbt_project` in `dg_projects/lakehouse`): `build_freshness_checks` + column
   schema-change checks on the declared contract boundary.
2. **Expand dbt source freshness** — configured on only 2 sources (`mitxonline`, `mitxpro`);
   extend and run `dbt source freshness` on a schedule.
3. **OpenMetadata trust dashboard** — push `dbt run_results.json` (already in S3) into OM as
   DBT test cases (OM natively understands the DBT platform) so the ~3,100 tests become an
   org-visible green/red dashboard. NB: OM push is currently inert (SDK stub
   `data_platform/.../metadata/databases.py` unwired); the real link is dbt artifacts → S3 →
   external OM ingestion + Trino harvest.
4. **Enrich `ol-dbt impact` with OM downstream lineage** (`get_entity_lineage`) to catch
   Superset dashboards + HMAC reverse-ETL + IR ad-hoc consumers the repo manifest is blind to.

---

## 9. Task index & sequencing

| Task | Phase | Priority | Gates on |
|------|-------|----------|----------|
| `tk-p0-ship-ol-dbt-diff-command-2407-12ae21` | 0 | p1 | — |
| `tk-p1-fast-pr-ci-for-dbt-github-actions-no-creds-de9076` | 1 | p1 | P1a, pytest-expand |
| `tk-p1a-dimensional-layering-lint-enforce-2072-dod-a76da5` | 1a | p1 | — |
| `tk-expand-pytest-yaml-to-run-all-repo-pytest-suites-0996f5` | 1 | p1 | — |
| `tk-p2a-add-schema-suffix-namespacing-for-starrocks--6e38f8` | 2a | p2 | — |
| `tk-p3-runtime-org-level-trust-dagster-asset-checks--23d637` | 3 | p2 | — |

**Recommended order:** P0 + P1a + pytest-expand can proceed in parallel (independent) →
assemble P1 workflow → P2a → P2 Concourse → P3. P0, P1, P1a are the immediate implementation
targets.

---

## 10. Open questions for implementation
- **impact gating:** hard-fail on BREAKING, or annotate-only? Default: annotate (breaking
  changes are legitimate under review); revisit once teams trust the signal.
- **diff `--auto-build`:** default off (assume relations pre-built) vs on (build both sides
  first). Default off for the by-hand case; Phase 2 CI passes `--auto-build`.
- **StarRocks schema var vs env_var** (§7) — reconcile the two namespacing mechanisms.
- **dbt_audit_helper version pin** — confirm the release compatible with the pinned dbt-core at
  implementation time.
