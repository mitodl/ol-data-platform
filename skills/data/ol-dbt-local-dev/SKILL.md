---
name: ol-dbt-local-dev
description: >
  Stand up and drive the local DuckDB + Iceberg dbt development environment via
  `ol-dbt local` and `ol-dbt run`. Use this skill when you need to build or
  iterate on src/ol_dbt models locally against real production data (zero-copy,
  no warehouse writes) — e.g. to materialize an old and a new model side-by-side
  for `ol-dbt diff`, to iterate fast on a single changed model, or to bootstrap a
  fresh local warehouse. Covers setup, Glue→DuckDB view registration, incremental
  runs, and safe teardown.
license: BSD-3-Clause
metadata:
  category: data
---

# Local dbt development (DuckDB + Iceberg) with `ol-dbt`

`ol-dbt local` gives you a **zero-copy** local warehouse: production Glue/Iceberg
tables are mounted as DuckDB views, and dbt's `ref()`/`source()` fall back to
those views for models you haven't built. So you can build old + new models
side-by-side against real prod data — without copying it or writing to the
warehouse. The default dbt target for all of this is **`dev_local`** (DuckDB).

## Prerequisite: run everything through `uv run --frozen`

Every command below — and every `dbt` command in `ol-dbt-migration-validation` —
must resolve `dbt` from this project's venv:

```bash
uv sync                          # once, to create/refresh .venv
source .venv/bin/activate        # then every bare command on this page works
dbt --version                    # expect: dbt 1.12.4, plugin duckdb 1.11.x
```

**The examples below are written bare (`ol-dbt run`, `ol-dbt diff`) and assume you
activated the venv.** Activation is what puts `.venv/bin` first on `PATH`, which is
the whole fix — verified: after `source .venv/bin/activate`, `which dbt` resolves to
this repo's 1.12.4. If you would rather not activate, prefix **every** command with
`uv run --frozen` (`uv run --frozen ol-dbt run ...`); what does not work is doing
neither, because `ol-dbt run` and `ol-dbt diff` both shell out to `dbt` and will
pick up whatever is on your `PATH`.

**Why this is not optional.** `ol-dbt` shells out to bare `"dbt"`
(`commands/run.py`: `cmd = ["dbt", subcommand, ...]`), resolved through `PATH`
rather than from the venv `ol-dbt` itself lives in. Any other dbt earlier on
`PATH` wins. A `pipx`/`pip --user` dbt is the usual culprit — one at
`~/Library/Python/3.9/bin/dbt` (dbt 1.8.1, no duckdb adapter) shadowed the
project's 1.12.4 here, and every `dev_local` command died with:

```
Error importing adapter: No module named 'dbt.adapters.duckdb'
Runtime Error  Credentials in profile "open_learning", target "dev_local" invalid:
  Could not find adapter type duckdb!
```

That error names the adapter, not the `PATH`, so it reads like a broken install.
Check with `which -a dbt` — if the first hit is not this repo's `.venv/bin/dbt`,
prefix with `uv run --frozen`. Activating the venv (`source .venv/bin/activate`)
works too. `--frozen` keeps `uv` from re-resolving and rewriting `uv.lock` as a
side effect of running a command.

## Workflow

### 1. One-time setup
```bash
uv run --frozen ol-dbt local setup   # bootstrap the local DuckDB + Iceberg env (installs deps, dbt debug)
```

### 2. Register production tables as DuckDB views
```bash
ol-dbt local register --all-layers      # register all standard dbt layer databases
ol-dbt local register --database ol_warehouse_production_raw   # one layer
ol-dbt local register --force           # re-register everything (default: only new/changed)
ol-dbt local list-sources               # show what's currently registered
```
`register` reads **AWS Glue + S3**, so it needs AWS credentials (unlike the
validation commands, which are fully offline). It is incremental by default and
parallelized; use `--dry-run` to preview.

**Re-register the layers you are about to read, immediately before every build —
not when the staleness warning tells you to.** `list-sources` warns at ">1 day
old", and that threshold is tuned for the wrong failure. `register` stores the
Iceberg `metadata_location` that Glue reports at that instant, and for a dbt-built
table Glue routinely points the canonical name at a `__dbt_tmp` directory. The next
production materialization of that model swaps and deletes the directory, so a
registration that was correct an hour ago now resolves to nothing:

```
HTTP Error: HTTP GET error reading
  's3://ol-data-lake-intermediate-production/<model>__dbt_tmp-<hash>/metadata/<n>.metadata.json'
  (HTTP 404 Not Found)
```

Measured 2026-09-14: a registry refreshed **60 minutes** earlier failed exactly this
way on `int__micromasters__dedp_proctored_exam_grades`, killing two mart models.
Re-registering only the intermediate layer (41 pointers moved, 118 unchanged) fixed
it. Sixty minutes, not the day the warning implies. Re-registering the same three
layers 26 hours later moved another **336** pointers (184 staging, 108 intermediate,
44 dimensional) — that is the churn rate you are racing.

**The 404 is the lucky outcome.** When the `__dbt_tmp` directory still exists but
holds a mid-build snapshot, the view returns duplicated or partial rows and *nothing
fails* — a clean run and wrong numbers, which is far worse when you are about to
quote those numbers in a PR. That is why the rule is unconditional re-registration
rather than a reaction to an error you can see. #2660 stopped `__dbt_tmp` tables
being registered as sources in their own right, but deliberately did **not** fix
canonical names pointing at `__dbt_tmp` locations — 621 of 636 dbt-built tables at
last count. Until that is fixed, freshness is your responsibility:

```bash
# re-register only the layers the models you are about to build actually read
uv run --frozen ol-dbt local register --database ol_warehouse_production_staging
uv run --frozen ol-dbt local register --database ol_warehouse_production_intermediate
uv run --frozen ol-dbt local register --database ol_warehouse_production_dimensional
```

**Read the `✗ Errors:` line — a non-zero count does not fail the command.**
`register` catches per-table failures, prints the tally, and still exits 0. A table
that fails to re-register keeps its previous view, so a run that *looks* successful
can leave you on exactly the stale pointer you were trying to replace. Treat
`✗ Errors: 0` as the success condition, not the exit status.

**One exception, and it is not really an exception.** When you are building two
relations to compare against each other, both sides must come from the *same*
registration — re-register between them and the difference you measure is drift,
not code. That is compatible with the rule above because you build both sides in
one invocation: register immediately before it. If you need to rebuild, re-register
and rebuild **both** sides, never one. `ol-dbt-migration-validation` step 1 states
this as the acceptance rule.

### 3. Iterate on models
```bash
ol-dbt run                  # incremental: rebuild only changed/errored models (state:modified+ result:error+/fail+ --defer)
ol-dbt run --select my_model+   # build a model and its downstream
ol-dbt run --full-refresh   # full rebuild; also re-initialises state for the next incremental run
```
`ol-dbt run` saves dbt state under `<dbt_project>/.dbt-state/` (`manifest.json`,
`run_results.json`) so subsequent runs only touch what changed — this is the same
slim-CI mechanism used in Phase 2.

### 4. Teardown
```bash
ol-dbt local cleanup-local          # drop locally-registered DuckDB views/tables
ol-dbt local cleanup --dry-run      # preview cleanup of remote dev schemas (namespaced by schema_suffix)
```
Cleanup honors a `PROTECTED_SCHEMAS` guard — it will not drop production or
shared schemas. Always `--dry-run` first when cleaning remote schemas.

## Typical use: materialize both sides for a diff
```bash
ol-dbt local register --all-layers          # mount prod data
ol-dbt run --select "dim_user_old dim_user" --full-refresh   # build both relations on dev_local
ol-dbt diff --old dim_user_old --new dim_user --primary-key user_pk
```
**Quote a multi-model selector.** `ol-dbt run` declares `--select` as a single
`str`, so an unquoted second model binds to the `SUBCOMMAND` positional and the
command dies with `Invalid value "dim_user" for SUBCOMMAND. Choose from: "build",
"run", "test"` — it never reaches dbt. Note the asymmetry with `ol-dbt diff`, whose
`-k`/`--exclude-columns` are `list[str]` and *do* take repeated or comma-separated
values; that inconsistency is what makes this easy to get wrong.

`--full-refresh` applies to the selection, so it re-derives both sides in full. Drop
it only if neither model is `materialized='incremental'`; otherwise the diff can
compare rows the incremental predicate never reselected.
For a model whose grain is more than one column, pass the whole key —
comma-separated (`-k a,b,c`) or by repeating the flag (`-k a -k b -k c`), which are
equivalent. `-k a b c` does not work. A non-unique key pairs rows many-to-many and
reports join artifacts as mismatches.

## Typical use: freeze a baseline before an in-place edit
When you change a model's SQL rather than adding a `_new` copy, snapshot the
pre-change build so the diff reflects only your change, not upstream data drift:
```bash
ol-dbt local snapshot my_model --as my_model_baseline   # materialize a frozen copy
# ...edit the SQL...
ol-dbt run --select my_model --full-refresh
ol-dbt diff --old my_model_baseline --old-raw --new my_model --primary-key my_model_pk
```
The snapshot is frozen, but the rebuild is not: without `--full-refresh` an
incremental `my_model` may leave the edited rows untouched, and the diff then
reports "no change" for a change that simply never ran.
`--old-raw` is required because the snapshot is a literal table, not a dbt
`ref()`-able model.

## Rules

- Default to the **`dev_local`** target — it needs no Trino/warehouse credentials
  and reads Iceberg directly. Only reach for `dev_qa`/`dev_production` targets
  when you specifically need the shared cluster.
- `register` needs AWS creds; `setup`, `run` (on dev_local), and the validation
  commands do not.
- **Re-register immediately before every build**, not when the staleness warning
  fires. Glue points dbt-built tables at `__dbt_tmp` locations that production
  deletes on its next run; this bites within the hour, and its silent form returns
  duplicated or partial rows with no error at all. See step 2.
- Prefer incremental `ol-dbt run` while **iterating** — you want the fast loop and
  only care that the model executes. Switch to `--full-refresh` as soon as you are
  going to **read the model's contents and draw a conclusion** from them
  (validating a change, diffing before/after, confirming a fix landed). An
  incremental run does execute the model SQL, but the model's own
  `is_incremental()` predicate decides **which rows get re-derived**; every row it
  excludes keeps the value the *old* code produced, and the run still reports `OK`.
  Read the whole relation afterwards and you are reading a mix of old-code and
  new-code rows. Measured: `dim_course_run` merged in 0.11s because its
  change-detection predicate found nothing to reselect, so the column under test
  was never re-derived. This is not "stale or wrong state" — the state is valid, it
  just does not reflect your new code.
- `~/.ol-dbt/local.duckdb` is **shared by every worktree and session on the
  machine**. Another checkout running `dbt run` overwrites your tables with no
  warning, so do not build in one step and measure in a much later one; snapshot a
  baseline you need to keep.
- Never point `cleanup` at a shared/production schema; rely on `--dry-run` and the
  `PROTECTED_SCHEMAS` guard.
- StarRocks-native `b2b_analytics` models are the exception — they are not
  representable on DuckDB and must be QA'd on StarRocks (see `ol-dbt starrocks`).

Pair this with the `ol-dbt-fast-validation` skill (validate / impact / diff) to
QA what you build, and with `ol-dbt-migration-validation` when the question is
whether a migrated model still holds the same data as its predecessor. See
`docs/specs/DBT_WAREHOUSE_CI_QA_SPEC.md` for the broader CI/QA plan and the
zero-copy substrate details.
