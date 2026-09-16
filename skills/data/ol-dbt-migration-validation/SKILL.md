---
name: ol-dbt-migration-validation
description: >
  Prove a dbt model migration preserved its data before merging — build the
  pre-migration model and the migrated model side by side on one dev_local
  DuckDB registration, then accept or reject on evidence. Use this skill for any
  epic #2072 dimensional/mart/reporting migration, or whenever a PR re-sources a
  column from a different upstream model and a reviewer asks "are we dropping
  records?", "did the fill rate change?", or "is this 1:1?". Covers choosing the
  join key from the model's own uniqueness test, the per-column multiset diff
  that needs no key at all, fill-rate parity, and — critically — which local
  numbers are trustworthy given that ~98% of registered non-raw Glue views point
  at __dbt_tmp locations.
license: BSD-3-Clause
metadata:
  category: data
---

# Validating a dbt model migration

A migration PR moves a column's derivation from one upstream model to another
(epic #2072: marts/reporting must source from the dimensional layer, not from
`int__`/`stg__`). The risk is never syntax — it is **silently changing data**:
rows dropped, a column nulled out, a grain that fans out. `dbt build` passing
tells you nothing about any of that.

This skill is the acceptance procedure. It sits on top of two tool-driver
skills: `ol-dbt-local-dev` (register + build) and `ol-dbt-fast-validation`
(validate / impact / diff). Read this one when the question is *"is the data the
same?"* rather than *"does it compile?"*.

Run every command here through `uv run --frozen` (`uv run --frozen ol-dbt ...`,
`uv run --frozen dbt ...`) or from an activated venv — see the prerequisite at the
top of `ol-dbt-local-dev`. A stray dbt earlier on `PATH` is picked up instead and
fails with `Could not find adapter type duckdb!`, which reads like a broken
install rather than a `PATH` problem.

## Read this first: what dev_local can and cannot prove

`ol-dbt local register` stores the `metadata_location` that Glue reports for
each table. For any dbt-materialized Iceberg table that location is a
`__dbt_tmp-<uuid>` path, an artifact of dbt's create-temp-then-swap. Measured
2026-09-09 immediately after a fresh register of every layer:

| layer | views | on `__dbt_tmp` |
|---|---|---|
| raw | 1,388 | 0 |
| staging | 256 | 244 |
| intermediate | 159 | 157 |
| dimensional | 56 | 55 |
| mart | 29 | 29 |
| reporting | 30 | 29 |
| external | 110 | 110 |
| **non-raw total** | **640** | **624 (97.5%)** |

Re-registering does **not** fix this — it is the steady state, not a race you
lost. Re-measured 2026-09-14, five days later: 651 of 667 non-raw views (97.6%),
with every layer but staging identical to the table above.

A `__dbt_tmp` view either 404s (loud) or silently returns the temp table's
**accumulated snapshots**: duplicated *and* partially missing rows. Two measured
examples: `int__mitxonline__proctored_exam_grades` read 292,704 rows for 9,442
distinct (31x), `int__micromasters__dedp_proctored_exam_grades` 5.4x. By
2026-09-14 the first of those had flipped to the 404 mode — its `__dbt_tmp` path
was cleaned up in the interim, which is the churn described below, observed.

**The duplication factor is per-view and wildly heterogeneous.** Measured
2026-09-14 across all 29 registered `dim_` views in *one* registration:

| view | rows | distinct pk | ratio |
|---|---|---|---|
| `dim_ocw_resource` | 179,863 | 2,908 | **61.85x** |
| `dim_course_run` | 10,884 | 8,508 | 1.28x |
| `dim_course` | 4,321 | 4,071 | 1.06x |
| 24 others | — | — | 1.00x |
| `dim_discussion_topic`, `dim_video` | — | — | 404, pointer rot |

Two views in the same layer and the same registration, one at 1.00x and one at
61.85x. That spread is the whole reason the next rule has to be scoped to
*shared* inputs: identical views cancel, different views do not.

The consequence, and the single most important rule in this skill:

> **An absolute number read through a `glue__` view is not evidence.** Row
> counts, fill-rate percentages, and "N of M rows are NULL" figures are all
> unreliable, and several have already been quoted in PR bodies as if they were
> production facts.
>
> **A difference between two locally-built sides of the same registration is
> evidence only where both sides read the same inputs.** One registration freezes
> every Glue pointer for the duration, so a shared upstream contributes the same
> polluted rows to both sides and its duplication cancels.

**The dependency paths of a migration diverge by construction, and that is where
the cancellation argument stops holding.** A #2072 migration re-points `ref()`
from `int__`/`stg__` to the dimensional layer, so the two sides read *different*
Glue views. `override_ref` resolves each unbuilt `ref()` to its own
`glue__ol_warehouse_production_<layer>__<model>` view, and each of those carries an
independent `__dbt_tmp` snapshot with its own mix of duplicated and missing rows.
Joins and filters downstream mean the two pollutions do not offset even
approximately. Before concluding anything about the divergent path:

1. **Build the divergent upstreams locally too**, in the same invocation — add
   both the old path's models and the new path's models to `--select`. Once a
   model is built locally, `override_ref` prefers the local table over the Glue
   view (`adapter.get_relation` hit), so both sides read stable inputs derived
   from the common raw/staging layer. This is the only way to get a clean answer.
2. If a divergent upstream is too expensive to build, **label every difference
   attributable to it as unverified source noise**, not as a result. Do not accept
   *or* reject the migration on it — that is deciding on the registration artifact
   this section exists to warn about. Step 6's deduplicated mapping assertion is
   the fallback: it cancels duplication, but not rows the polluted view is
   *missing*, so a `pre_not_new` figure on a divergent path still needs (1).

So the design of the comparison is what buys you trust, not the tooling. Never
compare a local build against the production mart; compare a local build of the
old code against a local build of the new code, with the inputs to both built
locally wherever the two paths differ.

Tracked as `tk-ol-dbt-local-register-stores-dbt-tmp-metadata-po-c833a7`
(open). Re-check it before quoting the table above — if it is fixed, absolute
fill rates become meaningful and this skill's step 5 can assert much more.

## The procedure

**Run every command in this procedure from the repository root.** dbt is anchored
with `--project-dir src/ol_dbt` and file paths are written `src/ol_dbt/...`, so
nothing depends on a `cd` you may or may not have done. Mixing the two — repo-root
paths for the files created in step 2, a project-dir shell for dbt — is how the
discovery commands below end up silently producing no output.

### 1. Register immediately before the build, and never between the two sides

```bash
#!/usr/bin/env bash
set -euo pipefail
for db in staging intermediate dimensional; do   # adjust to the layers YOUR selection reaches
  uv run --frozen ol-dbt local register --database "ol_warehouse_production_$db" \
    | tee "/tmp/reg_$db.log"
  grep -q '✗ Errors: 0' "/tmp/reg_$db.log" || {
    echo "STOP: $db registration had errors — do not build" >&2
    exit 1
  }
done
```

`exit 1`, not `break`. `break` only leaves the loop and the snippet still finishes
successfully, so an unattended run prints `STOP` and then builds anyway — a gate
that reports its own failure and does not gate. Run this as a script (hence the
shebang and `set -euo pipefail`); pasting it into an interactive shell will close
that shell on error.

**Check for zero errors; `register` will not tell you.** It catches per-table
failures, counts them, prints `✗ Errors: N` in the summary, and still **exits 0**
(`commands/local_dev.py` — no `raise` or non-zero exit on that path). A table that
fails to re-register keeps its *previous* view, so the very pointer you re-registered
to refresh can still be the stale one, and an unattended run proceeds against it.
That defeats the freshness requirement below without any visible failure, so gate
the build on the count rather than on the exit status.

Two requirements pull in different directions here, and both have to hold.

**Freshness.** Register *immediately* before the invocation, not the night before.
Pointers rot fast: a registry refreshed 60 minutes earlier already 404'd on
`int__micromasters__dedp_proctored_exam_grades` and killed two mart models
(measured 2026-09-14). Over 26 hours, 336 pointers moved across three layers —
184 of 283 staging (65%), 108 of 159 intermediate (68%), 44 of 56 dimensional
(79%). The 404 is the *lucky* outcome; the silent one returns duplicated or
partial rows and reports success. See `ol-dbt-local-dev` step 2 for the full
mechanism.

**Stability.** Both sides of a comparison must come from **one** registration.
Re-registering between them swaps the sources under one side, and every difference
you then measure is drift, not code.

These are compatible only because step 3 builds both sides in a single
invocation — register just before it and you get both properties at once. So the
rule is not "register once and never again":

> Register immediately before the invocation that builds both sides. Never
> re-register between the two sides of one comparison. **If you need to rebuild,
> re-register and rebuild both sides together — never one.**

That last clause is the one to get right. Fixing a bug in your `_pre` copy and
rebuilding only that side against a fresher registry silently compares two
substrates; a validation that runs long enough to need a rebuild is also long
enough for the registry to have rotted underneath it.

Only register the layers the model actually reads. `--all-layers` pulls 1,388
raw views you do not need.

**If step 3 selects `+<upstream>`, register every source layer that tree reaches —
not just `raw`.** A full ancestor tree bottoms out in models that read `source()`,
and `override_source` derives each one's Glue database from its *declared schema*,
which is not always raw: this project declares `dimensional` and `reporting`
sources alongside `ol_warehouse_raw_data`. Any of them left unregistered fails the
build at the leaves. Check what your selection actually reaches:

```bash
DBT_PROFILES_DIR=src/ol_dbt uv run --frozen dbt ls --project-dir src/ol_dbt \
  --select +<upstream> --resource-type source -t dev_local | grep '^source:'
```

and register a layer for each. Or keep the selection shallow with `1+`, which stops
at models you have already registered a layer for.

### 2. Materialize the pre-migration model *alongside* the new one

Do not check out the old code, build, snapshot, then check the new code back
out — that costs two dbt invocations and lets the registry drift between them.
Copy the pre-migration SQL to a second model file instead:

```bash
base=$(git merge-base HEAD origin/main)          # not origin/main itself
git show "$base:src/ol_dbt/models/<layer>/<area>/<model>.sql" \
  > "src/ol_dbt/models/<layer>/<area>/<model>_pre.sql"
```

**The merge-base, not `origin/main`.** Main moves while a PR is open — measured on
this branch: 3 commits touching 7 model files — so `origin/main:<model>.sql` can
hand you someone else's edit to the same model, and your diff then reports their
change as yours. `<layer>` because reporting migrations live under
`models/reporting/`, not `models/marts/`.

Both models now exist in one graph and build in one invocation against one
registration. Delete the `_pre` file before committing — it is scaffolding, and
it has no `.yml`, so `ol-dbt validate` will flag it if you forget.

`ol-dbt local snapshot <model> --as <model>_baseline` is the alternative when
the old code is not in git (an uncommitted edit) — it freezes the current build
as a plain table, immune to pointer rot. Then diff with `--old-raw`. Prefer the
`_pre` model file when the old code is a commit away, which for a migration PR
it always is.

### 3. Build both sides in one invocation — and force-refresh anything incremental

First find what actually diverges — the refs that differ between the two sides:

```bash
m=src/ol_dbt/models/marts/<area>/<model>     # the paths step 2 created
refs() { grep -oE "ref\([\"'][^\"')]*[\"']\)" "$1" | tr -d "\"'" \
         | sed 's/^ref(//; s/)$//' | sort -u; }
diff <(refs "${m}_pre.sql") <(refs "${m}.sql")
```

**Full paths, not basenames.** Step 2 writes these files under
`src/ol_dbt/models/...`; passing bare basenames makes both `grep` calls fail, and
two failed greps produce two empty outputs that `diff` reports as identical. You
read that as "no divergent refs" and omit the upstreams — the same silent,
looks-clean failure the quote-style bug had.

**Match both quote styles.** `ref("x")` is rarer but real — 13 occurrences across
5 models as of 2026-09-15, three of them in `reporting/`, which is exactly what
#2072 migrates (e.g. `models/reporting/data_detail_problems.sql`). A
single-quote-only pattern returns *nothing* on those files, so the diff shows no
divergent refs, you omit the upstreams, and the comparison is uncontrolled —
failing silently, in the direction that looks clean.

Stripping the quotes and the `ref(...)` wrapper matters too: it compares bare
model names, so re-quoting a `ref()` between the two versions cannot show up as a
false divergence.

Then build both sides **and both divergent ancestor paths** in one invocation:

```bash
DBT_PROFILES_DIR=src/ol_dbt uv run --frozen dbt run --project-dir src/ol_dbt \
  --select "<model>_pre <model> +<old_upstream> +<new_upstream>" \
  -t dev_local --full-refresh
```

`uv run --frozen dbt`, not bare `dbt` — see the prerequisite in
`ol-dbt-local-dev`; a stray global dbt on `PATH` will be picked up instead and
fails with `No module named 'dbt.adapters.duckdb'`.

**Selecting only the new upstream is not enough**, and that is the easy mistake:
`<model>_pre` still refs the old `int__`/`stg__` model, which — left out of the
selection — resolves to its own independently polluted Glue view, and the
comparison is uncontrolled in exactly the way the header section describes. Every
divergent ancestor has to be in the selection, on both sides, back to the point
where the two paths meet.

#### When a divergent ancestor will not build on DuckDB

**For the most common #2072 target this instruction cannot be followed.**
`+dim_course_run` and `+dim_course` fail on `dev_local`: `dim_course_run.sql:191`
calls `regexp_like` raw rather than through the cross-db macro, and that is a Trino
builtin DuckDB does not have. Measured on #2686: `ERROR=3 SKIP=5`, with the model
under test among the skipped — so you are left with only the `_pre` side. Tracked
as `tk-t1-unblock-local-validation-dim-course-run-sql-1-20b1b3`; check it before
assuming you are blocked.

When an ancestor cannot build, you have not lost the validation, but you have to
narrow the claim:

1. **Reconstruct the slice you need from ancestors that do build.** Select the
   buildable part of the tree and assemble the platform slice the change touches,
   rather than the whole dimensional model.
2. **Say what that proves.** You are then verifying *the derivation* — that the new
   expression maps the same inputs to the same outputs — not the built relation.
   That is a weaker claim than a full side-by-side, and the PR body has to state it
   as such. It is still worth doing: on #2686 exactly this caught a real error.

Do not substitute a Glue view for the unbuildable ancestor and call it controlled.
That is the divergent-path case from the header section, and the comparison is
uncontrolled no matter how clean the numbers look.

`+X` pulls X's whole ancestor tree back to the sources; `1+X` stops at X's
immediate parents. Measured on `dim_course_run`: `+` selects 53 models, `1+`
selects 7. Use `1+` when the divergence is one hop and `+` when you need the
guarantee — and remember that anything you leave out is silently reading Glue.

#### Verify the build from `run_results.json`, not the log

A selected model can be **skipped** while the command looks fine. Check the
artifact, not the output:

```bash
python3 -c "
import json; d=json.load(open('src/ol_dbt/target/run_results.json'))
BAD={'error','fail','skipped','runtime error'}
bad=[(r['status'], r['unique_id'].split('.')[-1]) for r in d['results'] if r['status'] in BAD]
print('NOT OK:', bad or 'none')
"
```

Two ways the log lies, both measured 2026-09-16 while validating #2686:

1. **`| tail` hides the per-model lines.** The deprecation summary dbt prints at
   the end is longer than most `tail -n` windows, so the `ERROR creating` and
   `SKIP relation` lines scroll past and you see only deprecation noise.
2. **`| tail` also masks the exit status.** `dbt run ... | tail -3` exits **0**
   even when dbt exited 1 — you get `tail`'s status. Verified: bare run `exit=1`,
   piped `exit=0`, piped under `set -o pipefail` `exit=1`. If you must pipe, use
   `pipefail`; note `${PIPESTATUS[0]}` is a bash-ism and is empty in zsh, where the
   spelling is `${pipestatus[1]}`.

An `ERROR` in an ancestor `SKIP`s everything downstream of it, which includes the
model under test — so this failure leaves you holding only the `_pre` side, and a
comparison against a relation that was never rebuilt.

`dbt run`, not `dbt build`. `build` runs tests inline under dbt's default eager
indirect selection — the cross-model `relationships_*` noise the section below
tells you to avoid — and a failing test there skips the downstream models you
selected, leaving one comparison side unmaterialized. Test separately, cautiously,
after both sides exist.

The skip is **intermittent in this project**, which is worse than consistent.
`dbt_project.yml` sets `tests: open_learning: +error_if: ">10"`, so a test with 10
or fewer failing rows only WARNs and the downstream model still builds. Verified
2026-09-14 on dev_local: 4 failing rows → `WARN`, downstream built; 24 failing
rows → `FAIL`, `SKIP relation ..._downstream`. So `dbt build` appears to work
until a test crosses the threshold, and then a comparison side silently does not
exist.

**`--full-refresh` is not optional when the model under test is incremental.**
dbt does execute the model SQL on an incremental run — what it does *not* do is
re-derive every row. The model's own `is_incremental()` predicate decides which
rows are reselected, and any row it excludes keeps the value the **old** code
produced. Read the relation afterwards and you are reading a mix of old-code and
new-code rows — and comparing it against a `_pre` side that, being a brand-new
model name, was built in full. The two sides are not comparable at all. Check for
`materialized='incremental'` in the config block of every model you selected.

#### Incremental vs `--full-refresh`: which to use when

The distinction is **iterating** vs **concluding**, not "fresh" vs "stale".

| You are... | Use | Why |
|---|---|---|
| Re-running a model repeatedly while editing SQL | incremental (plain `dbt run`) | You want the 0.1s loop, and you only care that it executes |
| About to read the model's contents and draw a conclusion | **`--full-refresh`** | Validation, before/after diffing, confirming a fix landed |
| Checking that a changed *expression* now produces different values | **`--full-refresh`** | `is_incremental()` re-derives only the rows it reselects |

The trap is the **incremental predicate**, not key stability. Two distinct ways a
changed expression fails to show up in the relation you then read:

1. **The predicate excludes the rows.** Whatever `is_incremental()` filters on —
   a watermark, a change-detection `not exists` — the excluded rows are never
   re-derived. Historical rows in particular are usually out of scope by design,
   so a migration that changes how a column is derived for *all* time is verified
   against only the sliver the predicate happened to reselect.
2. **The predicate finds nothing to reselect, and the run is a genuine no-op that
   reports `OK`.** `dim_course_run` did exactly this on PR #2403: `OK ... in
   0.11s`, and the `semester` expression under test never ran.

   Worth knowing *why this one is not self-explanatory*: `dim_course_run`'s
   predicate (the `{% if is_incremental() %}` block) is a SCD2 change-detection
   `not exists` that compares `semester` and `passing_grade` among others, so on
   paper it should have reselected any row whose semester changed. It reselected
   nothing, and the reason was never established. **That is the lesson** — do not
   reason from the predicate to "it must have re-derived". Confirm it by reading a
   value that should have moved, or sidestep the question with `--full-refresh`.

And even when rows *are* reselected, you do not get a clean replacement.
`dim_course_run` is `delete+insert` on `unique_key=['courserun_pk',
'effective_date']` with `effective_date = current_timestamp` for new rows: the
prior row is expired and **retained**, the new derivation is appended alongside it.
The relation grows a second generation rather than swapping the first one out, so
even the columns that did re-derive are not readable by a plain `select *`.

This is not "stale or wrong incremental state". The state is perfectly valid; it
just does not reflect your new code. That is why "reserve `--full-refresh` for
when the state is stale or wrong" does not warn you — by that test, nothing is
wrong.

Check the config block of every model in your selection for
`materialized='incremental'` before deciding, and read its `is_incremental()`
block to see what it would and would not have reselected. A suspiciously fast
success on one is a warning sign, not a good sign.

#### `~/.ol-dbt/local.duckdb` is shared mutable state

One DuckDB file backs `dev_local` for **every worktree, checkout and agent
session on the machine**. Another session running `dbt run` will overwrite the
tables you just built, with no warning and no lock you will notice.

Observed while validating #2403: `dim_course_run.semester` measured 4,513 of
4,513 populated at 18:22 UTC and 12 of 4,513 an hour later, because a concurrent
session rebuilt the model from `main`. Nothing in either session reported a
problem.

Two consequences, and the first is why this skill insists on one invocation:

1. **Never build a model in one step and measure it in a later step.**
   Materialize both comparison sides as physical tables in a single `dbt run`.

   A physical table is immune to **pointer rot** — unlike a `glue__` view it does
   not depend on a metadata pointer that can go stale under it. It is **not**
   immune to another `dbt run`, which will happily rebuild it; the first sentence
   of this section says exactly that. The #2403 marts survived because the
   concurrent session rebuilt `dim_course_run`, their *upstream*, and never
   selected the marts themselves. That is targeting, not immunity.

   So one invocation narrows the window; it does not make build-plus-measure
   atomic. Before you conclude anything: confirm you have `dev_local` to yourself
   (`git worktree list`, and check claimed witan tasks), and measure immediately
   after the build rather than after a break. If a comparison genuinely must be
   protected, `HOME=/tmp/iso-validation` points `dev_local` at a private DuckDB
   file — verified 2026-09-15, the real 19GB warehouse is untouched — but that
   database starts empty, so you pay a full `ol-dbt local register` (AWS creds)
   to use it. Worth it for a result you are going to publish; overkill while
   iterating.
2. **Any figure read from this database is valid only at the instant it was
   read.** Re-read anything you intend to put in a PR body, and prefer
   `ol-dbt local snapshot` for a baseline you need to keep across a break.

#### Test only your own models: `--indirect-selection=cautious`

Use `dbt run` above, then test separately:

```bash
DBT_PROFILES_DIR=src/ol_dbt uv run --frozen dbt test --project-dir src/ol_dbt \
  --select "<model>_pre <model>" \
  -t dev_local --indirect-selection=cautious
```

**Test only the two comparison models — do not reuse step 3's selection.** That
selection deliberately includes `+<upstream>` ancestor trees so the *inputs* are
built locally; repeating it here selects every ancestor's own tests too, and
`cautious` cannot save you from tests that belong to models you explicitly named.
Measured on `dim_course_run`: `--select dim_course_run` runs **7** tests under
`cautious`, `--select "dim_course_run +dim_course_run"` runs **365**. The build and
test selections are supposed to differ — what has to match is that you *built* both
sides, not that you test the same set.

dbt defaults to `--indirect-selection=eager`, which selects every test that
merely **references** a selected model — including `relationships_*` tests
**owned by other models**. Those compare your locally rebuilt model against fact
tables still resolving to production Glue views, so they report orphans by
construction and tell you nothing about your change. One of them on #2403 scans
`tfact_grade` (42.7M rows) and runs for many minutes.

Measured on #2403's two changed models:

| mode | tests selected |
|---|---|
| `eager` (default) | 20 |
| `buildable` | 12 |
| `cautious` | 11 |

The 9 that `eager` adds were all cross-model `relationships_*` tests, and
included every failure the PR body had been documenting as expected noise.
`cautious` also drops tests the changed model *does* own when their other parent
is outside the selection (e.g. `dim_course_run` → `dim_date`); those also compare
against production tables, so losing them is an acceptable trade. `buildable` is
the middle ground.

**Do not put expected-failure counts in a PR body.** They are a function of when
you registered and how much you had built locally, not a property of the change.
#2403's body documented 87/74/13 orphans; the same tests produced 861/178/150
plus a fourth failure it never mentioned. Fix the selection instead of
documenting the noise.

### 4. Choose the join key from the model's own uniqueness test

Never guess the grain from column names. Ask dbt which file documents the model,
then read its uniqueness tests:

```bash
yml=$(DBT_PROFILES_DIR=src/ol_dbt uv run --frozen dbt ls --project-dir src/ol_dbt \
        --select <model> --resource-type model \
        --output json --output-keys patch_path -t dev_local \
        | grep '^{' | sed 's/.*models\//models\//; s/".*//' | head -1)
awk -v m="<model>" '$0=="- name: "m {f=1; print; next} f && /^- name: / {exit} f' \
  "src/ol_dbt/$yml" \
  | grep -nE 'unique_combination_of_columns|expect_compound_columns_to_be_unique|combination_of_columns|column_list|where:'
```

**Scope the extraction to your model's block.** A shared schema file documents many
models — `_marts__combined__models.yml` declares 11 models and 14 uniqueness tests —
so grepping the file tells you that *some* key exists, not which one is yours, and
picking the wrong one lands you straight in the weak-key fan-out below. The `awk`
prints from your model's `- name:` to the next one; the first uniqueness test it
shows is the model-level key, with its `where` clause if it has one.

`--project-dir` (and `DBT_PROFILES_DIR`) so this runs from the repository root like
everything else: `patch_path` comes back project-relative, and `src/ol_dbt/$yml`
rejoins it. Without the anchor there is no directory that works — from the root
`dbt ls` cannot find `dbt_project.yml`, and from `src/ol_dbt` the join doubles the
prefix.

**Do not glob for the schema file.** Two reasons, both measured 2026-09-15. A
`models/**/_<area>__models.yml` pattern does not recurse in bash without
`globstar` — it matches one directory level, while 31 of 35 model schema files sit
two or three deep (`models/marts/micromasters/...`), so it silently expands to
nothing for most marts, the primary migration scope. It works in zsh, which is
exactly the kind of difference that makes a documented command fail for the next
person. And the filename convention is not uniform: `dim_course_run`'s schema is
`_dim_course_run.yml`, not `_<area>__models.yml`. `patch_path` is authoritative for
both shapes.

**Carry the test's `where` clause into every check.** A uniqueness test proves the
key only over the rows it filters to. `dim_course_run`'s is
`where: "is_current = true"`, so the key is proven for current rows and says
nothing about expired ones — and an SCD2 relation accumulates several expired rows
per business key, all sharing `is_current = false`. Run step 5's grain check
unfiltered and they collide: demonstrated on the SCD2 shape, two changes to one
key give `6 rows / 5 distinct keys`, which reads as a fan-out and is ordinary
history. With the predicate applied it is `3 / 3`. So apply the same `where` to the
grain check, the fill-rate comparison and the diff — or pick a key proven unique
across the whole relation, and say which you did.

- A passing compound-uniqueness test **is** the key — use its column list verbatim,
  together with its `where` clause if it has one.
  Two spellings are in use here: `dbt_expectations.expect_compound_columns_to_be_unique`
  (231 occurrences) and `dbt_utils.unique_combination_of_columns` (19, including
  `dim_course_run` itself, whose key is `platform + courserun_readable_id +
  is_current` under `where: is_current = true`). Grep the dotted spellings; the
  underscored forms only appear in compiled test names.
- A single column is safe alone only with **both** `unique` and `not_null`
  passing. `unique` ignores NULLs and a single-column join cannot pair NULL keys.
- Pass a composite key as `-k a,b,c` or `-k a -k b -k c`. `-k a b c` does **not**
  work — only the first token is read as a key and the rest become positional
  args, producing a confusing `--dbt-dir` error.

**Why a wrong key is worse than no key — and which number it actually breaks.**
The two figures `ol-dbt diff` prints do not depend on the key equally, and the
intuition is backwards:

| figure | how it is computed | key-sensitive? |
|---|---|---|
| per-column mismatch rate | `compare_column_values`: `full outer join b_query on a_query.<pk> = b_query.<pk>` | **yes — corrupted by a weak key** |
| "rows without an exact match" | `compare_queries`: `EXCEPT` on full row content; `primary_key` appears only in an `order by` | no |
| sample mismatch rows | same `EXCEPT` branch (`summarize=false`) | no |

So a non-unique or nullable key fans out the per-column join and **manufactures
mismatches that do not exist**. Measured 2026-09-15 on two byte-identical 4-row
tables whose key repeated once: `0 unmatched row-side(s)` — correct — alongside
`val: 33.33% (2 rows)`, entirely an artifact of 2x2 pairing on the duplicated key.
Trusting the rate there means reporting a 33% regression on identical data.

**If the key is not proven, do not run the keyed diff at all.** Use the keyless
multiset check in step 5(b), which needs no key and cannot fan out. That is the
honest fallback — not "run it anyway and squint at the rate".

Two related notes. A single-column `--primary-key` drops NULL-keyed rows, while
the composite path hashes through `diff_composite_key`, which encodes nulls so
those rows still pair (tracked as
`tk-ol-dbt-diff-single-column-primary-key-drops-null-1d1a40`). And the unmatched
count, though key-independent, is still inflated by **source duplication** — the
16.7x on #2403 was that, not key noise.

### 5. Localise cheaply, in this order

**(a) Row-count delta first.** A nonzero delta means the grain moved, and every
per-column number is suspect until you explain it. A **zero** delta is only
*count parity* — dropped rows offset by duplicates, or by newly added ones, net to
zero just as readily as a clean migration does. Confirm the grain before reading
per-column rates as interpretable, using the model's declared uniqueness key
(step 4) on **both** relations:

```sql
-- <pred> is the uniqueness test's `where` clause, or `true` if it has none
select 'pre' as side, count(*) as n_rows, count(distinct (<key cols>)) as distinct_keys
from <pre> where <pred>
union all select 'new', count(*), count(distinct (<key cols>)) from <new> where <pred>;
```

Carry that same `<pred>` into every query in this step and into step 6 — a key
proven only under a predicate tells you nothing about the rows it excludes.

`as n_rows`, not `rows` — `rows` is a reserved word in DuckDB and the query will
not parse. Pass a composite key to `count(distinct ...)` parenthesised as
`(a, b)`; `count(distinct a, b)` is a binder error.

`n_rows = distinct_keys` on both sides, plus a zero delta, is grain intact. Anything
else is a fan-out or a collapse that count parity was hiding.

**(b) Per-column multiset diff — needs no join key at all.** Two queries per
column tell you exactly which column moved:

```sql
select count(*) from (select "<col>" from <pre> except all select "<col>" from <new>);
select count(*) from (select "<col>" from <new> except all select "<col>" from <pre>);
```

This is the highest-value step in the skill: it took an 8,891-row mystery down
to "only `semester` moved" in one pass, with no key discovery and no fan-out
risk. Reading the direction is diagnostic:

| pattern | meaning |
|---|---|
| both directions non-zero | values genuinely changed in place |
| pre-only, new side zero | the *pre* side has rows the new side lacks — often partial source data, not a code change |
| `count(*)` >> `count(distinct)` on a source | accumulated `__dbt_tmp` snapshots — see below, the ratio is not a generation count |

On that last row: `count(*) / count(distinct <key>)` is **average key
multiplicity**, and nothing more. It would equal the number of accumulated
generations only if every snapshot held exactly the same key set — and these
snapshots are partially *missing* rows as well as duplicating them, so it does
not. Measured 2026-09-14: `dim_course_run` 1.28x and `dim_ocw_resource` 61.85x are
smells of accumulation, not counts of anything. Use the ratio to decide *whether*
a view is polluted; never quote it as a generation count or divide by it to
"recover" a true row count.

**(c) Fill-rate parity per column**, as a *paired* comparison:

```sql
select 'pre' as side, count(*) as n_rows,
       count("<col>") as non_null, round(100.0*count("<col>")/count(*),2) as pct
from <pre>
union all select 'new', count(*), count("<col>"), round(100.0*count("<col>")/count(*),2) from <new>;
```

Report the **delta** between the two sides, and say explicitly that the absolute
percentages are local-only and not production fill rates. A migration that nulls
out a column shows up here as a fill-rate collapse — this is exactly how the
`dim_course_run.semester` gap was caught (42.5% of rows went `'3T2022'` → NULL).

**Watch for the null-to-null trap.** A column that is unpopulated on *both*
sides shows zero mismatches and reads like a clean migration. It is not
validated at all — it is invisible. `passing_grade` did this on #2403 while
being just as broken as `semester`. Any column whose fill rate is ~0% on both
sides must be called out as *unverified*, never as *passing*.

**(d) Only then** reach for the keyed row-level diff:

```bash
ol-dbt diff --old <model>_pre --new <model> -k <a,b,c> --exclude-columns <load timestamps>
```

**Skip this step if the uniqueness test has a `where` clause.** `ol-dbt diff` has
no filter option — `--limit` only caps how many sample rows print — so there is no
way to apply the predicate, and it compares the full relations. On a conditionally
proven key that is the weak-key case above: `dim_course_run`'s key is unique only
among `is_current = true` rows, so the expired ones duplicate the join key and the
per-column rates it prints are fan-out artifacts.

You lose nothing by skipping it. Step 5(b)'s multiset diff needs no key at all, and
step 6's mapping assertion is hand-written SQL — both take a `where` directly, so
together they cover what 5(d) would have told you, on a key you can actually
defend. Reach for `ol-dbt diff` when the key is proven across the whole relation.

### 6. Accept on the distinct functional mapping, not the whole row

The change under test is responsible for **one mapping**, not for the entire
model. Assert exactly that mapping, deduplicated, so source duplication and
time-mixing cancel:

```sql
-- list the key columns bare; `<key> as k` would alias only the last one
with p as (select distinct <key cols>, <changed_col> as v from <pre> where <pred>),
     n as (select distinct <key cols>, <changed_col> as v from <new> where <pred>)
select (select count(*) from (select * from p except select * from n)) as pre_not_new,
       (select count(*) from (select * from n except select * from p)) as new_not_pre;
```

`0 / 0` over the affected keys is the evidence that belongs in the PR body. On
#2403 that read 0/0 over 104 pairs while the whole-row diff was inflated 16.7x
by source duplication — the whole-row number was the misleading one.

`select distinct` cancels *duplicated* rows; it cannot cancel rows a polluted
`__dbt_tmp` view is **missing**. So this assertion is conclusive only over keys
present on both sides, and only if the two sides' divergent upstreams were built
locally (see the header section). A nonzero `pre_not_new` on a key the new path's
Glue view simply lacks is source noise, not a regression — establish which it is
by building that upstream, not by arguing about it.

### 7. Enumerate what legitimately differs

Run step 5(b) over distinct rows and put the resulting table in the PR, so the
next reader does not mistake known source noise for a regression. State for each
column whether the change under test touches it. A reviewer can then check your
reasoning instead of re-deriving the whole comparison.

## What to write in the PR

State the claim at the strength you actually proved:

- ✅ "Row-count delta 0 and `count(*) = count(distinct <key>)` on both sides;
  `semester` mapping identical over all N affected keys (0/0 distinct-pair diff);
  no other column changed direction."
- ✅ "Fill-rate delta 0 on all 12 columns between the two local builds."
- ❌ "Validated on dev_local, 20,908/20,908 rows identical, all columns match."
  That sentence has already been produced by a run that happened to read a
  coincidentally-consistent `__dbt_tmp` snapshot, and by one reading 16.7x
  duplicated data. It is not a claim this substrate can support.

Say which target you ran on, and that `dev_local` is DuckDB — every Trino target
in `src/ol_dbt/profiles.yml` needs LDAP/OAuth, so "verified on Trino" is not
available locally and should not be implied.

## Rules

- Register immediately before the invocation that builds both sides; never
  between the two sides. Rebuild means re-register and rebuild *both*.
- Both sides of every comparison must be locally built from the same
  registration. A local-vs-production comparison is not valid on this substrate.
- Where the old and new dependency paths diverge, build those upstreams locally
  too. Two sides reading two different `glue__` views are not a controlled
  comparison, and the pollution does not cancel between them.
- `--full-refresh` whenever the model under test is incremental — an incremental
  run re-derives only what its `is_incremental()` predicate reselects.
- A zero row-count delta is count parity, not grain integrity. Check
  `count(*) = count(distinct <key>)` on both relations before trusting
  per-column rates.
- The join key comes from the model's own passing uniqueness test, or you do not
  have one.
- A column unpopulated on both sides is *unverified*, not *passing*.
- Delete the `_pre` scaffold model before committing.
- Treat `~/.ol-dbt/local.duckdb` as shared: build and measure without leaving a
  gap, and re-read any number before quoting it.
- Quote deltas between local builds, never absolute local row counts or fill
  rates, until `tk-...-c833a7` is fixed.

See `ol-dbt-local-dev` for the register/build mechanics, `ol-dbt-fast-validation`
for `validate`/`impact`/`diff` usage, and
`docs/specs/DBT_WAREHOUSE_CI_QA_SPEC.md` for where this sits in the phased CI/QA
plan.
