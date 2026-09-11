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

## Read this first: what dev_local can and cannot prove

`ol-dbt local register` stores the `metadata_location` that Glue reports for
each table. For any dbt-materialized Iceberg table that location is a
`__dbt_tmp-<uuid>` path, an artifact of dbt's create-temp-then-swap. Measured
2026-09-09 immediately after a fresh register of all three layers:

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
lost. A `__dbt_tmp` view either 404s (loud) or silently returns the temp table's
**accumulated snapshots**: duplicated *and* partially missing rows. Two measured
examples: `int__mitxonline__proctored_exam_grades` read 292,704 rows for 9,442
distinct (31x), `int__micromasters__dedp_proctored_exam_grades` 5.4x.

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

### 1. Register once, then do not touch the registry

```bash
ol-dbt local register --database ol_warehouse_production_staging
ol-dbt local register --database ol_warehouse_production_intermediate
ol-dbt local register --database ol_warehouse_production_dimensional
```

Register **before** both builds and not again between them. Pointer churn is
roughly 90% of a layer per day, so re-registering mid-validation silently swaps
the sources under one side of your comparison and every difference you then
measure is drift, not code. Build both sides back to back for the same reason.

Only register the layers the model actually reads. `--all-layers` pulls 1,388
raw views you do not need.

### 2. Materialize the pre-migration model *alongside* the new one

Do not check out the old code, build, snapshot, then check the new code back
out — that costs two dbt invocations and lets the registry drift between them.
Copy the pre-migration SQL to a second model file instead:

```bash
git show origin/main:src/ol_dbt/models/marts/<area>/<model>.sql \
  > src/ol_dbt/models/marts/<area>/<model>_pre.sql
```

Both models now exist in one graph and build in one invocation against one
registration. Delete the `_pre` file before committing — it is scaffolding, and
it has no `.yml`, so `ol-dbt validate` will flag it if you forget.

`ol-dbt local snapshot <model> --as <model>_baseline` is the alternative when
the old code is not in git (an uncommitted edit) — it freezes the current build
as a plain table, immune to pointer rot. Then diff with `--old-raw`. Prefer the
`_pre` model file when the old code is a commit away, which for a migration PR
it always is.

### 3. Build both sides in one invocation — and force-refresh anything incremental

```bash
cd src/ol_dbt && DBT_PROFILES_DIR=$(pwd) dbt run \
  --select <model>_pre <dimensional_model> <model> \
  -t dev_local --full-refresh
```

`dbt run`, not `dbt build`. `build` runs tests inline under dbt's default eager
indirect selection — the cross-model `relationships_*` noise the section below
tells you to avoid — and a failing test there can skip the downstream models you
selected, leaving one comparison side unmaterialized. Test separately, cautiously,
after both sides exist.

**`--full-refresh` is not optional when the model under test is incremental.**
dbt does execute the model SQL on an incremental run — what it does *not* do is
re-derive every row. The model's own `is_incremental()` predicate decides which
rows are reselected, and any row it excludes keeps the value the **old** code
produced. Read the relation afterwards and you are reading a mix of old-code and
new-code rows — and comparing it against a `_pre` side that, being a brand-new
model name, was built in full. The two sides are not comparable at all. A
suspiciously fast "success" on an incremental model is a warning sign, not a good
one. Check for
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
   Table-materialized output is immune once written — the #2403 comparison
   survived the clobbering above precisely because both marts were already
   physical tables.
2. **Any figure read from this database is valid only at the instant it was
   read.** Re-read anything you intend to put in a PR body, and prefer
   `ol-dbt local snapshot` for a baseline you need to keep across a break.

#### Test only your own models: `--indirect-selection=cautious`

Use `dbt run` above, then test separately:

```bash
dbt test --select <model>_pre <dimensional_model> <model> \
  -t dev_local --indirect-selection=cautious
```

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

Never guess the grain from column names. Grep the model's schema YAML:

```bash
grep -rn -A6 'expect_compound_columns_to_be_unique\|unique' \
  src/ol_dbt/models/**/_<area>__models.yml
```

- A passing `dbt_expectations.expect_compound_columns_to_be_unique` **is** the
  key — use its column list verbatim. (Grep the dotted spelling; the underscored
  form only appears in compiled test names.)
- A single column is safe alone only with **both** `unique` and `not_null`
  passing. `unique` ignores NULLs and a single-column join cannot pair NULL keys.
- Pass a composite key as `-k a,b,c` or `-k a -k b -k c`. `-k a b c` does **not**
  work — only the first token is read as a key and the rest become positional
  args, producing a confusing `--dbt-dir` error.

Why a wrong key is worse than no key: `audit_helper` pairs rows with a full
outer join, so a non-unique or nullable key fans out or fails to pair, and
reports the damage as "rows without an exact match on the other side" — a
number that reads like a catastrophic regression and is pure artifact. One
measured case reported 17,782 unmatched of 20,908 rows, all of it join noise
from a guessed key. Under a weak key, trust the per-column mismatch *rate* and
the sample rows, never the unmatched count.

### 5. Localise cheaply, in this order

**(a) Row-count delta first.** A nonzero delta means the grain moved, and every
per-column number is suspect until you explain it. A **zero** delta is only
*count parity* — dropped rows offset by duplicates, or by newly added ones, net to
zero just as readily as a clean migration does. Confirm the grain before reading
per-column rates as interpretable, using the model's declared uniqueness key
(step 4) on **both** relations:

```sql
select 'pre' side, count(*) rows, count(distinct (<key cols>)) distinct_keys from <pre>
union all select 'new', count(*), count(distinct (<key cols>)) from <new>;
```

`rows = distinct_keys` on both sides, plus a zero delta, is grain intact. Anything
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
| `count(*)` >> `count(distinct)` on a source | accumulated `__dbt_tmp` snapshots; divide to get the generation count |

**(c) Fill-rate parity per column**, as a *paired* comparison:

```sql
select 'pre' side, count(*) rows,
       count("<col>") non_null, round(100.0*count("<col>")/count(*),2) pct
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
ol-dbt diff --old <model>_pre --new <model> -k <key cols> --exclude-columns <load timestamps>
```

### 6. Accept on the distinct functional mapping, not the whole row

The change under test is responsible for **one mapping**, not for the entire
model. Assert exactly that mapping, deduplicated, so source duplication and
time-mixing cancel:

```sql
with p as (select distinct <key> as k, <changed_col> as v from <pre>),
     n as (select distinct <key> as k, <changed_col> as v from <new>)
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

- Register once, before both builds; never between them.
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
