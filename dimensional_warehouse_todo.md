# Dimensional Warehouse — Task Backlog

> Source: dimensional model review conducted 2026-03-30.
> Reference: `src/ol_dbt/models/dimensional/` (49 models), dev deployment at
> `ol_data_lake_production.ol_warehouse_production_kdelaney_dimensional` (37 tables).
>
> **Canvas is out of scope.**
>
> Tasks are ordered by dependency within each phase. Pass individual task sections
> directly to a coding agent — each contains full context, file paths, and
> acceptance criteria.

## Status Summary

| Symbol | Meaning |
|--------|---------|
| ✅ Done | Code committed to `feature/dimensional-model-expansion`; compile passes |
| ⏳ Pending | Not yet started |

**Last updated:** 2026-04-01
**Phase A:** 8/8 complete ✅
**Phase B:** 0/4 complete ⏳
**Phase C:** 0/3 complete ⏳
**Phase D:** 7/7 complete ✅
**Phase E:** 6/6 complete ✅
**Phase F:** 4/4 complete ✅
**Phase G:** 6/6 complete ✅

---

## Phase A — Unblock Production Promotion ✅ Complete

These are blockers. The dimensional layer cannot be promoted to
`ol_warehouse_production_dimensional` until all Phase A tasks are complete.

---

### A1 · Fix `dim_course` Surrogate Key Instability ✅

**File:** `src/ol_dbt/models/dimensional/dim_course.sql`

**Problem:**
The surrogate PK is generated as:
```sql
{{ dbt_utils.generate_surrogate_key([
    'course_readable_id',
    'cast(current_timestamp as varchar)'
]) }} as course_pk
```
`current_timestamp` changes on every run. Any full-refresh recreates every `course_pk`,
silently orphaning the `course_fk` column in `dim_course_run` and any future fact table
that joins to `dim_course`. This is a data integrity blocker.

**Fix:**
Replace with a stable business-key-based surrogate:
```sql
{{ dbt_utils.generate_surrogate_key(['primary_platform', 'course_readable_id']) }} as course_pk
```

The `is_current`/`effective_date`/`end_date` columns already handle SCD Type 2 history;
the PK itself must be stable across runs.

**Also update** the `new_and_changed_courses` CTE: remove `cast(current_timestamp as varchar)`
from the key generation expression. Keep `current_timestamp as effective_date` (that column
is correct — it records *when* the version became active, not the PK).

**Validation:**
```sql
-- Row count should be stable between consecutive runs (no full-refresh churn):
SELECT COUNT(*) FROM ol_warehouse_production_kdelaney_dimensional.dim_course;
-- All current course_run rows for mitxonline/mitxpro should have non-null course_fk:
SELECT COUNT(*) FROM ol_warehouse_production_kdelaney_dimensional.dim_course_run
WHERE course_fk IS NULL AND platform != 'edxorg';
-- Expected: 0
```

---

### A2 · Resolve `user_fk` in `tfact_certificate` and `tfact_payment` ✅

**Files:**
- `src/ol_dbt/models/dimensional/tfact_certificate.sql`
- `src/ol_dbt/models/dimensional/tfact_payment.sql`

**Problem:**
Both models set `user_fk = cast(null as varchar)`. `dim_user` has surrogate PKs based on
`generate_surrogate_key(['email'])`. The fact source models only carry platform-native
integer user IDs, requiring a lookup through `dim_user`.

**Fix — add a user lookup CTE to both files:**
```sql
, user_lookup as (
    select
        user_pk
        , mitxonline_application_user_id
        , mitxpro_application_user_id
        , edxorg_openedx_user_id
    from {{ ref('dim_user') }}
    where user_pk is not null
)
```

Then in the join that builds `certificates_with_fks` / `payments_with_fks`, replace the
`cast(null as varchar) as user_fk` line with:
```sql
, coalesce(
    -- mitxonline: join on application user id
    case when combined.platform = 'mitxonline'
        then (select user_pk from user_lookup where mitxonline_application_user_id = combined.user_id limit 1)
    end,
    -- mitxpro: join on application user id
    case when combined.platform = 'mitxpro'
        then (select user_pk from user_lookup where mitxpro_application_user_id = combined.user_id limit 1)
    end
) as user_fk
```

> **Note on performance:** scalar subqueries can be slow on large tables. Prefer explicit
> LEFT JOINs with separate aliases for each platform branch, or use a single
> `LEFT JOIN user_lookup u ON (platform='mitxonline' AND u.mitxonline_application_user_id=combined.user_id) OR (...)`.
> Trino supports this pattern.

**Validation:**
```sql
SELECT
    platform,
    COUNT(*) AS total,
    COUNT(user_fk) AS resolved,
    ROUND(100.0 * COUNT(user_fk) / COUNT(*), 1) AS pct_resolved
FROM ol_warehouse_production_kdelaney_dimensional.tfact_certificate
GROUP BY platform;
-- Expected: mitxonline ≥ 90%, mitxpro ≥ 90%
```

---

### A3 · Materialize `tfact_enrollment` and `tfact_order` in Dev ✅

**Files:**
- `src/ol_dbt/models/dimensional/tfact_enrollment.sql`
- `src/ol_dbt/models/dimensional/tfact_order.sql`

**Problem:**
Both SQL files exist in code but have never been materialized. They are absent from
`ol_warehouse_production_kdelaney_dimensional` (confirmed via `information_schema.tables`).
These are the two highest-priority business analytics fact tables.

**Steps:**
1. Run with full-refresh first to create the tables:
   ```bash
   cd src/ol_dbt
   dbt run --select tfact_enrollment tfact_order --full-refresh -t dev_qa
   ```
2. Check for errors in the dbt run output.
3. Validate row counts against mart layer (see below).
4. Run dbt tests:
   ```bash
   dbt test --select tfact_enrollment tfact_order -t dev_qa
   ```

**Validation:**
```sql
-- tfact_enrollment should roughly match combined enrollment mart:
SELECT platform, COUNT(*) FROM ol_warehouse_production_kdelaney_dimensional.tfact_enrollment
GROUP BY platform ORDER BY platform;

-- Compare to:
SELECT platform, COUNT(*) FROM ol_warehouse_production_mart.marts__combined_course_enrollment_detail
GROUP BY platform ORDER BY platform;

-- tfact_order: compare to mart
SELECT platform, COUNT(*) FROM ol_warehouse_production_kdelaney_dimensional.tfact_order
GROUP BY platform ORDER BY platform;

SELECT platform, COUNT(*) FROM ol_warehouse_production_mart.marts__combined__orders
GROUP BY platform ORDER BY platform;
```

Row counts need not be identical (fact tables have stricter grain) but should be in the
same order of magnitude per platform.

---

### A4 · Resolve `user_fk` in `tfact_enrollment` and `tfact_order` ✅

**Files:**
- `src/ol_dbt/models/dimensional/tfact_enrollment.sql`
- `src/ol_dbt/models/dimensional/tfact_order.sql`

**Depends on:** A2 (pattern established), A3 (tables exist)

**Problem:** Same as A2 — `user_fk = cast(null as varchar)` in both files.

**`tfact_enrollment` specifics:**
The model sources `user_id` from three platforms with different ID types:
- `mitxonline`: `user_id` maps to `dim_user.mitxonline_application_user_id`
- `mitxpro`: `user_id` maps to `dim_user.mitxpro_application_user_id`
- `edxorg`: `user_id` maps to `dim_user.edxorg_openedx_user_id`

Add the `user_lookup` CTE (same pattern as A2) and resolve `user_fk` in
`enrollments_with_fks`.

**`tfact_order` specifics:**
- `mitxonline`: `user_id` → `dim_user.mitxonline_application_user_id`
- `mitxpro`: `user_id` via `order_purchaser_user_id` → `dim_user.mitxpro_application_user_id`

**Validation:**
```sql
SELECT platform, COUNT(*) AS total, COUNT(user_fk) AS resolved
FROM ol_warehouse_production_kdelaney_dimensional.tfact_enrollment
GROUP BY platform;
```

---

### A5 · Wire `platform_fk` in All New Dimensional Fact Tables ✅

**Files:**
- `src/ol_dbt/models/dimensional/tfact_enrollment.sql`
- `src/ol_dbt/models/dimensional/tfact_order.sql`
- `src/ol_dbt/models/dimensional/tfact_certificate.sql`
- `src/ol_dbt/models/dimensional/tfact_payment.sql`
- `src/ol_dbt/models/dimensional/dim_course_run.sql`
- `src/ol_dbt/models/dimensional/dim_product.sql`

**Problem:** All of these set `platform_fk = cast(null as varchar)` with the comment
`-- dim_platform not in Phase 1-2`. `dim_platform` is fully populated in the dev schema
(5 platforms). This prevents platform-level slicing via the star schema.

**`dim_platform.platform_readable_id` values** (from production):
`mitxonline`, `mitxpro`, `edxorg`, `residential`, `bootcamps`

**Fix — add to each file:**
```sql
, dim_platform_lookup as (
    select platform_pk, platform_readable_id
    from {{ ref('dim_platform') }}
)
```

Then join and replace the `cast(null as varchar) as platform_fk` line:
```sql
left join dim_platform_lookup
    on combined.platform = dim_platform_lookup.platform_readable_id
-- In SELECT:
, dim_platform_lookup.platform_pk as platform_fk
```

**Validation:**
```sql
SELECT COUNT(*) FROM ol_warehouse_production_kdelaney_dimensional.tfact_certificate
WHERE platform_fk IS NULL;
-- Expected: 0 (all platforms have entries in dim_platform)
```

---

### A6 · Add edX.org Courses to `dim_course` ✅

**File:** `src/ol_dbt/models/dimensional/dim_course.sql`

**Depends on:** A1 (stable PK first)

**Problem:** `dim_course_run` contains 1,512 current edxorg course runs (and 1,660 total),
but all have `course_fk = NULL` because `dim_course` has no edxorg CTE. The `_dim_course.yml`
YAML lists `edxorg` as an `accepted_value` for `primary_platform`, confirming this was
intended.

**Source:** `{{ ref('int__edxorg__mitx_courseruns') }}`

Available columns in `int__edxorg__mitx_courseruns`:
- `courserun_readable_id` — e.g. `course-v1:MITx+6.002x+2T2012`
- `course_readable_id` — e.g. `course-v1:MITx+6.002x` (derived field if available; otherwise derive by stripping run segment)
- `courserun_title` → use as `course_title`
- `course_number`
- `courserun_is_published` → use as `course_is_live`

**Fix — add a CTE to `dim_course.sql`:**
```sql
, edxorg_courses as (
    select
        -- Derive course_readable_id by stripping the run segment from courserun_readable_id.
        -- course-v1:MITx+6.002x+2T2012 → course-v1:MITx+6.002x
        case
            when courserun_readable_id like 'course-v1:%'
                then substring(
                    courserun_readable_id,
                    1,
                    length(courserun_readable_id) - strpos(reverse(courserun_readable_id), '+')
                )
            else courserun_readable_id
        end as course_readable_id
        , cast(null as integer) as source_id  -- edxorg has no integer course ID
        , courserun_title as course_title
        , course_number
        , cast(null as varchar) as course_description
        , courserun_is_published as course_is_live
        , 'edxorg' as platform
    from {{ ref('int__edxorg__mitx_courseruns') }}
    qualify row_number() over (
        partition by
            case
                when courserun_readable_id like 'course-v1:%'
                    then substring(
                        courserun_readable_id,
                        1,
                        length(courserun_readable_id) - strpos(reverse(courserun_readable_id), '+')
                    )
                else courserun_readable_id
            end
        order by courserun_start_date desc nulls last
    ) = 1
)
```

Add `edxorg_courses` to the `combined_courses` UNION ALL, and update the deduplication
logic so edxorg rows don't overwrite mitxonline rows when both share the same
`course_readable_id` (priority: mitxonline > mitxpro > edxorg).

**Validation:**
```sql
SELECT platform, COUNT(*) FROM ol_warehouse_production_kdelaney_dimensional.dim_course
WHERE is_current = true GROUP BY platform;
-- Expected: edxorg row added

SELECT COUNT(*) FROM ol_warehouse_production_kdelaney_dimensional.dim_course_run
WHERE course_fk IS NULL AND platform = 'edxorg';
-- Expected: 0 (or near-zero for runs with unusual readable_id formats)
```

---

### A7 · Verify Materialization Configs on New Dimensional Models ✅

**Files to audit:** all `.sql` files in `src/ol_dbt/models/dimensional/`

**Context:** `dbt_project.yml` already configures the `dimensional/` folder with
`+schema: dimensional` and correct grants. No per-model schema config is needed.
However, incremental models require explicit `unique_key` and `incremental_strategy`.

**Audit each new model (not yet in production):**

| Model | Expected materialization | `unique_key` |
|-------|------------------------|--------------|
| `dim_course.sql` | `incremental` (SCD2) | `course_pk` |
| `dim_course_run.sql` | `incremental` (SCD2) | `courserun_pk` |
| `dim_product.sql` | `incremental` (SCD2) | `product_pk` |
| `tfact_enrollment.sql` | `incremental` | `enrollment_key` |
| `tfact_order.sql` | `incremental` | `order_key` |
| `tfact_certificate.sql` | `incremental` | `certificate_key` |
| `tfact_payment.sql` | `incremental` | `payment_key` |
| `dim_user.sql` | `table` | — |
| `dim_program.sql` | `table` | — |
| `dim_organization.sql` | `table` | — |
| `dim_instructor.sql` | `table` | — |
| `dim_date.sql` | `table` | `date_key` |
| `dim_topic.sql` | `table` | — |
| `dim_department.sql` | `table` | — |
| `dim_payment_method.sql` | `table` | — |
| `dim_certificate_type.sql` | `table` | — |
| `dim_discount_type.sql` | `table` | — |
| All `bridge_*.sql` | `table` | — |

All incremental models should use `incremental_strategy='delete+insert'` and
`on_schema_change='append_new_columns'` (consistent with existing models in the folder).

**Fix:** For any model missing the correct config block, add or update the `{{ config(...) }}`
macro at the top of the file.

**No changes needed** to `dbt_project.yml` — schema routing and grants are already correct.

---

### A8 · Add MicroMasters Requirements to `bridge_program_course` ✅

**File:** `src/ol_dbt/models/dimensional/bridge_program_course.sql`

**Problem:** `dim_program` contains 5 MicroMasters programs, but
`bridge_program_course` only sources from `int__mitxonline__program_requirements` and
`int__mitxpro__coursesinprogram`. MicroMasters program–course mappings are absent.

**Fix — add a third source CTE:**
```sql
, micromasters_requirements as (
    select
        program_id
        , course_id
        , true as is_required
        , 'micromasters' as platform
        , 'micromasters' as platform_code
    from {{ ref('int__micromasters__program_requirements') }}
)
```

Add it to the `combined_requirements` UNION ALL. The existing join to `dim_program` uses
`platform_code` which already supports `'micromasters'` (it's a platform_code in
`dim_program`). Confirm `int__micromasters__program_requirements` exposes `program_id`
and `course_id` by checking its SQL before implementing.

**Validation:**
```sql
SELECT p.platform_code, COUNT(*) AS courses_in_programs
FROM ol_warehouse_production_kdelaney_dimensional.bridge_program_course b
JOIN ol_warehouse_production_kdelaney_dimensional.dim_program p ON b.program_fk = p.program_pk
GROUP BY p.platform_code ORDER BY p.platform_code;
-- Expected: micromasters row appears with > 0 courses
```

---

## Phase B — Complete Platform Coverage ⏳

Run after all Phase A tasks are merged and the dimensional schema is promoted to production.

---

### B1 · Add Bootcamps to Dimensional Layer ⏳

**Files to modify:**
- `src/ol_dbt/models/dimensional/dim_course.sql`
- `src/ol_dbt/models/dimensional/dim_course_run.sql`
- `src/ol_dbt/models/dimensional/tfact_enrollment.sql`
- `src/ol_dbt/models/dimensional/tfact_certificate.sql`

**Depends on:** A1 (stable dim_course PK), A5 (platform_fk pattern)

**Source models available:**
- `{{ ref('int__bootcamps__courses') }}` — `course_id`, `course_title`, `course_readable_id`
- `{{ ref('int__bootcamps__course_runs') }}` — `courserun_id`, `courserun_readable_id`, `courserun_title`, `courserun_start_date`, `courserun_end_date`, `courserun_is_live`
- `{{ ref('int__bootcamps__courserunenrollments') }}` — `courserunenrollment_id`, `user_id`, `courserun_id`, `courserunenrollment_created_on`, `courserunenrollment_is_active`
- `{{ ref('int__bootcamps__courserun_certificates') }}` — `courseruncertificate_id`, `user_id`, `courserun_id`, `courseruncertificate_uuid`, `courseruncertificate_is_revoked`, `courseruncertificate_created_on`

**Pattern to follow:** Mirror the existing `mitxpro_*` CTEs in each file and use
`'bootcamps'` as the platform string. Confirm `dim_platform` has a `bootcamps` row
before referencing `platform_fk`.

**`dim_user` user ID mapping for Bootcamps:**
Bootcamps users are stored in `dim_user` but the model doesn't expose a
`bootcamps_application_user_id` column. Before resolving `user_fk` in the Bootcamps
fact CTEs, check whether `dim_user` needs a `bootcamps_application_user_id` column added
(source: `stg__bootcamps__app__postgres__auth_user`).

**Validation:**
```sql
SELECT platform, COUNT(*) FROM ol_warehouse_production_dimensional.dim_course_run
WHERE platform = 'bootcamps';
-- Expected: > 0

SELECT platform, COUNT(*) FROM ol_warehouse_production_dimensional.tfact_enrollment
WHERE platform = 'bootcamps';
-- Expected: > 0
```

---

### B2 · Add OCW to `dim_course` ⏳

**File:** `src/ol_dbt/models/dimensional/dim_course.sql`

**Depends on:** A1 (stable PK)

**Problem:** OCW has rich course metadata in intermediate models but is absent from
`dim_course`. No fact tables are needed for OCW (no enrollments or payments), but having
OCW courses in `dim_course` enables content analytics (topics, departments, instructors)
to be queried through the star schema.

**Source models available:**
- `{{ ref('int__ocw__courses') }}` or `{{ ref('stg__ocw__studio__postgres__websites_website') }}`
- OCW course metadata: `course_title`, `course_number`, `course_readable_id` (slug),
  `course_description`, `course_is_live`

**Approach:**
1. Identify which intermediate model exposes the cleanest OCW course grain
   (check `src/ol_dbt/models/intermediate/` for `int__ocw__*` files).
2. Add an `ocw_courses` CTE following the same pattern as `mitxonline_courses`.
3. OCW courses should have lowest deduplication priority (mitxonline > mitxpro > edxorg > ocw).
4. Note: OCW `course_readable_id` format differs from OpenEdX format — it is a URL slug
   (e.g., `courses/res-6-007-signals-and-systems-spring-2011`). Do not apply the
   `course-v1:` run-stripping logic to OCW.

**Validation:**
```sql
SELECT primary_platform, COUNT(*) FROM ol_warehouse_production_dimensional.dim_course
WHERE is_current = true GROUP BY primary_platform;
-- Expected: ocw row with > 0 courses (OCW has ~3,000+ courses)
```

---

### B3 · Fix `tfact_enrollment` Incremental Watermark ⏳

**File:** `src/ol_dbt/models/dimensional/tfact_enrollment.sql`

**Problem:** The current incremental filter:
```sql
where enrollment_created_on >= (select max(enrollment_created_on) from {{ this }})
   or enrollment_created_on is null
```
Only processes newly-created enrollments. It silently misses:
- Enrollment deactivations (`enrollment_is_active`: true → false)
- Mode upgrades (`audit` → `verified`)
- Status changes (`enrolled` → `unenrolled`)

`tfact_order.sql` solves this correctly using per-platform `order_updated_on` watermarks.

**Fix:** Each platform source needs an `updated_on` column or a lookback window:

- **MITxOnline:** `int__mitxonline__courserunenrollments` — check if `courserunenrollment_updated_on` is available; if so, use it as the watermark column alongside `created_on`.
- **MITxPro:** Check `int__mitxpro__courserunenrollments` for an `updated_on` field.
- **edX.org:** No `updated_on` available. Use a **lookback window** (e.g., reprocess
  last 7 days of created_on to catch any late-arriving updates).

**Recommended incremental filter pattern** (mirrors `tfact_order.sql`):
```sql
{% if is_incremental() %}
where (
    enrollment_created_on >= (
        select max(enrollment_created_on) from {{ this }}
        where platform = combined_enrollments.platform
    )
    -- Reprocess recent rows to catch status updates
    or enrollment_created_on >= current_date - interval '7' day
    or enrollment_created_on is null
)
{% endif %}
```

**Also add `on_schema_change='sync_all_columns'`** or `'append_new_columns'` to the
`config` block so future column additions (e.g. `enrollment_upgraded_on`) are handled
without a full-refresh.

**Validation:**
After deploying, verify that a known enrollment status change (query
`stg__mitxonline__app__postgres__courses_courserunenrollment` for a row where
`is_active=false`) is correctly reflected in `tfact_enrollment`.

---

### B4 · Enrich `dim_user` with edX.org Migration `user_global_id` ⏳

**File:** `src/ol_dbt/models/dimensional/dim_user.sql`

**Problem:** Only 773K of 7.4M `dim_user` rows have a `user_global_id` (10.4%). The
remaining ~6.4M are primarily edX.org-only users who have not yet linked their account
to MIT Learn. For learners who *did* migrate from edX.org to MITx Online, a crosswalk
exists in:

```
ol_warehouse_production_migration.edxorg_to_mitxonline_users
```

Available columns (check `src/ol_dbt/models/migration/edxorg_to_mitxonline_course_runs.sql`
and nearby files for the schema):
- `edxorg_user_id` (or `edxorg_openedx_user_id`)
- `mitxonline_user_id` (or `mitxonline_application_user_id`)

**Fix:** In `dim_user.sql`, after building `users_with_global_id`, add a CTE that
joins the migration table to backfill `user_global_id` for edxorg rows that matched a
MITx Online user:

```sql
, migration_crosswalk as (
    select
        edxorg_user_id
        , mitxonline_user_id
    from {{ ref('edxorg_to_mitxonline_users') }}
)

-- In the final combined_users CTE for the edxorg union branch:
-- Replace: null as user_global_id
-- With:    migration_crosswalk.user_global_id (looked up via mitxonline_user_id → learn_user_view.user_global_id)
```

This may require a two-step join: edxorg_user_id → mitxonline_user_id → user_global_id
from the MITx Online user branch.

**Validation:**
```sql
SELECT COUNT(user_global_id) AS with_global_id, COUNT(*) AS total
FROM ol_warehouse_production_dimensional.dim_user;
-- Expected: with_global_id should increase above the baseline ~773K
```

---

## Phase C — New Models and Refinements ⏳

Run after Phase B. These add analytical depth and reduce maintenance burden.

---

### C1 · Create `tfact_grade` — Course Run Grade Fact Table ⏳

**New file:** `src/ol_dbt/models/dimensional/tfact_grade.sql`
**New YAML:** Add entry to `src/ol_dbt/models/dimensional/_fact_tables.yml`

**Depends on:** A2 (user_fk pattern established)

**Purpose:** The single most important learning-outcomes question — *"did the learner
pass?"* — has no representation in the dimensional layer. `courserungrade_is_passing` exists
in the mart layer but requires a full join through `marts__combined_course_enrollment_detail`.
`tfact_grade` gives BI tools direct access to grade data from the star schema.

**Grain:** One row per (user × course run) grade record.

**Sources:**
- `{{ ref('int__mitxonline__courserun_grades') }}` — `user_id`, `courserun_id`, `courserungrade_grade`, `courserungrade_is_passing`, `courserungrade_created_on`
- `{{ ref('int__mitxpro__courserun_grades') }}` — same shape
- `{{ ref('int__edxorg__mitx_courserun_grades') }}` — `user_id`, `courserun_readable_id`, `courserungrade_grade`, `courserungrade_is_passing`, `courserungrade_created_on`

**Target schema:**
```sql
{{ config(
    materialized='incremental',
    unique_key='grade_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Columns:
grade_key               -- surrogate: generate_surrogate_key(['cast(user_id as varchar)', 'cast(courserun_id as varchar)', 'platform'])
grade_date_key          -- int FK to dim_date (from courserungrade_created_on)
user_fk                 -- varchar FK to dim_user (use pattern from A2)
courserun_fk            -- varchar FK to dim_course_run
platform_fk             -- varchar FK to dim_platform (use pattern from A5)
platform                -- varchar
grade_value             -- double (0.0–1.0)
is_passing              -- boolean
letter_grade            -- varchar (if available; null otherwise)
grade_created_on        -- varchar (for incremental watermark)
```

**Incremental strategy:** Use per-platform max on `grade_created_on` watermark. Check
whether grade records are immutable or can be updated (e.g., grade corrections).
If mutable, use a 7-day lookback like the pattern in B3.

**Validation:**
```sql
SELECT platform, COUNT(*), AVG(CAST(is_passing AS int)) AS pass_rate
FROM ol_warehouse_production_dimensional.tfact_grade
GROUP BY platform;
-- Expected: pass_rate ~0.5–0.7 for mitxonline (consistent with known stats)
```

---

### C2 · Create `tfact_program_certificate` — Program Certificate Fact Table ⏳

**New file:** `src/ol_dbt/models/dimensional/tfact_program_certificate.sql`
**New YAML:** Add entry to `src/ol_dbt/models/dimensional/_fact_tables.yml`

**Depends on:** A2 (user_fk pattern)

**Purpose:** `tfact_certificate` covers course-level certificates only. Program completion
is tracked in `marts__combined_program_enrollment_detail` but not in the dimensional layer.

**Grain:** One row per (user × program) certificate issuance event.

**Sources:**
- `{{ ref('int__mitxonline__program_certificates') }}` — `user_id`, `program_id`, `programcertificate_id`, `programcertificate_uuid`, `programcertificate_is_revoked`, `programcertificate_created_on`
- `{{ ref('int__mitxpro__program_certificates') }}` — same shape
- `{{ ref('int__micromasters__program_certificates') }}` — may use different column names; inspect before implementing

**Target schema:**
```sql
{{ config(
    materialized='incremental',
    unique_key='program_cert_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Columns:
program_cert_key        -- surrogate: generate_surrogate_key(['cast(certificate_id as varchar)', 'platform'])
cert_date_key           -- int FK to dim_date
user_fk                 -- varchar FK to dim_user
program_fk              -- varchar FK to dim_program
platform_fk             -- varchar FK to dim_platform
platform                -- varchar
certificate_uuid        -- varchar
is_revoked              -- boolean
certificate_created_on  -- varchar (for incremental watermark)
```

**Validation:**
```sql
SELECT p.program_title, COUNT(*) AS certs_issued
FROM ol_warehouse_production_dimensional.tfact_program_certificate tpc
JOIN ol_warehouse_production_dimensional.dim_program p ON tpc.program_fk = p.program_pk
GROUP BY p.program_title ORDER BY certs_issued DESC LIMIT 20;
-- Cross-check with marts__micromasters_program_certificates and
-- marts__mitxonline_course_certificates aggregated to program level
```

---

### C3 · Enrich `dim_organization` with B2B Contract Metadata ⏳

**File:** `src/ol_dbt/models/dimensional/dim_organization.sql`

**Problem:** `dim_organization` captures B2B org names but has no contract context
(contract number, seat count, validity dates). This data exists in:
- `{{ ref('stg__mitxonline__app__postgres__b2b_contractpage') }}`
- `{{ ref('int__mitxonline__b2b_contract_to_courseruns') }}`

**Approach (two options — choose one):**

**Option A — Enrich `dim_organization` directly:**
Add contract attributes as columns on `dim_organization`, using the most recent active
contract per organization. Best if each org has one active contract at a time.

```sql
-- New columns to add:
contract_number         -- varchar
contract_start_date     -- varchar
contract_end_date       -- varchar
contract_seat_count     -- integer
contract_is_active      -- boolean
```

**Option B — Create a separate `dim_contract` dimension:**
Better if orgs can have multiple concurrent contracts. One row per contract with
`organization_fk` as a FK to `dim_organization`. Also enables `bridge_organization_courserun`
to carry `contract_fk` instead of `organization_fk`.

Inspect `stg__mitxonline__app__postgres__b2b_contractpage` first to determine
cardinality (one-to-one or one-to-many org-to-contract) before deciding.

**Validation:**
```sql
SELECT COUNT(*), COUNT(contract_number) FROM ol_warehouse_production_dimensional.dim_organization
WHERE platform = 'mitxonline';
-- Expected: contract_number populated for orgs that have contracts
```

---

## Reference: Dependency Graph

```
A1 (dim_course PK) ──────────────────────────────── A6 (edxorg in dim_course)
                                                      B1 (bootcamps)
                                                      B2 (ocw)

A2 (user_fk pattern) ──── A4 (user_fk enrollment/order)
                      └─── C1 (tfact_grade)
                      └─── C2 (tfact_program_cert)

A3 (materialize tfact_enrollment + tfact_order) ─── A4

A5 (platform_fk) ────────────────────────────────── B1 (bootcamps)

[ A1 + A2 + A3 + A4 + A5 + A6 + A7 + A8 all done ] → Promote to production

B1 ──┐
B2 ──┤
B3 ──┤  → Phase B complete
B4 ──┘

C1 → C2 → C3  (independent, can run in any order)
```

---

## Phase D — Fix Structural Bugs (Data Integrity Blockers) ✅ Complete

> Source: structural audit conducted 2026-03-30.
> These bugs cause silent data loss or permanently null FKs in the already-promoted
> dimensional tables. Fix in order; D1–D3 are the highest-severity issues.

---

### D1 · Fix `user_fk` in `tfact_payment` via Order Bridge ✅

**File:** `src/ol_dbt/models/dimensional/tfact_payment.sql`

**Problem:**
Both payment source models carry `null as user_id` by design — the CyberSource/Stripe
payment gateway callback tables (`int__mitxonline__ecommerce_transaction`,
`int__mitxpro__ecommerce_receipt`) do not include user identity. The `user_lookup` join
added in Phase A resolves nothing. Every row in `tfact_payment` has `user_fk = NULL`.

**Root cause chain:**
- `int__mitxonline__ecommerce_transaction` → has `order_id`, no `user_id`
- `int__mitxpro__ecommerce_receipt` → has `order_id`, no `user_id`
- User is on the **order**, not the transaction/receipt

**Fix — add an order bridge CTE:**
```sql
, order_user_lookup as (
    select
        order_id
        , user_id
        , 'mitxonline' as platform
    from {{ ref('int__mitxonline__ecommerce_order') }}
    union all
    select
        order_id
        , order_purchaser_user_id as user_id
        , 'mitxpro' as platform
    from {{ ref('int__mitxpro__ecommerce_order') }}
)
```

Then replace the `user_lookup` LEFT JOINs with:
```sql
left join order_user_lookup as oul
    on combined_payments.order_id = oul.order_id
    and combined_payments.platform = oul.platform
left join user_lookup as ul_mitxonline
    on oul.platform = 'mitxonline' and oul.user_id = ul_mitxonline.mitxonline_application_user_id
left join user_lookup as ul_mitxpro
    on oul.platform = 'mitxpro' and oul.user_id = ul_mitxpro.mitxpro_application_user_id
```

Note: `tfact_payment` already carries `order_id` in the SELECT list, so the bridge join
column is available.

**Validation:**
```sql
SELECT platform, COUNT(*) AS total, COUNT(user_fk) AS resolved,
    ROUND(100.0 * COUNT(user_fk) / COUNT(*), 1) AS pct_resolved
FROM ol_warehouse_production_kdelaney_dimensional.tfact_payment
GROUP BY platform;
-- Expected: mitxonline ≥ 90%, mitxpro ≥ 90% (some payments may precede user account creation)
```

---

### D2 · Fix `tfact_payment` Incremental Watermark (Duplicate Rows) ✅

**File:** `src/ol_dbt/models/dimensional/tfact_payment.sql`

**Problem:**
The incremental filter uses `>=`:
```sql
where transaction_created_on >= (select max(transaction_created_on) from {{ this }})
```
With `unique_key='payment_key'` and `delete+insert` strategy this is safe, but if the
watermark row is deleted from the target before re-insertion completes, a partial window
overlap can produce duplicates. More critically, using `>=` on the watermark means every
incremental run re-processes the entire last batch, which is wasteful on large tables.

**Fix:** Use `>` (strictly greater than) and align with `tfact_order`'s per-platform
watermark pattern:
```sql
where (
    transaction_created_on > (
        select max(transaction_created_on) from {{ this }}
        where platform = combined_payments.platform
    )
    or transaction_created_on is null
)
```

---

### D3 · Fix `dim_program` — `platform_fk` Still NULL ✅

**File:** `src/ol_dbt/models/dimensional/dim_program.sql`

**Problem:**
`dim_program` was not included in the A5 scope. Line 102 still has:
```sql
cast(null as varchar) as platform_fk,  -- dim_platform not in Phase 1-2
```
This means any star-schema query that joins `dim_program.platform_fk → dim_platform`
(e.g. "programs by platform") returns no rows.

**Fix — add a `dim_platform_lookup` CTE (same pattern as A5):**
```sql
, dim_platform_lookup as (
    select platform_pk, platform_readable_id
    from {{ ref('dim_platform') }}
)
```

Then in `with_platform_fk`:
```sql
left join dim_platform_lookup
    on combined_programs.platform_readable_id = dim_platform_lookup.platform_readable_id
-- In SELECT:
, dim_platform_lookup.platform_pk as platform_fk
```

Note: `dim_program` uses `platform_readable_id` (e.g. `mitxonline`) as the join column —
confirm the alias in the source CTE before implementing.

**Validation:**
```sql
SELECT COUNT(*) FROM ol_warehouse_production_kdelaney_dimensional.dim_program
WHERE platform_fk IS NULL;
-- Expected: 0
```

---

### D4 · Fix `tfact_enrollment` Per-Platform Incremental Watermark ✅

**File:** `src/ol_dbt/models/dimensional/tfact_enrollment.sql`

**Depends on:** Phase B task B3 (tracked separately) — but the cross-platform watermark
bug is independent and should be fixed now.

**Problem:**
```sql
where enrollment_created_on >= (select max(enrollment_created_on) from {{ this }})
   or enrollment_created_on is null
```
This is a **global watermark** across all platforms. If mitxpro lags in data delivery and
its latest `enrollment_created_on` is 2 days behind mitxonline's, the next incremental
run sets the watermark to mitxonline's value and silently drops 2 days of mitxpro
enrollments.

**Fix — adopt per-platform watermark, matching `tfact_order`:**
```sql
where (
    enrollment_created_on > (
        select max(enrollment_created_on) from {{ this }}
        where platform = combined_enrollments.platform
    )
    or enrollment_created_on is null
)
```

**Note on program enrollments:** `enrollment_created_on` is null for program enrollment
rows (they carry `programenrollment_created_on` renamed). The `or enrollment_created_on
is null` clause catches these, so program enrollments are always re-evaluated on
incremental runs. This is correct behavior but adds overhead — a future optimization
could partition program vs. course enrollments into separate incremental windows.

---

### D5 · Add edX.org Certificates to `tfact_certificate` ✅

**File:** `src/ol_dbt/models/dimensional/tfact_certificate.sql`

**Problem:**
The comment "edxorg_certificates omitted - requires complex intermediate dependencies"
is outdated. `int__edxorg__mitx_courserun_certificates` already exists and exposes all
required columns.

**Available source:** `{{ ref('int__edxorg__mitx_courserun_certificates') }}`

Key columns:
- `courseruncertificate_id` → `certificate_id`
- `user_id` → maps to `dim_user.edxorg_openedx_user_id`
- `courserun_readable_id` → join to `dim_course_run` (edxorg has no integer `source_id`)
- `courseruncertificate_mode` → certificate type (use as `certificate_type_code`)
- `courseruncertificate_created_on` → `certificate_created_on`

**Fix — add an `edxorg_certificates` CTE:**
```sql
, edxorg_certificates as (
    select
        courseruncertificate_id as certificate_id
        , user_id
        , cast(null as integer) as courserun_id  -- edxorg has no integer source_id
        , courserun_readable_id              -- used for dim_course_run join
        , cast(null as varchar) as certificate_uuid
        , false as certificate_is_revoked    -- edxorg doesn't track revocations
        , courseruncertificate_created_on as certificate_created_on
        , 'edxorg' as platform
    from {{ ref('int__edxorg__mitx_courserun_certificates') }}
)
```

Add to `combined_certificates` UNION ALL. For the `dim_course_run` join, edxorg
certificates must join on `courserun_readable_id` (not `source_id`) — add an OR
condition matching the pattern used in `tfact_enrollment`:
```sql
left join dim_course_run
    on combined_certificates.platform = dim_course_run.platform
    and (
        (combined_certificates.platform != 'edxorg'
            and combined_certificates.courserun_id = dim_course_run.source_id)
        or (combined_certificates.platform = 'edxorg'
            and combined_certificates.courserun_readable_id = dim_course_run.courserun_readable_id)
    )
```

For `user_fk`, add `edxorg_openedx_user_id` to the `user_lookup` CTE and join:
```sql
case when combined_certificates.platform = 'edxorg'
    then ul_edxorg.user_pk
end
```

**New column needed in `combined_certificates` union:** add
`cast(null as varchar) as courserun_readable_id` to mitxonline/mitxpro CTEs so the
schema is consistent.

**Validation:**
```sql
SELECT platform, COUNT(*) FROM ol_warehouse_production_kdelaney_dimensional.tfact_certificate
GROUP BY platform;
-- Expected: edxorg row now appears
```

---

### D6 · Fix MicroMasters Course FK in `bridge_program_course` ✅

**File:** `src/ol_dbt/models/dimensional/bridge_program_course.sql`

**Problem:**
The `micromasters_requirements` CTE added in A8 uses `platform_code = 'micromasters'`
to join `dim_course`, but `dim_course.primary_platform` has no `'micromasters'` rows.
MicroMasters courses are edX.org courses — the `course_id` in
`int__micromasters__program_requirements` is a MicroMasters-internal integer ID, while
`dim_course` (edxorg rows) has no integer `source_id` (`cast(null as integer)`).
All MicroMasters bridge rows are silently dropped by the `INNER JOIN`.

**Fix — two-step approach:**

**Step 1:** Add MicroMasters course readable_id mapping. The MicroMasters staging table
`stg__micromasters__app__postgres__courses_course` exposes `course_edx_key` (the edX.org
course readable ID). Use this as the bridge:

```sql
, micromasters_course_keys as (
    select
        course_id
        , course_edx_key as course_readable_id
    from {{ ref('stg__micromasters__app__postgres__courses_course') }}
)
```

**Step 2:** In `micromasters_requirements`, join to `micromasters_course_keys` to get
`course_readable_id`, then join to `dim_course` on
`course_readable_id = dim_course.course_readable_id` (ignoring `primary_platform`
for micromasters, since those courses appear as `edxorg` in dim_course).

```sql
, micromasters_requirements as (
    select
        r.program_id
        , ck.course_readable_id  -- use readable_id to match dim_course (edxorg rows)
        , true as is_required
        , 'micromasters' as platform
        , 'micromasters' as platform_code
    from {{ ref('int__micromasters__program_requirements') }} r
    inner join micromasters_course_keys ck on r.course_id = ck.course_id
)
```

Then update the `dim_course` JOIN in the `bridge` CTE to handle the readable_id path:
```sql
inner join dim_course
    on (
        -- Standard path: integer source_id + platform match
        (combined_requirements.platform_code != 'micromasters'
            and combined_requirements.course_id = dim_course.source_id
            and combined_requirements.platform_code = dim_course.primary_platform)
        -- MicroMasters path: match on course_readable_id (courses are edxorg in dim_course)
        or (combined_requirements.platform_code = 'micromasters'
            and combined_requirements.course_readable_id = dim_course.course_readable_id)
    )
```

Note: `course_edx_key` format in MicroMasters may be `{org}+{course_number}` or
`course-v1:{org}+{course_number}` — validate against dim_course edxorg readable_ids
before deploying.

**Validation:**
```sql
SELECT p.platform_code, COUNT(*) AS courses_in_programs
FROM ol_warehouse_production_kdelaney_dimensional.bridge_program_course b
JOIN ol_warehouse_production_kdelaney_dimensional.dim_program p ON b.program_fk = p.program_pk
GROUP BY p.platform_code ORDER BY p.platform_code;
-- Expected: micromasters row appears with > 0 courses
```

---

### D7 · Fix `tfact_discussion_events` — Wire `platform_fk` ✅

**File:** `src/ol_dbt/models/dimensional/tfact_discussion_events.sql`

**Problem:**
`tfact_discussion_events` still has `cast(null as varchar) as platform_fk` at line 204.
This was missed by A5, which only targeted the 6 models listed in the Phase A spec.

**Fix:** Apply the same `dim_platform_lookup` CTE pattern used in A5 — add the CTE and
replace `cast(null as varchar) as platform_fk` with the joined `platform_pk`.

---

## Phase E — Missing Platform Coverage ✅

> These tasks extend the dimensional layer to cover platforms and data types that
> are currently completely absent. Each is independent.

---

### E1 · Add MicroMasters Enrollments to `tfact_enrollment` ✅

**Investigation finding:** `int__micromasters__course_enrollments` sources from
`int__mitx__courserun_enrollments_with_programs`, which in turn pulls from
`int__mitx__courserun_enrollments` (edxorg) and `int__mitxonline__courserunenrollments_with_programs`
(mitxonline). These records are already included in the `edxorg_enrollments` and
`mitxonline_enrollments` CTEs in `tfact_enrollment`. Adding a micromasters UNION would
cause duplicate enrollment rows.

**What was done instead:** E5 (residential) and the existing edxorg/mitxonline paths cover
all MicroMasters course enrollment records. To add MicroMasters *program context* to existing
enrollments, a JOIN-based approach is needed (not a union). Tracked as Obs-3 below.

**Files:**
- `src/ol_dbt/models/dimensional/tfact_enrollment.sql`
- No new staging/intermediate needed — sources already exist

**Problem:**
MicroMasters course enrollments are entirely absent from `tfact_enrollment`.
`int__micromasters__course_enrollments` exists and exposes the required columns.

**Available source:** `{{ ref('int__micromasters__course_enrollments') }}`

Key columns (from the model):
- `user_id` → MicroMasters internal user ID
- `courserun_readable_id` → join to `dim_course_run` on readable_id
- `courserunenrollment_created_on` → `enrollment_created_on`
- `platform` → already set to the correct platform value

Similarly, `int__micromasters__program_enrollments` can populate **MicroMasters program
enrollments**, which are currently omitted with this comment:
> "edxorg MicroMasters programs use a separate program ID namespace that does not match
> the program IDs in dim_program"

The fix: join through `stg__micromasters__app__postgres__dashboard_programenrollment`
(which has `micromasters_program_id`) → `dim_program` (which has a `micromasters`
`platform_code` and `source_id` = MicroMasters program integer ID).

**For `user_fk`:** MicroMasters `user_id` maps to a MicroMasters-internal integer. To
resolve to `dim_user.user_pk`, add `micromasters_user_id` as a column to `dim_user`
(see E4).

---

### E2 · Add MicroMasters Orders to `tfact_order` ✅

**Implemented:** Added `micromasters_orders` CTE from `int__micromasters__orders` and
unioned into `combined_orders`. User FK resolved via `micromasters_user_id` in `dim_user`.
`platform = 'micromasters'`, `platform_code = 'micromasters'`.

---

### E3 · Add MicroMasters Certificates to `tfact_certificate` ✅

**Implemented:** Added `micromasters_certificates` CTE from `int__micromasters__course_certificates`.
- `user_fk` resolved via email join to `dim_user` (no integer user_id in source)
- `courserun_fk` via `courserun_readable_id` join (same path as edxorg)
- `certificate_type_code = 'honor'` (MicroMasters edxorg certs are honor mode)
- Added `user_email` as carrier column to all certificate CTEs (null for non-micromasters)

---

### E4 · Add `micromasters_user_id` to `dim_user` ✅

**Implemented:** `int__mitx__users` already exposes `user_micromasters_id`. Added to:
- `mitx_users` CTE (select from `int__mitx__users`)
- `mitx_users_view`, `users_with_global_id`, `combined_users` (mitx branch)
- All other UNION branches as `null as micromasters_user_id`
- `agg_view` as `max(micromasters_user_id)`
- Final SELECT as `agg.micromasters_user_id`

---

### E5 · Add Residential Enrollments to `tfact_enrollment` ✅

**Implemented:** Added `residential_enrollments` CTE from `int__mitxresidential__courserun_enrollments`.
- `platform = 'residential'`, `platform_code = 'residential'`
- `user_fk` resolved via `residential_openedx_user_id` (already in `dim_user`)
- `courserun_fk` via `courserun_readable_id` join

---

### E6 · Add xPro Product FK to `tfact_order` via `ecommerce_line` ✅

**Implemented:** Added `mitxpro_order_product_lookup` CTE from `int__mitxpro__ecommerce_line`
joining on `order_id` for xPro rows. The `product_fk` now resolves for xPro orders via
`coalesce(mitxpro_order_product_lookup.product_id, combined_orders.product_id)` keyed
against `dim_product.source_product_id`.

---

## Phase F — Deduplication & Grain Corrections ✅

> These are design-level correctness issues. They change the semantic meaning of the
> dimension tables. Coordinate with downstream consumers before deploying.

---

### F1 · Change `dim_course` Grain to `(platform, course_readable_id)` ✅

**Implemented (Option B):** Removed cross-platform deduplication. Every
`(platform, course_readable_id)` pair is now a distinct row.

- `dim_course`: removed `deduped_courses` CTE. `current_courses = combined_courses`.
  SCD Type 2 incremental logic now keys on `(course_readable_id, primary_platform)`.
- `dim_course_run`: added `primary_platform` to the `dim_course` join to prevent fan-out.
- `bridge_program_course`: MicroMasters path scoped to `edxorg` rows only.
  Converted remaining `INNER JOINs` to `LEFT JOIN` with `WHERE IS NOT NULL` guard.

---

### F2 · Fix `dim_instructor` — Don't Merge Same-Name Instructors Across Platforms ✅

**Implemented:** Changed dedup grain from `instructor_name` to `(instructor_name, platform)`.
Surrogate key updated to `generate_surrogate_key(['instructor_name', 'primary_platform'])`.
`bridge_course_instructor` updated to join on `(instructor_name, platform)`.
All bridge joins changed from INNER to LEFT JOIN with WHERE IS NOT NULL guard.

---

### F3 · Fix `dim_topic` and `dim_department` — Same-Name Cross-Platform Merging ✅

**Implemented:** Both models changed to per-platform grain `(name, platform)`.
- `dim_topic`: surrogate key = `generate_surrogate_key(['topic_name', 'primary_platform'])`.
  Parent-topic self-join now constrained within-platform.
- `dim_department`: surrogate key = `generate_surrogate_key(['department_name', 'primary_platform'])`.
- `bridge_course_topic` and `bridge_course_department` updated to join on `(name, platform)`.

---

### F4 · Fix Bridge Tables — Replace `INNER JOIN` with `LEFT JOIN` + dbt Tests ✅

**Implemented:** All metadata bridge tables (`bridge_course_instructor`, `bridge_course_topic`,
`bridge_course_department`, `bridge_organization_courserun`) changed from INNER JOIN to
LEFT JOIN with `WHERE fk IS NOT NULL` guards. The existing `not_null` dbt tests on FK
columns will surface any unresolved rows when they appear.

---

## Staging / Raw Layer Gaps

> The following source tables are referenced by the fixes above but may not yet be
> available in the raw or staging layer. Each needs to be verified and, if absent,
> added via Airbyte ingestion or a new staging model.

### Tables Already Staged (No Raw Work Needed)

These intermediate/staging models exist and are ready to use:

| Intermediate Model | Source Staging | Status |
|--------------------|---------------|--------|
| `int__edxorg__mitx_courserun_certificates` | `stg__edxorg__bigquery__mitx_person_course` + `stg__edxorg__bigquery__mitx_user_info_combo` | ✅ exists |
| `int__mitxonline__ecommerce_order` | `stg__mitxonline__app__postgres__ecommerce_order` | ✅ exists |
| `int__mitxpro__ecommerce_order` | `stg__mitxpro__app__postgres__ecommerce_order` | ✅ exists |
| `int__mitxpro__ecommerce_line` | `stg__mitxpro__app__postgres__ecommerce_line` | ✅ exists |
| `int__micromasters__course_enrollments` | `stg__micromasters__app__postgres__*` | ✅ exists |
| `int__micromasters__program_enrollments` | `stg__micromasters__app__postgres__dashboard_programenrollment` | ✅ exists |
| `int__micromasters__orders` | `stg__micromasters__app__postgres__ecommerce_order` | ✅ exists |
| `int__micromasters__course_certificates` | `stg__micromasters__app__postgres__grades_coursecertificate` + subqueries | ✅ exists |
| `int__micromasters__users` | `stg__micromasters__app__postgres__auth_user` | ✅ exists |
| `int__mitxresidential__courserun_enrollments` | `stg__mitxresidential__openedx__courserun_enrollment` | ✅ exists |
| `stg__micromasters__app__postgres__courses_course` | (already staged) | ✅ exists — needed for D6 |

### Tables That Need a New Intermediate Model

These staging tables exist but no intermediate model exposes the right columns for the
dimensional layer:

| Missing Intermediate | Source Staging Available | Needed For |
|---------------------|--------------------------|------------|
| `int__mitxonline__ecommerce_line` | `stg__mitxonline__app__postgres__ecommerce_line` ✅ | `tfact_order` MITx Online product_fk (currently resolved via `int__mitxonline__ecommerce_order` which already handles this — verify before creating) |
| `int__mitxresidential__courserun_certificates` | No staging model yet ❌ | `tfact_certificate` residential coverage |

### Tables That Need New Staging (Airbyte / Raw Layer Work)

These source tables do not yet have staging models in the dbt project. They need either
a new Airbyte connector or a raw data export + staging model:

| Missing Staging Model | Source System | Raw Table | Needed For |
|----------------------|---------------|-----------|------------|
| `stg__mitxresidential__openedx__courserun_certificate` | MITx Residential OpenEdX (MySQL) | `certificates_generatedcertificate` | `tfact_certificate` residential, `int__mitxresidential__courserun_certificates` (new intermediate) |
| `stg__mitxresidential__openedx__courserun_grade` | MITx Residential OpenEdX (MySQL) | `grades_persistentcoursegrade` | Residential grade fact tables |

> **Note on MITx Residential:** The residential OpenEdX instance has staging for
> `courserun_enrollment` and `courserun_grade` (via
> `src/ol_dbt/models/staging/mitxresidential/`), but no certificate staging. The
> `certificates_generatedcertificate` MySQL table needs to be added to the Airbyte
> residential OpenEdX connector and staged before residential certificates can flow
> into `tfact_certificate`.

---

## New Observations (Added During Phase D)

### Obs-1 · `dim_certificate_type` may not cover edX.org certificate modes ✅

**Implemented:** Added `'honor'` (id=6) and `'credit'` (id=7) rows to `dim_certificate_type`.
The model now covers: `verified`, `professional`, `completion`, `audit`, `micromasters`,
`honor`, `credit` — all known edX.org certificate mode values.

---

### Obs-2 · `dim_program.platform_fk` is NULL for MicroMasters rows ✅

**Implemented:** In `dim_program.sql`, the `dim_platform_lookup` join now maps
`platform_code = 'micromasters'` to the `'edxorg'` platform entry via a CASE expression.
`platform_code` still remains `'micromasters'` for other join paths (bridge tables, etc).

---

### Obs-3 · MicroMasters program context missing from `tfact_enrollment` ✅

**Implemented:** Added `micromasters_program_lookup` CTE to `tfact_enrollment.sql` that maps
`courserun_readable_id` → `micromasters program_pk` (via `int__micromasters__course_enrollments`
→ `dim_program` on `platform_code = 'micromasters'`). The `program_fk` column now uses
`COALESCE(dim_program.program_pk, micromasters_program_lookup.micromasters_program_pk)`
so edxorg/mitxonline enrollment rows gain MicroMasters program context where applicable.

---

## Phase G — Grain Audit Findings ⏳

> Added 2026-04-01 after a comprehensive grain audit of all dimensional models.
> Each issue was identified by analyzing the declared grain of every intermediate model
> referenced by the dimensional layer, then tracing fan-out paths and fragile dedup patterns.

---

### G1 · Fix `bridge_program_course` MicroMasters Elective-Set Fan-Out ⏳

**File:** `src/ol_dbt/models/dimensional/bridge_program_course.sql`

**Priority:** HIGH — will cause uniqueness test failure once real data includes elective-set courses.

**Problem:**
`int__micromasters__program_requirements` has grain `(program_id, course_id, electiveset_id)`.
A single course can satisfy multiple elective sets within the same MicroMasters program.
The `micromasters_requirements` CTE in `bridge_program_course.sql` selects from this
intermediate without deduplicating on `(program_id, course_id)`. After the `dim_program`
and `dim_course` joins, multiple rows with identical `(program_fk, course_fk)` reach the
final output, violating the `unique_combination_of_columns(program_fk, course_fk)` test.

**Fix:**
In the `micromasters_requirements` CTE, deduplicate before the dim joins:

```sql
, micromasters_requirements_deduped as (
    select distinct
        program_id
        , course_readable_id
        , platform_code
        , is_required        -- or use min(is_required) if the column exists
    from micromasters_requirements
)
```

Then join `micromasters_requirements_deduped` (not the raw CTE) to `dim_program`
and `dim_course`. The bridge should represent the program → course relationship once
per pair, regardless of how many elective sets the course appears in.

**Validation:**
```bash
dbt test --select bridge_program_course -t dev_local
# unique_combination_of_columns_bridge_program_course_program_fk__course_fk should pass
```

---

### G2 · Add Defensive QUALIFY Guard to `tfact_enrollment` ⏳

**File:** `src/ol_dbt/models/dimensional/tfact_enrollment.sql`

**Priority:** MEDIUM — prevents silent data corruption on incremental runs.

**Problem:**
The final CTE is a direct pass-through from `enrollments_with_fks` (a UNION ALL of 5
platform CTEs) with no final deduplication. If any upstream intermediate develops
duplicates at its declared grain — or if incremental boundary conditions produce
off-by-one overlaps — duplicate `enrollment_key` values enter the fact table and silently
violate the `unique_key` constraint used in incremental `MERGE`.

**Fix:**
Add a QUALIFY guard to the final SELECT:

```sql
select
    <all columns>
from enrollments_with_fks
qualify row_number() over (
    partition by enrollment_key
    order by enrollment_created_on desc nulls last
) = 1
```

This is a defensive guard, not a correction of known duplicates. It ensures correctness
under any upstream grain drift without masking the root cause (which should still be
fixed upstream if it occurs).

**Validation:**
```bash
dbt test --select tfact_enrollment -t dev_local
```

---

### G3 · Add Defensive QUALIFY Guard to `tfact_certificate` + Verify edxorg Enrollment Join ⏳

**File:** `src/ol_dbt/models/dimensional/tfact_certificate.sql`
**File (investigate):** `src/ol_dbt/models/intermediate/edxorg/int__edxorg__mitx_courserun_certificates.sql`

**Priority:** MEDIUM — the edxorg path has a structural join risk; the QUALIFY guard covers all platforms.

**Problem (two parts):**

1. Same defensive dedup gap as G2 — no QUALIFY guard on the final SELECT in `tfact_certificate`.

2. Higher-risk upstream: `int__edxorg__mitx_courserun_certificates` (used as the edxorg
   certificate source) `LEFT JOINs` to `int__edxorg__mitx_courserun_enrollments` on
   `(user_username, courserun_readable_id)`. If a user has multiple enrollment records for
   the same course run (audit followed by re-enroll, etc.), this join fans out the certificate
   row. The grain of `int__edxorg__mitx_courserun_enrollments` must be verified before
   the QUALIFY guard can be considered sufficient vs. requiring an upstream fix.

**Fix:**

Step 1 — verify `int__edxorg__mitx_courserun_enrollments` grain:
```sql
-- Is (user_username, courserun_readable_id) unique?
select user_username, courserun_readable_id, count(*)
from {{ ref('int__edxorg__mitx_courserun_enrollments') }}
group by 1, 2
having count(*) > 1
limit 10;
```

If duplicates exist: add a `QUALIFY row_number() OVER (PARTITION BY user_username,
courserun_readable_id ORDER BY ...) = 1` guard inside
`int__edxorg__mitx_courserun_certificates` before the offending join.

Step 2 — regardless of step 1 result, add QUALIFY guard to `tfact_certificate` final SELECT:
```sql
qualify row_number() over (
    partition by certificate_key
    order by certificate_created_on desc nulls last
) = 1
```

**Validation:**
```bash
dbt test --select tfact_certificate -t dev_local
```

---

### G4 · Harden `int__mitxonline__courserunenrollments` DEDP Fan-Out Guard ⏳

**File:** `src/ol_dbt/models/intermediate/mitxonline/int__mitxonline__courserunenrollments.sql`

**Priority:** LOW-MEDIUM — currently protected by `select distinct`, but the pattern is fragile.

**Problem:**
The `dedp_enrollments_verified_in_mitxonline` CTE (lines ~63–80) references
`int__mitxonline__ecommerce_order`, which is at `(order_id, line_id)` grain — not order
grain. The join on `(user_id, courserun_id)` fans out for users who purchased the same
course run across multiple orders (e.g., repurchase after refund). The `select distinct
(user_id, courserun_id)` currently guards against this, but:

- The guard is non-obvious — a future refactor could silently remove it.
- Adding extra columns to the CTE without re-applying `select distinct` would re-introduce
  the fan-out.

**Fix (near-term):** Add an explicit comment above the `select distinct`:
```sql
-- IMPORTANT: int__mitxonline__ecommerce_order is at (order_id, line_id) grain.
-- A user may have ordered the same course run across multiple orders (repurchase).
-- The SELECT DISTINCT is required to collapse back to (user_id, courserun_id) grain
-- and must be preserved if this CTE is modified.
```

**Fix (medium-term):** Replace the `int__mitxonline__ecommerce_order` reference with a
direct join to `stg__mitxonline__app__postgres__ecommerce_order` +
`stg__mitxonline__app__postgres__ecommerce_line`, which avoids inheriting the line-grain
intermediate entirely. Keep `select distinct` as a deliberate safety measure.

---

### G5 · Verify `int__edxorg__mitx_courserun_certificates` Enrollment Join Grain ⏳

**Files:**
- `src/ol_dbt/models/intermediate/edxorg/int__edxorg__mitx_courserun_certificates.sql`
- `src/ol_dbt/models/intermediate/edxorg/int__edxorg__mitx_courserun_enrollments.sql`

**Priority:** LOW-MEDIUM — investigation task; may require no code change, or a QUALIFY guard.

**Problem:**
`int__edxorg__mitx_courserun_certificates` LEFT JOINs to `int__edxorg__mitx_courserun_enrollments`
on `(user_edxorg_username, courserun_readable_id)`. If the enrollment intermediate is not
uniquely grained on this pair, the join fans out each certificate row.

**Steps:**
1. Read `int__edxorg__mitx_courserun_enrollments.sql` and identify the declared grain.
2. Run the duplicate-check query above (see G3 Step 1).
3. If duplicates are possible: add a QUALIFY guard inside the enrollment intermediate
   (`QUALIFY row_number() OVER (PARTITION BY user_username, courserun_readable_id ORDER BY
   enrollment_created_on DESC NULLS LAST) = 1`) or in the certificate intermediate before
   the join.
4. If grain is already unique: close this task as confirmed-safe and add a comment to
   the certificate intermediate noting the verified grain.

**This task is a prerequisite for fully resolving G3.**

---

### G6 · Document `dim_course` edxorg QUALIFY Collapse in YAML ⏳

**File:** `src/ol_dbt/models/dimensional/_dim_course.yml`
**File:** `src/ol_dbt/models/dimensional/dim_course.sql` (comment only)

**Priority:** LOW — documentation only, no behavior change.

**Problem:**
`dim_course` uses `QUALIFY row_number() OVER (PARTITION BY course_readable_id ORDER BY
courserun_start_date DESC NULLS LAST) = 1` to collapse edxorg courserun-grain data to
course grain (because edxorg has no integer course ID — only courserun-level records).
This means the course title, course number, and `is_live` for an edxorg course are
sourced from its **most-recent course run**. This is intentional but undocumented.

**Fix:**
In `_dim_course.yml`, add to the model description:

> For edxorg, course-level attributes (title, course_number, is_live) are derived from
> the most recent course run sharing the same `course_readable_id`, because edxorg exposes
> no native course-level record. `source_id` is always null for edxorg courses.

Add to the `source_id` column description:

> Null for edxorg courses (edxorg has no integer course ID; courses are represented by
> their most-recent course run's metadata).

In `dim_course.sql`, add a comment above the edxorg `QUALIFY`:
```sql
-- edxorg has no native course table — one row per course_readable_id,
-- using the most-recent course run to represent the course.
```

---

## Updated Dependency Graph

```
Phase D (structural bugs):
D1 ─── (no deps)
D2 ─── (no deps)
D3 ─── (no deps)
D4 ─── (no deps)
D5 ─── (no deps, edxorg int already exists)
D6 ─── A6 (edxorg courses in dim_course)
D7 ─── (no deps)

Phase E (platform coverage):
E1 ─── E4 (micromasters user_id in dim_user)
E2 ─── E4
E3 ─── E4, D5 (edxorg certs pattern)
E4 ─── (no deps)
E5 ─── (no deps, residential int already exists)
E6 ─── (no deps, xpro line int already exists)

Phase F (grain corrections):
F1 ─── (independent, but coordinate with downstream)
F2 ─── (independent)
F3 ─── (independent)
F4 ─── (independent, run after E tasks to capture new platforms)

Phase G (grain audit):
G1 ─── (independent)
G2 ─── (independent)
G3 ─── G5 (verify enrollment join grain before deciding G3 scope)
G4 ─── (independent)
G5 ─── (investigation; prerequisite for G3)
G6 ─── (independent, documentation only)
```

---

## Quick-Reference: Key Files and Locations

| Resource | Path |
|----------|------|
| Dimensional SQL models | `src/ol_dbt/models/dimensional/*.sql` |
| Dimensional YAML docs | `src/ol_dbt/models/dimensional/_*.yml` |
| dbt project config | `src/ol_dbt/dbt_project.yml` |
| dbt profiles | `src/ol_dbt/profiles.yml` |
| Intermediate — combined | `src/ol_dbt/models/intermediate/combined/` |
| Intermediate — MITxOnline | `src/ol_dbt/models/intermediate/mitxonline/` |
| Intermediate — MITxPro | `src/ol_dbt/models/intermediate/mitxpro/` |
| Intermediate — edxorg | `src/ol_dbt/models/intermediate/edxorg/` |
| Intermediate — micromasters | `src/ol_dbt/models/intermediate/micromasters/` |
| Intermediate — bootcamps | `src/ol_dbt/models/intermediate/bootcamps/` |
| Intermediate — OCW | `src/ol_dbt/models/intermediate/ocw/` |
| Migration crosswalk tables | `src/ol_dbt/models/migration/` |
| Dev dimensional schema | `ol_data_lake_production.ol_warehouse_production_kdelaney_dimensional` |
| Production dimensional schema | `ol_data_lake_production.ol_warehouse_production_dimensional` |
| Run dbt in dev | `cd src/ol_dbt && dbt run --select <model> -t dev_qa` |
| Run dbt tests | `cd src/ol_dbt && dbt test --select <model> -t dev_qa` |
