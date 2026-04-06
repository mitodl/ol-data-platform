# Dimensional Model Conventions

## Architectural Role

The dimensional layer (`models/dimensional/`) is the **architectural boundary** between
data preparation and data consumption in this project.

```
staging → intermediate → [ DIMENSIONAL LAYER ] → marts → reporting
```

- **Below the boundary** (`staging/`, `intermediate/`): raw ingestion, cleaning, reshaping,
  and platform-specific business logic. Models here are subject to change as sources evolve.
- **At the boundary** (`dimensional/`): stable star-schema dimensions (`dim_*`), transaction
  fact tables (`tfact_*`), activity fact tables (`afact_*`), and bridge tables (`bridge_*`).
  These expose a clean, platform-agnostic contract to consumers.
- **Above the boundary** (`marts/`, `reporting/`): analytics-ready tables and report views
  for BI tools and external consumers. These must only reference dimensional models (or
  other mart/reporting models that themselves follow this rule).

> **Rule:** Marts and reporting models must not reference `staging/` or `intermediate/`
> models directly. Any required data that is not yet available in the dimensional layer
> should be added there first, before updating the mart or report.

---

This document covers conventions and constraints that all contributors must understand
before writing SQL against or extending the dimensional warehouse models.

---

## ⚠️ Platform is Required for Course and Course Run Identification

**A `courserun_readable_id` or `course_readable_id` is NOT globally unique.**

The same readable ID can exist on more than one platform. This is a confirmed data
quality reality, not a hypothetical: at least three course run IDs are known to exist
on both `edxorg` and `mitxonline` simultaneously (courses originally delivered on
edX.org that were later migrated to MITx Online retain their original readable IDs):

| `courserun_readable_id` | Platforms |
|-------------------------|-----------|
| `MITx/14_73x/1T2014` | edxorg, mitxonline |
| `MITx/14.73x/2013_Spring` | edxorg, mitxonline |
| `MITx/14.73x_1/1T2015` | edxorg, mitxonline |

### Rule: Always pair `courserun_readable_id` / `course_readable_id` with `platform`

Every join that uses `courserun_readable_id` or `course_readable_id` as a join key
**must** also include a `platform` constraint on both sides of the join. Omitting it
causes a fan-out (duplicate rows), and any subsequent deduplication will silently pick
an arbitrary FK value from the wrong platform.

#### ✅ Correct pattern

```sql
-- When the fact table uses an integer source_id:
left join dim_course_run
    on combined.courserun_id = dim_course_run.source_id
    and combined.platform = dim_course_run.platform

-- When the fact table uses a readable_id (edxorg / micromasters):
left join dim_course_run
    on combined.courserun_readable_id = dim_course_run.courserun_readable_id
    and dim_course_run.platform = 'edxorg'  -- both sides scoped

-- For dim_course (use primary_platform):
left join dim_course
    on combined.course_readable_id = dim_course.course_readable_id
    and dim_course.primary_platform = combined.platform
```

#### ❌ Incorrect — will silently fan-out for migrated courses

```sql
-- Missing platform constraint:
left join dim_course_run
    on combined.courserun_readable_id = dim_course_run.courserun_readable_id

left join dim_course
    on combined.course_readable_id = dim_course.course_readable_id
```

### Which dimensions require platform scoping

| Table | Natural Key | Platform column | Notes |
|-------|------------|-----------------|-------|
| `dim_course_run` | `courserun_readable_id` | `platform` | Grain is `(platform, courserun_readable_id)` |
| `dim_course` | `course_readable_id` | `primary_platform` | Grain is `(primary_platform, course_readable_id)` |
| `dim_course_content` | `(courserun_readable_id, block_id)` | `platform` | Same block can exist on 2 platforms |
| `dim_video` | `video_block_pk` | `platform` | Inherits from dim_course_content |
| `dim_problem` | `problem_block_pk` | `platform` | Inherits from dim_course_content |
| `dim_discussion_topic` | `(courserun_readable_id, commentable_id)` | `platform` | Inherits from dim_course_content |

### Platform values

Platforms used across the dimensional model:

| Value | Description |
|-------|-------------|
| `mitxonline` | MITx Online (MITxT) |
| `mitxpro` | MIT xPro |
| `edxorg` | edX.org (includes MicroMasters-era courses) |
| `residential` | MIT Residential OpenEdX |
| `micromasters` | MicroMasters program enrollment layer (cert/enrollment data only; course content lives under `edxorg`) |

Note: `micromasters` as a platform appears in enrollment and certificate fact tables
but MicroMasters courses are not a separate OpenEdX instance — their course runs and
content are stored under `platform = 'edxorg'` in dimension tables.

---

## Surrogate Key Conventions

- Surrogate keys (`*_pk`) are generated with `dbt_utils.generate_surrogate_key()`.
- For `dim_course_content`: surrogate is `(block_id, retrieved_at)` — stable across platforms since block_id encodes the course run.
- For `dim_discussion_topic`: surrogate includes `platform` — `(platform, courserun_readable_id, discussion_block_pk, commentable_id)` — because the same readable_id can exist on two platforms.
- Fact tables store integer surrogate FK references (e.g. `courserun_fk`) pointing to the `*_pk` of the appropriate dimension row.

---

## Incremental Fact Table Watermark Pattern

Incremental fact tables (`tfact_*`) use a watermark CTE to track the last processed
timestamp per platform. The correct pattern is a `LEFT JOIN` so that new platforms
(with no prior watermark) process all historical rows on first run:

```sql
left join watermarks on watermarks.platform = 'my_platform'
where (max_ts is null or event_timestamp > max_ts)
```

**Never** use a scalar subquery `(select max_ts from ... where platform = 'x')` as a
direct `WHERE` filter — if the result is NULL (first run), `event_timestamp > NULL`
evaluates to NULL (falsy) and silently drops all rows.
