# irx__ models → Simeon file mapping

Spec artifact for [#2359](https://github.com/mitodl/ol-data-platform/issues/2359). Verified
2026-08-31 against the delivered files in the legacy buckets, the raw Iceberg tables, and
[MIT-IR/simeon](https://github.com/MIT-IR/simeon) at `main`.

## The two contracts are not the same, and this is the open question

`README.md` in this directory records that the `irx__` models "were written to match the SQL file
output of the edx-analytics-exporter", deliberately breaking with "a legacy interface that is no
longer being used".

On 2026-07-07 IRx replied on #2359: *"It would be great if the file format stays the same as the
legacy data exports."*

Those are two different targets. None of the six legacy CSV filenames appears anywhere in the
Simeon source tree, and the legacy per-deployment layout is not the per-course layout Simeon walks.
So "the same format" cannot mean both. **Resolve this with IRx before building the facade's tabular
assets.** Everything below documents both contracts so the conversation has something concrete
under it.

## What Simeon actually expects

`simeon/report/utilities.py` defines `TARGET_FILES` and `_course_has_all_files(folder, ...)`, which
checks *"a given course's folder from unpacking the SQL archive"*. The unit is a **course run**, not
a deployment:

```
{course_dir}/
├── auth_user-analytics.sql
├── auth_userprofile-analytics.sql
├── certificates_generatedcertificate-analytics.sql
├── course-analytics.xml.tar.gz
├── course_structure-analytics.json
├── courseware_studentmodule-analytics.sql
├── django_comment_client_role_users-analytics.sql
├── forum.mongo
├── grades_persistentcoursegrade-analytics.sql
├── grades_persistentsubsectiongrade-analytics.sql
├── student_courseaccessrole-analytics.sql
├── student_courseenrollment-analytics.sql
├── user_id_map-analytics.sql
└── ora/{table}-analytics.sql        (carried by the unpacker; no report query reads these)
```

`format_sql_filename` (`simeon/download/utilities.py:181`) is what produces that layout from an
edx.org GPG-encrypted bundle. The naming rule for our purposes is just
`{table}-analytics.sql`, with the forum dump named `forum.mongo`.

## Coverage: `irx__` model → Simeon filename

The `irx__` model names were chosen to line up with `TARGET_FILES`, so the mapping is a pure name
transform: `irx__{deployment}__openedx__mysql__{table}` → `{table}-analytics.sql`.

| Simeon filename | `irx__` model (`{table}` part) | mitx | xpro | mitxonline |
|---|---|:--:|:--:|:--:|
| `auth_user-analytics.sql` | `auth_user` | ✅ | ✅ | ✅ |
| `auth_userprofile-analytics.sql` | `auth_userprofile` | ✅ | ✅ | ✅ |
| `certificates_generatedcertificate-analytics.sql` | `certificates_generatedcertificate` | ✅ | ✅ | ✅ |
| `courseware_studentmodule-analytics.sql` | `courseware_studentmodule` | ❌ | ❌ | ❌ |
| `django_comment_client_role_users-analytics.sql` | `django_comment_client_role_users` | ✅ | ✅ | ✅ |
| `grades_persistentcoursegrade-analytics.sql` | `grades_persistentcoursegrade` | ✅ | ✅ | ✅ |
| `grades_persistentsubsectiongrade-analytics.sql` | `grades_persistentsubsectiongrade` | ✅ | ✅ | ✅ |
| `student_courseaccessrole-analytics.sql` | `student_courseaccessrole` | ✅ | ✅ | ✅ |
| `student_courseenrollment-analytics.sql` | `student_courseenrollment` | ✅ | ✅ | ✅ |
| `user_id_map-analytics.sql` | `user_id_map` | ✅ | ✅ | ✅ |
| `course-analytics.xml.tar.gz` | — (openedx `course_xml` asset) | ✅ | ✅ | ✅ |
| `course_structure-analytics.json` | — (openedx `course_structure` asset) | ✅ | ✅ | ✅ |
| `forum.mongo` | — (see the forum track) | ❌ | ❌ | ❌ |

Ten of the thirteen are one rename away. The `assessment_*`, `submissions_*` and `workflow_*`
models map into `ora/` in the same way; no Simeon report query reads them, so they are carried, not
consumed.

## Legacy CSV → `irx__` model, column by column

Headers below were read from the delivered objects (`20260830/*.csv` in each legacy bucket), not
from the query builders. **They are byte-identical across all three deployments**, and so are the
`irx__` model select lists, so this mapping is deployment-invariant.

### `users_query.csv`
`id,username,first_name,last_name,email,is_staff,is_active,is_superuser,last_login,date_joined,course_id`

`irx__{d}__openedx__mysql__auth_user` emits 22 columns: the 10 shared ones plus `pass_word`,
`status`, `email_key`, `avatar_type`, `country`, `show_country`, `date_of_birth`,
`interesting_tags`, `ignored_tags`, `email_tag_filter_strategy`, `display_tag_filter_strategy`,
`consecutive_days_visit_count` — all hardcoded `''`/`0`. Those are the Askbot forum-user columns
the edX SQL bundle's `auth_user` carries, which is the contract the model was built for.

**Gap:** `course_id` is not selected. The model already inner-joins `student_courseenrollment`, so
one row is emitted per (user, enrollment) — but with no `course_id` column those rows are
indistinguishable duplicates. One-line fix; the join is already there.

### `enrollment_query.csv`
`id,user_id,course_id,created,is_active,mode`

`irx__{d}__openedx__mysql__student_courseenrollment` emits `course_id, mode, id, is_active`.

**Gaps:** `user_id` and `created` are missing. Both exist on
`raw__{d}__openedx__mysql__student_courseenrollment` (`id, mode, created, user_id, course_id,
is_active`), so this is two added lines. `user_id` is load-bearing — an enrollment file without it
is not usable.

### `role_query.csv`
`id,user_id,org,course_id,role` — from `student_courseaccessrole` (course *staff* access roles).

`irx__{d}__openedx__mysql__student_courseaccessrole` emits `org, course_id, user_id, role`.

**Gap:** `id` is missing. Present on the raw table. One line.

Note `student_courseaccessrole.org` is free text and drifts in case from the course key (mitxonline
production has both `MITxt` and `MITxT`). Use the column, do not derive it.

### `role_users.csv`
`id,user_id,org,course_id,role` — same header as `role_query.csv`, different meaning: these are
*forum* roles (Student, Moderator, Community TA), from `django_comment_client_role_users`.

`irx__{d}__openedx__mysql__django_comment_client_role_users` emits `course_id, user_id, name`.

**Gaps:**
- `id` — present on `raw__{d}__openedx__mysql__django_comment_client_role_users`. One line.
- `name` → `role` — a rename.
- `org` — **not derivable from anything currently ingested.** The legacy op joins
  `organizations_organizationcourse` and `organizations_organization`; neither table exists in
  `ol_warehouse_production_raw` for any deployment. This is an *ingestion* gap, not a modelling one.

  Deriving `org` from the course key is a 99.55% approximation, not an equivalence: measured against
  the delivered mitxonline `role_users.csv` (549,130 rows), 2,476 rows disagree, because the
  organization a course belongs to is not always its course-key org (e.g.
  `course-v1:UAI_ET+UAI.1+2025_C503` belongs to organization `UAI_SOURCE`). Either ingest the two
  tables or agree the approximation with IRx explicitly.

### `studentmodule_query.csv`
`id,module_type,module_id,student_id,state,grade,created,modified,max_grade,done,course_id`

No `irx__` model in any deployment. **But this is the cheapest gap on the list:**
`raw__{d}__openedx__mysql__courseware_studentmodule` exists in all three deployments with exactly
those eleven columns, and `simeon/upload/schemas/schema_studentmodule.json` declares exactly those
eleven fields in exactly that order. Legacy CSV, raw table and Simeon schema all agree — the model
is a plain `select` over the raw table.

Size caution: `legacy_openedx` carries a 32Gi memory limit in ol-infrastructure specifically
"because of studentmodule loading to memory". `get_dbt_model_as_dataframe` returns a
`pl.LazyFrame`, so the export asset must `sink_csv` it rather than `.collect().write_csv()` the way
`b2b_organization/assets/data_export.py` does.

### `course_ids.csv`
`course_id` — single column. No `irx__` equivalent, and there does not need to be one. The legacy
pipeline builds it from the Open edX course API via `list_courses`; the `openedx` code location
already does the same enumeration in `sensors/openedx.py::course_run_sensor`, which calls
`get_edx_course_ids()` and registers every course run as a dynamic partition
(`{deployment}_openedx_course_run`). `course_ids.csv` is that partition set serialised — no new API
call and no new source.

## Summary of gaps

| gap | kind | cost |
|---|---|---|
| `courseware_studentmodule` model absent (×3) | modelling | plain `select`; raw table already matches the Simeon schema exactly |
| `auth_user` missing `course_id` | modelling | one line; join already present |
| `student_courseenrollment` missing `user_id`, `created` | modelling | two lines |
| `student_courseaccessrole` missing `id` | modelling | one line |
| `django_comment_client_role_users` missing `id`, `name`→`role` | modelling | two lines |
| `django_comment_client_role_users` missing `org` | **ingestion** | needs `organizations_organizationcourse` + `organizations_organization`, or an agreed 99.55% approximation |
| `course_ids.csv` has no warehouse source | none | already available as the `{deployment}_openedx_course_run` dynamic partition set |
| `forum.mongo` replacement | modelling | 12 `forum_*` tables are ingested but all `modeled: false` |

The issue body calls the `irx__` set "a **superset**" of the six legacy CSVs. It is not. Correct
that claim whenever #2359 is next updated.

## Why this all lands in the `openedx` code location

Decided 2026-08-31. The `irx_export` code location #2359 proposes is blocked by the code-location
freeze, and `openedx` already holds three of the thirteen Simeon files locally — `course_xml`,
`course_structure`, and the course-run enumeration that `course_ids.csv` is. The remaining ten come
from Glue via `get_dbt_model_as_dataframe`. The facade needs its own S3 io-manager key, because
`openedx`'s `s3file_io_manager` is bound to the landing-zone bucket
(`openedx/definitions.py:155`), not to the IRx bucket.
