# Raw Table Naming Conventions

This document defines the naming convention for all raw data tables in the
`ol_warehouse_raw_data` Iceberg schema. It applies to every table produced by
dlt pipelines in `dg_projects/data_loading`, Airbyte syncs, and any other
process that writes to the raw layer. The same names appear as `source` table
references in dbt staging models.

Consistent naming lets engineers and agents navigate the warehouse without
prior knowledge of specific sources — the table name encodes where the data
came from and how it was ingested.

---

## Pattern

```
raw__{source_system}[__{subsystem}]__{storage_technology}[__{namespace}]__{entity_name}
```

All segments are **lowercase**, separated by double underscores (`__`). Single
underscores are used within a segment (e.g., `auth_user`, `tracking_logs`).

| Segment | Required | Description |
|---|---|---|
| `raw` | Always | Marks the raw / landing zone layer |
| `source_system` | Always | The application or external service that owns the data |
| `subsystem` | When applicable | The internal component within the source system (see below) |
| `storage_technology` | Always | The database engine, file format, or protocol used to store or deliver the data |
| `namespace` | When applicable | An optional grouping within the storage layer (e.g., `tables` for S3 database exports) |
| `entity_name` | Always | The table, resource, or entity name within the source |

### When to include `subsystem`

Include `subsystem` when the source system has distinct internal components
that each own different entities. Omit it when the source is a flat service
with a single data store.

| Example | Source | Subsystem | Reason |
|---|---|---|---|
| `raw__mitxonline__app__postgres__courses_course` | MITx Online | `app` | Django application database, distinct from OpenEdX MySQL |
| `raw__mitxonline__openedx__mysql__auth_user` | MITx Online | `openedx` | OpenEdX LMS database, distinct from the Django app |
| `raw__ocw__studio__postgres__websites_website` | OCW | `studio` | OCW Studio application, distinct from the OCW website |
| `raw__edxorg__s3__tracking_logs` | edX.org | *(omitted)* | Flat S3 export with no internal component distinction |
| `raw__ovs__postgres__ui_video` | OVS | *(omitted)* | Single PostgreSQL database, no meaningful subsystem |

### `storage_technology` values in use

| Value | Meaning |
|---|---|
| `postgres` | PostgreSQL database (via Airbyte) |
| `mysql` | MySQL database (via Airbyte) |
| `s3` | Files on Amazon S3 (CSV, TSV, JSON, Parquet) |
| `api` | REST or JSON API endpoint |
| `rss` | RSS / Atom feed |
| `google_sheets` | Google Sheets CSV export |
| `bigquery` | Google BigQuery (via IRx or partner data delivery) |

---

## Examples by source

### Application databases (Airbyte → Iceberg)

```
raw__mitxonline__app__postgres__courses_course
raw__mitxonline__app__postgres__ecommerce_order
raw__mitxonline__openedx__mysql__grades_persistentcoursegrade
raw__xpro__app__postgres__courses_courserun
raw__xpro__openedx__mysql__student_courseenrollment
raw__micromasters__app__postgres__courses_program
raw__ocw__studio__postgres__websites_websitecontent
raw__ovs__postgres__ui_video
```

### edX.org S3 exports (IRx)

```
raw__edxorg__s3__tracking_logs
raw__edxorg__s3__tables__student_courseenrollment
raw__edxorg__s3__tables__certificates_generatedcertificate
raw__edxorg__s3__course_blocks
raw__edxorg__s3__program
raw__edxorg__s3__mitx_course
raw__edxorg__s3__mitx_course_run
```

### edX.org API (via dlt)

```
raw__edxorg__discovery__api__programs      ← MIT-authored programs from the edX Discovery API
                                             (subsystem=discovery, storage_technology=api)
```

### MIT Learn catalog sources (via dlt)

```
raw__mit_climate__api__articles            ← MIT Climate Portal Explainers + Ask MIT Climate
raw__mitpe__api__courses                   ← MIT Professional Education course feed
raw__oll__google_sheets__courses           ← Open Learning Library (Google Sheets CSV export)
```

### Media / feeds (via dlt)

```
raw__podcast__rss__channels                ← MIT Learn podcast channels from RSS
raw__podcast__rss__episodes                ← MIT Learn podcast episodes from RSS
```

### External partner data

```
raw__irx__edxorg__bigquery__mitx_user_info_combo
raw__irx__edxorg__bigquery__email_opt_in
raw__emeritus__bigquery__api_enrollments
raw__global_alumni__bigquery__api_enrollments
```

---

## Applying the convention in dlt pipelines

Set the `name` in each `@dlt.resource` decorator:

```python
@dlt.resource(
    name="raw__mitpe__api__courses",   # ← full canonical name
    primary_key=["title", "url"],
    write_disposition="replace",
)
def courses() -> Generator[dict[str, Any], None, None]:
    ...
```

dlt writes the Iceberg table using this name exactly. The asset key in Dagster
is then `["ol_warehouse_raw_data", "raw__mitpe__api__courses"]`.

In the `defs.yaml` for `DltLoadCollectionComponent`, set
`key: "{{ resource.name }}"` so the Dagster asset key matches:

```yaml
translation:
  key: "{{ resource.name }}"
  key_prefix: "ol_warehouse_raw_data"
```

---

## Applying the convention in dbt source YAML

Reference the raw table as the `identifier` inside the source definition for
the staging layer:

```yaml
sources:
  - name: ol_warehouse_raw_data
    tables:
      - name: raw__mitpe__api__courses
        identifier: raw__mitpe__api__courses
        description: "MIT Professional Education courses from the /feeds/courses/ API."
```

---

## Naming new tables

When adding a new dlt source or Airbyte sync, apply the following checklist:

1. **Identify `source_system`** — use the canonical short name for the
   application or external service (e.g., `mitxonline`, `edxorg`, `mitpe`).
   Match the name already in use if this source has existing tables.

2. **Determine whether `subsystem` is needed** — add it only if the source
   system has multiple distinct internal components (e.g., a Django app
   database vs. its embedded OpenEdX MySQL database).

3. **Choose `storage_technology`** from the values table above. If the
   technology is new, add a row to the table in this document.

4. **Name the `entity`** after the table, endpoint, or resource — use the
   upstream name where possible (e.g., `courses_courserun` mirrors the Django
   model name `courses.CourseRun`). For API resources with no table name, use
   a descriptive singular noun (e.g., `articles`, `programs`).

5. **Check for conflicts** — grep for `raw__{source_system}__` in
   `src/ol_dbt/models/staging/` to confirm the name is not already taken by a
   different table.
