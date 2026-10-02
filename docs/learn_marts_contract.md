# Learn Integration Schema Contract

This document defines the stable interface that MIT Learn's Trino-pull ETL tasks depend on. All Learn-facing dbt integration models follow the naming convention and column requirements specified here.

## Model Location

Integration models are located in `src/ol_dbt/models/integrations/learn/`. These models expose application-level contracts for data consumption, sitting above the marts layer and below the consuming application.

## Naming Convention

All Learn-facing integration models follow the pattern:

```
integrations__learn__<source>_<entity>
```

Where:
- `<source>` is the ETL source name (e.g., `ocw`, `mitxonline`, `xpro`, `mit_edx`, `oll`)
- `<entity>` is the entity type (e.g., `courses`, `programs`, `content_files`)

Examples:
- `integrations__learn__ocw_courses`
- `integrations__learn__mitxonline_programs`
- `integrations__learn__content_files`

## Required Columns

Every Learn mart must expose the following columns:

| Column | Type | Description | Nullability |
|--------|------|-------------|-------------|
| `readable_id` | string | Unique identifier for the resource, used for upsert matching | NOT NULL |
| `title` | string | Human-readable title | NOT NULL |
| `last_modified` | varchar (ISO-8601) | Last modification time from source system, formatted as an ISO-8601 timestamp string | NOT NULL |
| `etl_source` | string | Source system name (must match `ETLSource` enum values) | NOT NULL |

## Optional Core Columns

These columns are optional but recommended for most marts:

| Column | Type | Description |
|--------|------|-------------|
| `description` | string | Resource description |
| `url` | string | Public URL for the resource |
| `image_url` | string | URL for the resource image/thumbnail |

## Source-Specific Columns

Each source may add additional columns following its needs. Common patterns include:

### Course Sources (OCW, MITxOnline, xPRO, MIT edX)

| Column | Type | Description |
|--------|------|-------------|
| `platform` | string | Platform name (e.g., "ocw", "xpro") |
| `topics` | string | Comma-separated topic labels (e.g., `"Engineering, Computer Science"`) |
| `instructors` | string | Comma-separated instructor names |
| `runs` | string | Semicolon-separated run records; each record is pipe-delimited: `readable_id\|start_on\|end_on\|is_live` |
| `published` | boolean | Whether the resource is published/live |

### Program Sources (MITxOnline, xPRO, MIT edX)

| Column | Type | Description |
|--------|------|-------------|
| `courses` | string | Comma-separated list of course `readable_id` values belonging to this program |
| `departments` | string | Comma-separated department names |

### Content File Sources

Content file models are one row per file, not per resource, so they do not carry
`readable_id`. Their scope key is (`etl_source`, `run_readable_id`), which a scoped
pull filters and prunes on; see
[`design/contentfile_scoped_pull_contract.md`](design/contentfile_scoped_pull_contract.md) §9.
`integrations__learn__content_files` (MITx Online and xPRO) carries:

| Column | Type | Description |
|--------|------|-------------|
| `etl_source` | string | `mitxonline` or `xpro` |
| `run_readable_id` | string | Course run id; equals `ContentFile.run.run_id` |
| `key` | string | `ContentFile.key`, the edX module id; unique within a run |
| `edx_module_id` | string | Same value as `key` |
| `title` | string | Display name, video name for a transcript, or a title made from the file name |
| `url` | string | Jump URL in the LMS, or the asset URL; null where Learn has none |
| `content` | string | Extracted text; null when extraction failed |
| `content_title` | string | Tika's metadata title; `''` until it is carried |
| `content_type` | string | Always `file` |
| `source_path` | string | Path within the course export, e.g. `course/static/handout.pdf` |
| `file_extension` | string | File extension with its dot, e.g. `.pdf` |
| `checksum` | string | MD5 of `content` |
| `extraction_status` | string | `extracted`, or `failed` for a file whose text could not be read |
| `published` | boolean | Always true |
| `last_modified` | string | ISO 8601 time the file's text was extracted |

`description`, `file_type`, `content_author`, `content_language`, `image_src` and
`uid` are present and null, as Learn's Open edX ETL leaves them unset.

## Grain Expectations

- **Catalog models** (`integrations__learn__<source>_courses`, `integrations__learn__<source>_programs`): One row per resource
- **Content file models** (`integrations__learn__content_files`): One row per content file

## Nullability Rules

1. All required columns must be `NOT NULL`
2. Content file models are exempt from the resource-level required columns; their non-null columns are `etl_source`, `run_readable_id`, `key`, `source_path` and `extraction_status`
3. String-aggregated columns (`topics`, `instructors`, `runs`, `courses`) may be `NULL` when no data exists; consumers should treat `NULL` as empty
4. Text columns (`description`, `url`, `image_url`) may be `NULL` when no data exists

## ETL Source Values

The `etl_source` column must match one of the following `ETLSource` enum values in MIT Learn:

- `ocw` - OCW courses
- `mitxonline` - MITx Online courses
- `xpro` - xPRO courses
- `mit_edx` - MIT edX courses
- `oll` - Open Learning Library
- `canvas` - Canvas courses
- `youtube` - YouTube videos
- `podcasts` - Podcast episodes
- `ovs` - OVS videos

## Version History

| Date | Author | Changes |
|------|--------|---------|
| 2026-05-27 | Initial draft | Foundation schema contract |
| 2026-08-19 | Tobias Macey | Drop `micromasters`. MIT Learn unpublished and then deleted its MicroMasters resources (`learning_resources` migrations 0117/0118) and removed `micromasters` from its `ETLSource` enum, so the source has no destination. `integrations__learn__micromasters_programs` retired with it; Cohort 1 is 6 sources, not 7. |
| 2026-10-02 | Tobias Macey | Content file columns rewritten to what `integrations__learn__content_files` carries, keyed by (`etl_source`, `run_readable_id`) per the scoped-pull contract. The earlier `file_size` column is not part of it. |
