# Learn Integration Schema Contract

This document defines the stable interface that MIT Learn's Trino-pull ETL tasks depend on. All Learn-facing dbt integration models follow the naming convention and column requirements specified here.

## Model Location

Integration models are located in `ol_dbt/models/integrations/learn/`. These models expose application-level contracts for data consumption, sitting above the marts layer and below the consuming application.

## Naming Convention

All Learn-facing integration models follow the pattern:

```
integrations__learn__<source>_<entity>
```

Where:
- `<source>` is the ETL source name (e.g., `ocw`, `mitxonline`, `xpro`, `mit_edx`, `micromasters`)
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
| `last_modified` | timestamp | Last modification time from source system | NOT NULL |
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

### Program Sources (MITxOnline, xPRO, MicroMasters)

| Column | Type | Description |
|--------|------|-------------|
| `courses` | string | Comma-separated list of course `readable_id` values belonging to this program |
| `departments` | string | Comma-separated department names |

### Content File Sources

| Column | Type | Description |
|--------|------|-------------|
| `content_type` | string | File type classification |
| `file_extension` | string | File extension (e.g., ".pdf", ".html") |
| `file_size` | integer | File size in bytes |
| `content` | string | Extracted text content (for embeddings) |

## Grain Expectations

- **Catalog models** (`integrations__learn__<source>_courses`, `integrations__learn__<source>_programs`): One row per resource
- **Content file models** (`integrations__learn__content_files`): One row per content file

## Nullability Rules

1. All required columns must be `NOT NULL`
2. String-aggregated columns (`topics`, `instructors`, `runs`, `courses`) may be `NULL` when no data exists; consumers should treat `NULL` as empty
3. Text columns (`description`, `url`, `image_url`) may be `NULL` when no data exists

## ETL Source Values

The `etl_source` column must match one of the following `ETLSource` enum values in MIT Learn:

- `ocw` - OCW courses
- `mitxonline` - MITx Online courses
- `xpro` - xPRO courses
- `mit_edx` - MIT edX courses
- `micromasters` - MicroMasters programs
- `oll` - Open Learning Library
- `canvas` - Canvas courses
- `youtube` - YouTube videos
- `podcasts` - Podcast episodes
- `ovs` - OVS videos

## Version History

| Date | Author | Changes |
|------|--------|---------|
| 2026-05-27 | Initial draft | Foundation schema contract |
