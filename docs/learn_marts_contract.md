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
- `integrations__learn__ocw_content_files`

## Required Columns

Every catalog model (courses and programs) must expose the following columns.
Content file and media models are keyed differently and are exempt; their own
keys and non-null columns are given in their sections below.

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

### Course Sources (MITxOnline, MIT edX)

| Column | Type | Description |
|--------|------|-------------|
| `platform` | string | Platform name (e.g., "ocw", "xpro") |
| `topics` | string | Comma-separated topic labels (e.g., `"Engineering, Computer Science"`) |
| `instructors` | string | Comma-separated instructor names |
| `runs` | string | Semicolon-separated run records; each record is pipe-delimited: `readable_id\|start_on\|end_on\|is_live` |
| `published` | boolean | Whether the resource is published/live |

### Program Sources (MITxOnline, MIT edX)

| Column | Type | Description |
|--------|------|-------------|
| `courses` | string | Comma-separated list of course `readable_id` values belonging to this program |
| `departments` | string | Comma-separated department names |

### OCW

`integrations__learn__ocw_courses` has one row per course in the OCW live bucket, read from
the course's published `data.json`, which is what MIT Learn's OCW ETL reads. It does not
follow the delimited-string columns above. A course has one run, so the run's fields are
columns of the course. A course that leaves the bucket leaves the model.

MIT Learn matches an OCW course on `url`, not `readable_id`; both are unique here.

| Column | Type | Description |
|--------|------|-------------|
| `readable_id` | string | Primary course number, `+` slugified term, `_` year, e.g. `6.1810+fall_2023` |
| `last_modified` | string | When the platform read the course's current objects. It changes only when an object of the course does. MIT Learn's ETL stores the S3 modification time of `data.json` |
| `description` | string | Rendered HTML (`course_description_html`) |
| `url` | string | Course URL on ocw.mit.edu, with a trailing slash |
| `image_url`, `image_alt`, `image_description` | string | The course image; `image_url` is absolute |
| `published` | boolean | Always true |
| `platform` | string | Always `ocw` |
| `run_id` | string | `legacy_uid`, else `site_uid`, without dashes. Equals `run_readable_id` in `integrations__learn__ocw_content_files` |
| `slug` | string | `courses/<slug>` |
| `term` | string | `Fall`, `Spring`, `Summer`, `January IAP`, or null |
| `year` | int | Null when not given |
| `level` | array(string) | Level labels, e.g. `Undergraduate` |
| `course_number` | string | Primary course number |
| `extra_course_numbers` | array(string) | Cross-listed course numbers |
| `departments` | array(string) | MIT department numbers |
| `topics` | array(string) | The course's MIT Learn topic names, sorted |
| `ocw_topics` | array(string) | Every OCW topic, subtopic and speciality, sorted |
| `content_tags` | array(string) | Learning resource types, e.g. `Lecture Notes` |
| `instructors` | string | JSON array as published, each with `first_name`, `last_name`, `middle_initial`, `salutation`, `title` |
| `hide_download` | boolean | Whether OCW hides the course's download link |

Three steps of MIT Learn's OCW ETL are left to the consumer, each one function in mit-learn:
`clean_data` on `description` (nh3 with Learn's tag allowlist), `parse_instructors` on
`instructors`, and `transform_topics` on `ocw_topics` for a course whose `topics` is empty
(one course on 2026-10-07), which needs Learn's topic mappings. `transform_levels` maps
`level` labels to Learn's codes. With those applied, the model matched every field MIT Learn
stores for all 2,587 OCW courses on 2026-10-07.

### xPRO

The three xPRO models carry what MIT Learn's xPRO ETL reads from xPRO's catalog API
(`/api/courses/` and `/api/programs/`), with the API's own rules for what is listed. They
do not follow the delimited-string columns above.

`integrations__learn__xpro_courses` has one row per live course with a live CMS page,
published or not. Beyond the required columns:

| Column | Type | Description |
|--------|------|-------------|
| `url` | string | Absolute URL of the course page |
| `image_url` | string | The page's thumbnail, or the xPRO site's default image |
| `published` | boolean | Whether a run in `integrations__learn__xpro_runs` has a price above zero. MIT Learn publishes an xPRO course on that, not on the course being live |
| `platform` | string | xPRO platform name (`xPRO`, `Emeritus`, `Global Alumni`, ...) |
| `topics` | array(string) | xPRO topic names, sorted. A child topic is `Parent:Child` |
| `format` | string | `Online` or `Hybrid` |
| `availability` | string | Always `dated` |
| `continuing_ed_credits` | double | CEUs from the page's certificate page |
| `duration`, `time_commitment` | string | Free text from the page |
| `min_weeks`, `max_weeks`, `min_weekly_hours`, `max_weekly_hours` | int | From the page; null when not given |

`integrations__learn__xpro_runs` has one row per run the API lists under a course: live,
with a start date, not past its end or enrollment end, enrollment open, and with an active
product. This is evaluated when the model builds, so a run leaves the model when it expires,
and MIT Learn unpublishes a run that is absent.

| Column | Type | Description |
|--------|------|-------------|
| `readable_id` | string | The run's course |
| `run_id` | string | Courseware id; unique |
| `title` | string | Run title |
| `start_date`, `end_date`, `enrollment_start`, `enrollment_end` | string | ISO 8601 UTC; all but `start_date` nullable |
| `price` | decimal | From the latest version of the run's product |
| `instructors` | array(string) | Names on the course page's faculty page |
| `published` | boolean | Whether `price` is above zero |

`integrations__learn__xpro_programs` has one row per live program with a live CMS page and a
product. It has the course columns above, plus `instructors` (array, the program
page's faculty), `courses` (comma-separated readable ids of its live courses, in program
order), and the fields of the single run MIT Learn gives a program: `price`, `start_date` and
`enrollment_start` (from the earliest open run of the program's first course) and `end_date`
(the latest end of any live run of its courses).

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
| `content_title` | string | Always `''`, as in Learn's production content files (Learn's lookup of Tika's metadata title finds none) |
| `content_type` | string | Always `file` |
| `source_path` | string | Path within the course export, e.g. `course/static/handout.pdf` |
| `file_extension` | string | File extension with its dot, e.g. `.pdf` |
| `checksum` | string | MD5 of `content` |
| `extraction_status` | string | `extracted`, or `failed` for a file whose text could not be read |
| `published` | boolean | Always true |
| `last_modified` | string | ISO 8601 time the file's text was extracted |

`description`, `file_type`, `content_author`, `content_language`, `image_src` and
`uid` are present and null, as Learn's Open edX ETL leaves them unset.

`integrations__learn__ocw_content_files` carries the same columns for OCW, read
from the `data.json` files of the OCW live bucket as Learn's OCW ETL reads them:

| Column | Type | Description |
|--------|------|-------------|
| `etl_source` | string | Always `ocw` |
| `run_readable_id` | string | The course's `legacy_uid`, else its `site_uid`, without dashes; equals `ContentFile.run.run_id` |
| `key` | string | `ContentFile.key`, the page's or resource's directory in the bucket, e.g. `courses/<course>/resources/<name>/` |
| `title`, `content_title` | string | Title of the page or resource |
| `description` | string | A page's description, or a resource's HTML body. Not sanitized: Learn passes it through `clean_data` |
| `url` | string | `https://ocw.mit.edu/` followed by `key` |
| `file_type` | string | MIME type of a resource's file; null for a page |
| `content` | string | A page's HTML, or the text Tika extracts from a resource's file |
| `content_type` | string | `page`, `video`, `pdf` or `file` |
| `image_src` | string | Thumbnail of a video resource |
| `file_extension` | string | Extension of a resource's file with its dot |
| `checksum` | string | MD5 of `content`, null when it is null or empty; equals `ContentFile.checksum` |
| `extraction_status` | string | `extracted`, `empty` or `failed`; null for a page and for a file Learn reads no text from |
| `published` | boolean | Always true |
| `last_modified` | string | ISO 8601 time the course was last read from the bucket |
| `content_tags` | string | JSON array of learning resource types, loaded as content tags |
| `youtube_id` | string | YouTube id of a video resource |

`content_author`, `content_language`, `uid`, `edx_module_id` and `source_path` are
present and null, as Learn's OCW ETL leaves them unset.

### Media Sources (YouTube, podcasts)

MIT Learn pulls these as sets of flat models and nests them itself: a playlist
with its videos, a podcast with its episodes. Each pull is a full sync. Learn
reads every row of every model in the set and unpublishes what the set no longer
lists, so it never filters on `last_modified`. Learn fails the pull before
writing when a model returns no rows, or when the pull would unpublish more than
10% of what it has published for the source (playlists, videos, podcasts or
episodes, each counted on its own). A real removal of that size has to be run in
Learn with the limit lifted.

| Set | Models | Joined on |
|-----|--------|-----------|
| YouTube | `integrations__learn__youtube_channels`, `_playlists`, `_playlist_videos`, `_videos` | `playlists.channel_id` = `channels.channel_id`; `playlist_videos.playlist_readable_id` and `video_readable_id` = the `readable_id` of a playlist and a video |
| Podcasts | `integrations__learn__podcasts`, `integrations__learn__podcast_episodes` | `podcast_episodes.podcast_readable_id` = `podcasts.readable_id` |

The models of a set are built separately, so a set can be read between two
builds. Learn fails the pull when a playlist lists a video that is not in the
videos model.

Left to Learn, which applies the functions its Celery ETL uses:

- HTML cleaning of `description` (podcasts, episodes) and `description_raw` (videos), and YouTube's boilerplate-line removal
- `duration_raw` to an ISO-8601 duration and `published_on_raw` to `last_modified` (episodes)
- `offered_by` of a video, which is its playlist's
- For a `create_videos = false` playlist, matching videos to OCW content files and the 60% rule

The media models are exempt from the required columns above. Each has its own
key and non-null columns, which its dbt tests enforce:

| Model | Unique key | Other non-null columns |
|-------|------------|------------------------|
| `integrations__learn__youtube_channels` | `channel_id` | `title` |
| `integrations__learn__youtube_playlists` | `readable_id` | `channel_id`, `title`, `create_videos` |
| `integrations__learn__youtube_playlist_videos` | (`playlist_readable_id`, `video_readable_id`) and (`playlist_readable_id`, `position`) | none |
| `integrations__learn__youtube_videos` | `readable_id` | `youtube_id` |
| `integrations__learn__podcasts` | `readable_id` | `title`, `rss_url`, `last_modified`, `etl_source` |
| `integrations__learn__podcast_episodes` | `readable_id` | `podcast_readable_id`, `audio_url`, `etl_source` |

Only `_youtube_videos` and `_podcasts` have a `last_modified` column. The other
columns are described in
`src/ol_dbt/models/integrations/learn/_integrations__learn__cohort3__schema.yml`.

## Grain Expectations

- **Catalog models** (`integrations__learn__<source>_courses`, `integrations__learn__<source>_programs`): One row per resource
- **Content file models** (`integrations__learn__content_files`, `integrations__learn__ocw_content_files`): One row per content file
- **Media models**: One row per channel, playlist, video, podcast or episode; `integrations__learn__youtube_playlist_videos` is one row per video in a playlist

## Nullability Rules

1. All required columns must be `NOT NULL` in catalog models
2. Content file models are exempt from the resource-level required columns; their non-null columns are `etl_source`, `run_readable_id` and `key`, plus `source_path` and `extraction_status` in `integrations__learn__content_files` and `url` and `content_type` in `integrations__learn__ocw_content_files`
3. Media models are exempt from the resource-level required columns; their keys and non-null columns are in the table under Media Sources
4. String-aggregated columns (`topics`, `instructors`, `runs`, `courses`) may be `NULL` when no data exists; consumers should treat `NULL` as empty
5. Text columns (`description`, `url`, `image_url`) may be `NULL` when no data exists

## ETL Source Values

The `etl_source` column must match one of the following `ETLSource` enum values in MIT Learn:

- `ocw` - OCW courses
- `mitxonline` - MITx Online courses
- `xpro` - xPRO courses
- `mit_edx` - MIT edX courses
- `oll` - Open Learning Library
- `canvas` - Canvas courses
- `youtube` - YouTube videos
- `podcast` - Podcasts and their episodes
- `ovs` - OVS videos

## Version History

| Date | Author | Changes |
|------|--------|---------|
| 2026-05-27 | Initial draft | Foundation schema contract |
| 2026-08-19 | Tobias Macey | Drop `micromasters`. MIT Learn unpublished and then deleted its MicroMasters resources (`learning_resources` migrations 0117/0118) and removed `micromasters` from its `ETLSource` enum, so the source has no destination. `integrations__learn__micromasters_programs` retired with it; Cohort 1 is 6 sources, not 7. |
| 2026-10-02 | Tobias Macey | Content file columns rewritten to what `integrations__learn__content_files` carries, keyed by (`etl_source`, `run_readable_id`) per the scoped-pull contract. The earlier `file_size` column is not part of it. |
| 2026-10-03 | Tobias Macey | Added `integrations__learn__ocw_content_files`. |
| 2026-10-05 | Tobias Macey | YouTube and podcasts are pulled from the warehouse, not pushed by webhook. Added the media sources section. `etl_source` for podcasts is `podcast`, as the models and MIT Learn's `ETLSource` have it. |
| 2026-10-06 | Tobias Macey | xPRO models rebuilt on what MIT Learn's xPRO ETL reads from the xPRO catalog API. Added `integrations__learn__xpro_runs` with prices and enrollment dates. `runs`, `page_slug`, `length`, `effort` and the course `instructors` string are gone from the xPRO models; `topics` is an array; `url` is absolute; `published` follows price. |
| 2026-10-07 | Tobias Macey | `integrations__learn__ocw_courses` rebuilt on each course's published `data.json`, which MIT Learn's OCW ETL reads, in place of ocw-studio's database. `readable_id` takes Learn's form; added `run_id`, `slug`, `image_alt`, `image_description`, `ocw_topics`, `content_tags`, `hide_download`; `level`, `extra_course_numbers`, `departments` and `topics` are arrays; `departments` holds numbers; `instructors` is JSON; `description` is HTML; `url` ends in a slash. |
