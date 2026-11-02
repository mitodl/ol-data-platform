# Learn Integration Schema Contract

This document defines the stable interface that MIT Learn's warehouse-pull ETL tasks depend on. Learn reads these models from StarRocks over the MySQL protocol. All Learn-facing dbt integration models follow the naming convention and column requirements specified here.

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

Each source may add additional columns following its needs.

### MIT edX programs

`integrations__learn__mit_edx_programs` carries list-valued fields as arrays, not
delimited strings. Beyond the required columns, the ones MIT Learn links on are:

| Column | Type | Description |
|--------|------|-------------|
| `course_readable_ids` | array(string) | The program's course keys in the programs API's order. MIT Learn links the ones it publishes |
| `topics` | array(string) | MIT Learn topics the program's published courses' edX subjects resolve to through the topic crosswalk |

The model's full column list is in `_integrations__learn__cohort2__schema.yml`.

### OCW

`integrations__learn__ocw_courses` has one row per course in the OCW live bucket, read from
the course's published `data.json`, which is what MIT Learn's OCW ETL reads. A course has
one run, so the run's fields are columns of the course. A course that leaves the bucket
leaves the model. A directory left in the bucket by a site that ocw-studio now publishes
under another path is not a course, and its files are not in
`integrations__learn__ocw_content_files`. Neither is a course whose site ocw-studio has
unpublished: it leaves the model on the first build after ocw-studio's database syncs,
which can be before the bucket load's next daily sweep sees the directory gone, and also
covers a removal that is pending or errored. It returns once a live publish is requested
and the bucket load has read its data.json again.

MIT Learn matches an OCW course on `url`, not `readable_id`; both are unique here.

| Column | Type | Description |
|--------|------|-------------|
| `readable_id` | string | Primary course number, `+` slugified term, `_` year, e.g. `6.1810+fall_2023` |
| `last_modified` | string | When the platform last read the course from the bucket. A course is read again when one of its objects changes, or to retry a file Tika failed on. MIT Learn's ETL stores the S3 modification time of `data.json` |
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
(`/api/courses/` and `/api/programs/`), with the API's own rules for what is listed.

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
| `platform_code` | string | MIT Learn's platform code for the course's `platform`, as `XPRO_PLATFORM_TRANSFORM` maps it (`emeritus`, `globalalumni`, `simplilearn`, `susskind`, `whu`, `xpro`). The run's resource's `platform.code`, which a content file's vector point key uses. Null for a platform Learn does not map: its loader skips such a course |

`integrations__learn__xpro_programs` has one row per live program with a live CMS page and a
product. It has the course columns above, plus `instructors` (array, the program
page's faculty), `courses` (comma-separated readable ids of its live courses, in program
order), and the fields of the single run MIT Learn gives a program: `price`, `start_date` and
`enrollment_start` (from the earliest open run of the program's first course) and `end_date`
(the latest end of any live run of its courses).

### MITx Online

The four MITx Online models carry what MIT Learn's MITx Online ETL
(`learning_resources/etl/mitxonline.py`) reads from MITx Online's catalog API, with the
API's rules for what is listed and the ETL's rules for what is published. Courses come
from `/api/internal/courses/`, which lists every course and every run and ignores
`?live=true`; programs come from `/api/v2/programs/?live=true`, which leaves out B2B-only
programs.

Neither catalog model has a `url`: MIT Learn points a MITx Online resource at its own site
(`/courses/<readable_id>`, `/programs/<readable_id>`, or `/courses/p/<readable_id>` for a
program with `display_mode` `course`) when `has_page` is true, and builds that from its own
base URL. `description` is the page's HTML: the API strips the tags (`bleach.clean` with no
allowed tags) before MIT Learn's `clean_data`, so the Sync task has to do both.

`integrations__learn__mitxonline_courses` has one row per course, published or not, except
courses titled "Proctored Exam", which the ETL drops. Beyond the required columns:

| Column | Type | Description |
|--------|------|-------------|
| `image_url` | string | The page's feature image with the `?v=<file hash>` suffix the API adds |
| `has_page` | boolean | Whether the course has a CMS page |
| `published` | boolean | Page live, `include_in_learn_catalog`, and a published run in `integrations__learn__mitxonline_runs` |
| `include_in_learn_catalog`, `ingest_content_files_for_ai` | boolean | Page flags; the second is MIT Learn's `force_ingest` |
| `topics` | array(string) | The page's topics and their parent topics, sorted |
| `departments` | array(string) | Department names, sorted. MIT Learn maps names to department ids and drops a name it does not know |
| `course_numbers` | array(string) | Course numbers of the runs, latest run first; the first is primary |
| `certification` | boolean | Whether a published, current run offers the verified mode |
| `certification_type` | string | `MicroMasters Credential` or `Certificate of Completion`; used only when `certification` is true |
| `availability` | string | `dated` when any run is enrollable and not self-paced, else `anytime` |
| `pace` | array(string) | Distinct `pace` of the course's runs |
| `duration`, `time_commitment` | string | Free text from the page |
| `min_weeks`, `max_weeks` | int | From the page |
| `min_weekly_hours`, `max_weekly_hours` | string | From the page, as stored; MIT Learn parses them to integers |
| `min_price`, `max_price` | decimal | The page's certificate price range |
| `has_catalog_page` | boolean | Whether the page is live and in the Learn catalog, which is when MITx Online's pages API lists it to MIT Learn |
| `about`, `what_you_learn`, `prerequisites` | string | The page's sections as stored HTML, for `full_description`; null unless `has_catalog_page` |
| `faqs` | string | JSON array of the page's FAQs as `{"question", "answer"}` objects, in page order, the answer as stored HTML; `[]` for a page without FAQs; null unless `has_catalog_page` |

`integrations__learn__mitxonline_runs` has one row per run of those courses, source runs and
B2B runs included, because MIT Learn stores those too, unpublished. `published`, `status`
and `prices` depend on the time the model builds.

| Column | Type | Description |
|--------|------|-------------|
| `readable_id` | string | The run's course |
| `run_id` | string | Courseware id; unique |
| `title` | string | Run title |
| `start_date`, `end_date`, `enrollment_start`, `enrollment_end` | string | ISO 8601 UTC, nullable. `start_date` falls back to `enrollment_start` |
| `published` | boolean | Not a variant, enrollable, and the course page is live |
| `b2b_only`, `is_variant` | boolean | A variant is a B2B-only run or one whose language matches a non-default variant set of the course |
| `status` | string | `Current` when the course has a page and the run is fully enrollable, else `Archived` |
| `pace` | string | `self_paced` or `instructor_paced` |
| `language`, `course_number` | string | From the run |
| `has_verified_mode` | boolean | Whether the run offers the verified enrollment mode |
| `prices` | array(decimal) | Zero for the audit mode, the page's min and max price for the verified mode; only zero when the run is not current or the page has no price |
| `instructors` | array(string) | Names of the course page's instructors. A name linked twice appears twice |

`integrations__learn__mitxonline_programs` has one row per live program that is not
B2B-only. It has `image_url`, `has_page`, `topics` (the page topics of its live courses,
without parents), `departments`, `certification`, `certification_type`, `availability`, the
duration and price columns above, `has_catalog_page`, `about`, `what_you_learn`,
`prerequisites`, `faqs`, `display_mode`, `published` (page live and
`include_in_learn_catalog`) and the fields of the single run MIT Learn gives a program: `start_date`, `end_date`,
`enrollment_start`, `enrollment_end` and `prices`. The run's `pace` is the distinct pace of
the program's courses and is left to the Sync task.

`integrations__learn__mitxonline_program_children` has one row per program and child, from
the program's requirement tree:

| Column | Type | Description |
|--------|------|-------------|
| `readable_id` | string | The parent program |
| `child_resource_type` | string | `course` or `program` |
| `child_readable_id` | string | The child course or program |
| `child_display_mode` | string | A child program's display mode; null for a course |
| `child_position` | int | Zero-based place in the tree, in the order MIT Learn gives a program's courses. Null for a child program not shown as a course, whose courses are listed under the parent with positions of their own |
| `child_program_order` | int | Zero-based place of a child program among the program's child programs, by its first node in the tree. MIT Learn's ETL lists child programs in this order and numbers the ones without a `child_position` after the program's other children in it. Null for a course |

A course is listed only when its page is live, so positions have gaps. A required program is
followed one level down: no required program requires another today.

`courses_programrequirement` and `courses_program_departments` are loaded by full refresh,
so a deleted tree node or a department removed from a program leaves raw on the next sync.
The staging model tests the tree path as unique. Two rows with one path are a deleted node
whose path went to another node, one sign that the table is loaded incrementally again.

#### `full_description`

MIT Learn's ETL builds `full_description` for courses and programs (mit-learn #4089) from
the product pages MITx Online's Wagtail pages API (`/api/v2/pages/?fields=*`) lists, which
for MIT Learn's request (its `Api-Key` header authenticates no user) are the live pages in
the Learn catalog. Wagtail also leaves out pages behind a view restriction, which raw does
not carry; none were seen on 2026-10-19. The models deliver the sections and the Sync task
runs MIT Learn's own assembly, `learning_resources.etl.mitxonline.parse_full_description`,
on them: each section through `clean_data` under an `<h2>` heading, then `html_to_markdown`
and `strip_markdown_images`. Markdown conversion is left to the Sync task because a SQL
rewrite would not match `html2text`. When `has_catalog_page` is false MIT Learn does not see
the page and leaves the stored `full_description` alone, so the Sync task should too. A page
with no sections gets `None`.

The sections are the HTML MITx Online stores, not what its API serves: the API expands
Wagtail's internal links (`<a linktype="page" id="...">`) to URLs. `clean_data` drops `<a>`
tags and keeps their text, so the assembled text is the same either way. If MIT Learn ever
keeps links in `full_description`, the Sync task would need the page URLs.

MIT Learn appends the page's FAQs under "Frequently asked questions", each question as an
`<h3>` and its answer through `clean_data`, skipping one without a question. They come from
`ProductPageFAQ` (table `cms_productpagefaq`, loaded by full refresh), and `faqs` carries
them as the pages API's `faqs` field does: a JSON array in `sort_order` (then id), `[]`
when the page has none. Each object has only `question` and `answer`; the API's `id` is
left out because MIT Learn does not read it. The Sync task parses the column with
`json.loads` and passes the list as the page's `faqs`. The answer is stored HTML, so the
internal-link note above applies to it too. On 2026-10-19, 1 course page (6 FAQs) and 18
program pages (112 FAQs) of the ones MIT Learn reads had FAQs.

Checked on 2026-10-19 by building the models on production raw (Athena) and running MIT
Learn's `parse_full_description` (mit-learn main at 2662d01e4) on both the models' sections
and MITx Online's pages API: with FAQs left out of both, the result is identical for all 186
courses and 28 programs whose page the API lists (159 and 28 of them published). The one
listed page without `has_catalog_page` has the catalog flag off in raw (synced 12:06 UTC)
and on in the API (read 22:12 UTC). MIT Learn's production API had no `full_description` on
any MITx Online course or program that day, so the comparison is with MIT Learn's code, not
stored values.

Checked again on 2026-10-20 with FAQs, after `cms_productpagefaq`'s first production sync
(14:28 UTC): MIT Learn's `parse_full_description` (mit-learn main at e04e9003b) gives the
same result from the models' sections and `faqs` as from the pages API (read 14:36 UTC) for
all 187 courses and 28 programs, including the 19 pages with FAQs. Raw has one FAQ row more
than the API: an empty question and answer on a page that is not live, so `faqs` is null
there.

Checked on 2026-10-13 against a build on production raw: the models publish the same 157
courses and 28 programs MIT Learn does, with the same published runs, run dates, prices,
course numbers, certification, availability, pace and images, and the same position for
every program child MIT Learn shows.

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
| `release_date` | string | `ContentFile.release_date`, ISO 8601 UTC (`YYYY-MM-DDTHH:MM:SS.ffffffZ`): the latest OLX `start` on the path from the course down to the file, as Learn's `OlxArchive.release_dates` computes it. Null when undated. Learn hides a file until this date passes, so a future-dated row is present but not yet visible |

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
4. Aggregated columns (`topics`, `instructors`, and `courses` in `integrations__learn__xpro_programs`) may be `NULL` when no data exists; consumers should treat `NULL` as empty. `topics` is an array except in `integrations__learn__podcasts`, `integrations__learn__podcast_episodes` and `integrations__learn__mit_climate_articles`, where it is a comma-separated string, as is `courses`
5. Text columns (`description`, `url`, `image_url`) may be `NULL` when no data exists

## ETL Source Values

The `etl_source` column must match one of the following `ETLSource` enum values in MIT Learn:

- `ocw` - OCW courses
- `mitxonline` - MITx Online courses
- `xpro` - xPRO courses
- `mit_edx` - MIT edX programs
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
| 2026-10-09 | Tobias Macey | The OCW models leave out a bucket directory that no ocw-studio site has as its `url_path` (the old path of a renamed site), which had put two rows with one `readable_id` and `run_id` in `integrations__learn__ocw_courses`. |
| 2026-10-13 | Tobias Macey | MITx Online models rebuilt on what MIT Learn's MITx Online ETL reads from the MITx Online catalog API. Added `integrations__learn__mitxonline_runs` and `integrations__learn__mitxonline_program_children`. `runs`, `courses`, `url`, `page_slug`, `price`, `length`, `effort`, `instructors` and `program_type` are gone from the courses and programs models; `topics` and `departments` are arrays; `published` follows MIT Learn's rule; B2B-only programs are left out and courses that are not live are included. |
| 2026-10-15 | Tobias Macey | `integrations__learn__oll_courses` rebuilt on what MIT Learn's OLL ETL writes. OCW-offered rows are included under Learn's `{course_number}+{semester}_{year}` id, with `offered_by` `ocw` (else `mitx`); only Learn's `SKIP_OCW_COURSES` run is left out. Added `run_id`, `course_number`, `offered_by`, `level`, `instructors`, `year`, `semester`, `duration`, `min_weeks`, `max_weeks`, `time_commitment`, `min_weekly_hours` and `max_weekly_hours`; `topics` is an array of the sheet's MITx Online topics; `published` follows the sheet; `runs` is gone. |
| 2026-10-16 | Tobias Macey | `courses_programrequirement` and `courses_program_departments` load by full refresh, so deleted tree nodes and departments leave `integrations__learn__mitxonline_program_children`, the programs' `departments` and `start_date`, and the courses' `certification_type`. No column changes. |
| 2026-10-20 | Tobias Macey | Added `has_catalog_page`, `about`, `what_you_learn`, `prerequisites` and `faqs` to `integrations__learn__mitxonline_courses` and `integrations__learn__mitxonline_programs` for MIT Learn's `full_description`. |
| 2026-10-22 | Tobias Macey | Drop MIT edX courses. MIT Learn's `mit_edx` courses are retired rather than moved to a warehouse pull, so no Learn Sync task reads them and `integrations__learn__mit_edx_courses` is gone along with the Course Sources (MIT edX) columns. `mit_edx` stays as the source of `integrations__learn__mit_edx_programs`. The MIT edX programs columns now name what the model has (`course_readable_ids` and `topics` arrays) rather than `courses` and `departments` strings, which it never had. |
| 2026-10-22 | Tobias Macey | Added `child_program_order` to `integrations__learn__mitxonline_program_children`, so a Sync task can number child programs not shown as courses in requirement tree order, as MIT Learn's ETL does, and not by readable id. |
| 2026-10-22 | Tobias Macey | Added `release_date` to `integrations__learn__content_files` for MIT Learn's `ContentFile.release_date` (mit-learn#4093). The file exclusions also follow mit-learn#4106: an asset name no longer matches a link that drops one of its separators. |
| 2026-10-27 | Tobias Macey | Added `platform_code` to `integrations__learn__xpro_runs`, Learn's platform code for the run's course, so the content-file embedding chunks key an xPRO file's points under its course's platform and not always `xpro`. |
