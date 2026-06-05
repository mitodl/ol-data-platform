# Podcast RSS Ingest

Dagster component that loads MIT Learn podcast data from RSS feeds into Iceberg
tables in the raw data layer, using [dlt](https://dlthub.com) for extraction and
loading.

This is a Phase 3 data source migration from the MIT Learn Celery-based
`podcast.py` ETL into the data platform.

## Overview

**Config source:** [mitodl/open-podcast-data](https://github.com/mitodl/open-podcast-data)
(34 YAML files, one per podcast)

**Data flow:**
```
GitHub repo (mitodl/open-podcast-data/podcasts/*.yaml)
    → rss_url per podcast
        → raw__podcast__rss__channels   (one row per podcast channel)
        → raw__podcast__rss__episodes   (one row per episode)
```

**Dagster assets:**
| Asset Key | Description |
|-----------|-------------|
| `ol_warehouse_raw_data/raw__podcast__rss__channels` | One row per configured podcast |
| `ol_warehouse_raw_data/raw__podcast__rss__episodes` | One row per episode across all podcasts |

**Write disposition:** `merge` on `readable_id` — updates existing records and
adds new episodes without dropping the full table on each run.

## Podcast YAML Schema

Each YAML config in `mitodl/open-podcast-data/podcasts/` contains:

```yaml
rss_url: https://feeds.simplecast.com/8fQdS6Dx   # required
website: https://chalk-radio.simplecast.com/      # required
topics: Pedagogy and Curriculum, Digital Learning  # optional, comma-separated
offered_by: OCW                                    # optional
podcast_title: Chalk Radio                         # optional, overrides RSS title
apple_podcasts_url: https://podcasts.apple.com/…  # optional
google_podcasts_url: https://podcasts.google.com/… # optional
```

## Output Tables

### `raw__podcast__rss__channels`

| Column | Source |
|--------|--------|
| `readable_id` | Derived from `rss_url` path (stable ID matching Learn's convention) |
| `rss_url` | YAML `rss_url` |
| `website` | YAML `website` |
| `title` | YAML `podcast_title` or RSS `<title>` |
| `description` | RSS `<description>` |
| `language` | RSS `<language>` |
| `last_build_date` | RSS `<lastBuildDate>` |
| `image_url` | RSS iTunes `<itunes:image href>` or `<image><url>` |
| `offered_by` | YAML `offered_by` |
| `topics` | YAML `topics` |
| `apple_podcasts_url` | YAML `apple_podcasts_url` |
| `google_podcasts_url` | YAML `google_podcasts_url` |
| `etl_source` | `"podcast"` (static) |

### `raw__podcast__rss__episodes`

| Column | Source |
|--------|--------|
| `readable_id` | RSS `<guid>` or fallback to URL path |
| `channel_readable_id` | FK to `raw__podcast__rss__channels.readable_id` |
| `channel_rss_url` | Parent channel RSS URL |
| `title` | RSS `<title>` |
| `description` | RSS `<description>` |
| `url` | RSS `<link>` or `<enclosure url>` |
| `audio_url` | RSS `<enclosure url>` |
| `episode_link` | RSS `<link>` |
| `duration` | RSS `<itunes:duration>` |
| `pub_date` | RSS `<pubDate>` |
| `image_url` | RSS `<itunes:image href>` or inherited from channel |
| `etl_source` | `"podcast"` (static) |

## Configuration

### GitHub Access Token (Optional)

The `mitodl/open-podcast-data` repo is public. No token is required, but an
authenticated token raises rate limits from 60 to 5,000 requests/hour. To set one:

```toml
# .dlt/secrets.toml
[sources.loads.podcast_rss_source]
github_access_token = "ghp_your_token_here"
```

Or via environment variable:
```bash
export SOURCES__LOADS__PODCAST_RSS_SOURCE__GITHUB_ACCESS_TOKEN=ghp_your_token_here
```

### Source Parameters (Optional Overrides)

These can be overridden in `config.toml` if the defaults need to change:

```toml
[sources.loads.podcast_rss_source]
github_repo = "mitodl/open-podcast-data"  # default
github_folder = "podcasts"                 # default
github_branch = "master"                   # default
```

### Destinations

The pipeline selects its destination from `DAGSTER_ENVIRONMENT`:

| `DAGSTER_ENVIRONMENT` | Destination | Dataset |
|---|---|---|
| `dev` / unset | `podcast_local` → `file:///tmp/.dlt/data/podcast` | `podcast_rss_local` |
| `qa` | `podcast_qa` → `s3://ol-data-lake-raw-qa/podcast` | `ol_warehouse_qa_raw` |
| `production` | `podcast_production` → `s3://ol-data-lake-raw-production/podcast` | `ol_warehouse_production_raw` |

## Local Development

### Run the pipeline directly

```bash
cd /path/to/ol-data-platform/dg_projects/data_loading

# Load all podcasts to local Parquet files
uv run python -m data_loading.defs.podcast_rss_ingest.loads
# Output: /tmp/.dlt/data/podcast/raw__podcast__rss__channels/ and raw__podcast__rss__episodes/
```

### Query local results with DuckDB

```bash
duckdb -c "
SELECT readable_id, title, offered_by, topics
FROM read_parquet('/tmp/.dlt/data/podcast/raw__podcast__rss__channels/**/*.parquet');
"

duckdb -c "
SELECT c.title as podcast, COUNT(e.readable_id) as episode_count
FROM read_parquet('/tmp/.dlt/data/podcast/raw__podcast__rss__channels/**/*.parquet') c
JOIN read_parquet('/tmp/.dlt/data/podcast/raw__podcast__rss__episodes/**/*.parquet') e
  ON e.channel_readable_id = c.readable_id
GROUP BY c.title
ORDER BY episode_count DESC;
"
```

### Verify assets in Dagster

```bash
cd /path/to/ol-data-platform/dg_projects/data_loading
dg list defs | grep podcast
```

## Validation Against Learn

To validate the output matches the current Learn database:

```sql
-- Expected: ~34 channels
SELECT COUNT(*) FROM raw__podcast__rss__channels;

-- Expected: varies; check against Learn's PodcastEpisode count
SELECT COUNT(*) FROM raw__podcast__rss__episodes;

-- Spot-check a known podcast
SELECT * FROM raw__podcast__rss__channels
WHERE readable_id LIKE '%chalk-radio%';
```

## Migration Context

This module is part of **Phase 3** of the Learn → Data Platform migration
([learn_dagster_migration_strategy.md](../../../../../../../learn_dagster_migration_strategy.md)).

**Current Learn implementation:** `learning_resources/etl/podcast.py` — uses
PyGithub + BeautifulSoup + Celery tasks to fetch and load podcast data.

**Data platform replacement:** This dlt pipeline replaces the Celery-based
ingestion for the raw data layer. The raw tables produced here feed downstream
dbt models that will produce the final `marts__learn__podcasts` and
`marts__learn__podcast_episodes` tables for consumption by the Learn application.

## References

- [dlt filesystem source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/filesystem)
- [dlt REST API source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/basic)
- [Dagster dlt integration](https://docs.dagster.io/integrations/libraries/dlt)
- [mitodl/open-podcast-data](https://github.com/mitodl/open-podcast-data)
- [Learn podcast ETL](../../../../../../../mit-learn/learning_resources/etl/podcast.py)
