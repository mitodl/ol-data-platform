{#
  integrations__learn__podcasts
  Exposes MIT podcast channels for MIT Learn's ETL (webhook delivery).
  Contract: docs/learn_marts_contract.md
  Episodes live in integrations__learn__podcast_episodes, joined on readable_id.

  STALENESS: raw__podcast__rss__channels is loaded with write_disposition=
  "merge", so a podcast removed from mitodl/open-podcast-data is never deleted
  -- it is left un-upserted, and its _dlt_load_id stays behind at the last load
  that saw it. A plain full-table read would therefore keep delivering removed
  podcasts forever, and MIT Learn (which unpublishes only what is ABSENT from
  the batch) could never retire one.

  Rows are filtered to those seen in the last {{ var('podcast_absence_grace_loads', 3) }}
  loads rather than only the most recent one. The grace period exists because
  the dlt source SKIPS a feed it cannot fetch or parse: with a strict
  most-recent-load filter, one transient RSS outage would drop that podcast
  from the batch and unpublish a live podcast and all its episodes. At a daily
  ingest cadence a podcast must be missing three consecutive days before it is
  treated as removed.
#}

with channels as (
    select * from {{ ref('stg__podcast__rss__channels') }}
)

-- Rank the distinct load ids so recency can be expressed in "loads ago"
-- rather than by comparing opaque load-id strings. rank 1 = most recent load.
, loads as (
    select
        podcast_dlt_load_id
        , row_number() over (order by podcast_dlt_load_id desc) as loads_ago
    from (select distinct podcast_dlt_load_id from channels)
)

, current_channels as (
    select channels.*
    from channels
    inner join loads on channels.podcast_dlt_load_id = loads.podcast_dlt_load_id
    where loads.loads_ago <= {{ var('podcast_absence_grace_loads', 3) }}
)

select
    -- MIT Learn derives a podcast's readable_id as rss_url.split("//")[-1]
    -- (learning_resources/etl/podcast.py:parse_readable_id_from_url), which
    -- keeps any trailing slash. The dlt source strips it, so recompute here
    -- from rss_url instead of reusing the raw readable_id -- otherwise the
    -- webhook would create a second resource alongside the Celery ETL's.
    regexp_replace(podcast_rss_url, '^.*//', '')             as readable_id
    , podcast_title                                          as title
    , podcast_description                                    as description
    , podcast_website                                        as url
    , podcast_image_url                                      as image_url
    , podcast_topics_raw                                     as topics
    , podcast_offered_by                                     as offered_by
    , podcast_rss_url                                        as rss_url
    , podcast_apple_podcasts_url                             as apple_podcasts_url
    , podcast_google_podcasts_url                            as google_podcasts_url
    -- The RSS <lastBuildDate> is RFC 2822, which Trino cannot parse without a
    -- format string that varies by feed; current_timestamp is the same
    -- conservative fallback mitpe/oll use.
    , {{ cast_timestamp_to_iso8601('current_timestamp') }}   as last_modified
    , 'podcast'                                              as etl_source
    , 'podcast'                                              as platform
    , 'podcast'                                              as resource_type
    , 'anytime'                                              as availability
    , true                                                   as published
from current_channels
where podcast_rss_url is not null and podcast_title is not null
