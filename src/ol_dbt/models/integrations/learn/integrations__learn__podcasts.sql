{#
  integrations__learn__podcasts
  Exposes MIT podcast channels for MIT Learn's ETL (webhook delivery).
  Contract: docs/learn_marts_contract.md
  Episodes live in integrations__learn__podcast_episodes, joined on readable_id.
#}

with channels as (
    select * from {{ ref('stg__podcast__rss__channels') }}
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
from channels
where podcast_rss_url is not null and podcast_title is not null
