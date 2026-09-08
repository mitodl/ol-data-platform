{#
  integrations__learn__podcast_episodes
  Exposes MIT podcast episodes for MIT Learn's ETL (webhook delivery).
  Contract: docs/learn_marts_contract.md
  Joined to integrations__learn__podcasts on podcast_readable_id = readable_id.

  STALENESS: same merge-load problem as integrations__learn__podcasts, and the
  same fix. An episode dropped from a feed is left un-upserted rather than
  deleted, so without filtering it would be redelivered forever and MIT Learn
  could never retire it. Episodes are filtered on their OWN load recency, not
  their channel's: a feed can drop a single old episode while the channel
  itself keeps being refreshed every day.

  Channel attributes come from integrations__learn__podcasts rather than from
  stg__podcast__rss__channels, and that is deliberate. Joining the filtered
  episodes to the UNFILTERED staging table would let an episode still inside
  its grace window attach to a channel that had already aged out of the
  podcasts model -- producing rows whose podcast_readable_id matches no
  delivered podcast. The delivery asset drops those, so the payload stayed
  correct, but the row count did not: MIN_EPISODES counts rows in this model,
  so orphans would inflate the very number that guards against a short read.
  The two tables also rank recency over independent _dlt_load_id universes, so
  "3 loads ago" does not necessarily mean the same instant in each. Reading the
  parent from the podcasts model makes referential integrity structural instead
  of coincidental -- an episode can only exist here if its podcast ships.
#}

with episodes as (
    select * from {{ ref('stg__podcast__rss__episodes') }}
)

, podcasts as (
    select * from {{ ref('integrations__learn__podcasts') }}
)

-- rank 1 = most recent load; see integrations__learn__podcasts for why a
-- grace window is used instead of "most recent load only".
, loads as (
    select
        episode_dlt_load_id
        , row_number() over (order by episode_dlt_load_id desc) as loads_ago
    from (select distinct episode_dlt_load_id from episodes)
)

, current_episodes as (
    select episodes.*
    from episodes
    inner join loads on episodes.episode_dlt_load_id = loads.episode_dlt_load_id
    where loads.loads_ago <= {{ var('podcast_absence_grace_loads', 3) }}
)

select
    -- The dlt source already derives the episode identifier the way MIT Learn
    -- does (<guid>, else the link/audio URL with the scheme stripped), so this
    -- one passes through unchanged -- unlike the channel identifier.
    current_episodes.episode_readable_id                     as readable_id
    , podcasts.readable_id                                   as podcast_readable_id
    , current_episodes.episode_title                         as title
    , current_episodes.episode_description                   as description
    , current_episodes.episode_url                           as url
    , current_episodes.episode_audio_url                     as audio_url
    , current_episodes.episode_link                          as episode_link
    , current_episodes.episode_image_url                     as image_url
    -- Free-form itunes:duration text; normalized to ISO-8601 by the delivery
    -- asset, mirroring learning_resources/etl/utils.py:iso8601_duration.
    , current_episodes.episode_duration_raw                  as duration_raw
    -- RFC 2822 <pubDate>; parsed by the delivery asset into last_modified.
    , current_episodes.episode_published_on_raw              as published_on_raw
    -- Topics and offered_by are inherited from the parent podcast, matching
    -- transform_episode() in learning_resources/etl/podcast.py.
    , podcasts.topics                                        as topics
    , podcasts.offered_by                                    as offered_by
    , 'podcast'                                              as etl_source
    , 'podcast'                                              as platform
    , 'podcast_episode'                                      as resource_type
    , 'anytime'                                              as availability
    , true                                                   as published
from current_episodes
-- The episode carries its channel's feed URL, so the parent is resolved the
-- same way integrations__learn__podcasts derives its own readable_id, keeping
-- the two in lockstep by construction.
inner join podcasts
    on regexp_replace(current_episodes.podcast_rss_url, '^.*//', '')
    = podcasts.readable_id
