{#
  integrations__learn__youtube_channels
  Exposes the YouTube channels configured in mitodl/open-video-data for MIT
  Learn's VideoChannel rows (webhook delivery). Mirrors transform_channel in
  learning_resources/etl/youtube.py.
  Contract: docs/learn_marts_contract.md

  STALENESS: the raw table is merge-loaded, so a channel removed from the config
  keeps the _dlt_load_id of the last load that saw it. Only rows from the most
  recent load are kept. Unlike the podcast models there is no grace window: the
  youtube dlt source raises on an API error or a bad config file instead of
  skipping a channel, so a load writes every configured channel or nothing. (A
  channel YouTube returns no data for is the exception; see below.)

  Only channels from the config file MIT Learn reads are kept. Learn's
  YOUTUBE_CONFIG_URL names one file (youtube/channels.yaml in every deployed
  environment); the dlt source loads every file in the folder, including
  shorts.yaml, whose channel reaches Learn through OVS instead.

  Known difference: a configured channel YouTube returns no data for is dropped
  here, so the webhook would unpublish it. MIT Learn's Celery task instead logs a
  warning and leaves the channel and its playlists as they were.
#}

with channels as (
    select * from {{ ref('stg__youtube__api__channels') }}
)

select
    channel_id
    , channel_title                                          as title
    , channel_offered_by                                     as offered_by
    , 'youtube'                                              as etl_source
    , true                                                   as published
    , channel_dlt_load_id                                    as dlt_load_id
from channels
where
    channel_dlt_load_id = (select max(channel_dlt_load_id) from channels)
    and channel_config_file = '{{ var("youtube_learn_config_file", "channels.yaml") }}'
