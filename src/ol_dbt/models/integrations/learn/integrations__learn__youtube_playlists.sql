{#
  integrations__learn__youtube_playlists
  Exposes YouTube playlists for MIT Learn's video_playlist LearningResources
  (warehouse pull). Mirrors transform_playlist in
  learning_resources/etl/youtube.py. Videos are in
  integrations__learn__youtube_videos, ordered per playlist by
  integrations__learn__youtube_playlist_videos.

  STALENESS: same merge-load problem and fix as integrations__learn__youtube_channels:
  only playlists from the most recent load are kept. A playlist can only appear
  here if its channel does.

  create_videos = false (the OCW channel) is carried through, not resolved. For
  those playlists MIT Learn's load_playlist matches each video to an OCW
  ContentFile by youtube_id and publishes the playlist only if at least 60% of
  its videos match (OCW_PLAYLIST_VIDEO_THRESHOLD), so an empty one is never
  published (mit-learn #3882). The content files live in MIT Learn, so its
  pull task applies that rule. A create_videos = true playlist is published even
  when it has no videos, as on mit-learn main.
#}

with playlists as (
    select * from {{ ref('stg__youtube__api__playlists') }}
)

, channels as (
    select * from {{ ref('integrations__learn__youtube_channels') }}
)

select
    playlists.playlist_id                                    as readable_id
    , playlists.channel_id
    , playlists.playlist_title                               as title
    , 'https://www.youtube.com/playlist?list=' || playlists.playlist_id as url
    , playlists.playlist_image_url                           as image_url
    , playlists.playlist_title                               as image_alt
    , playlists.playlist_offered_by                          as offered_by
    , playlists.playlist_create_videos                       as create_videos
    , 'youtube'                                              as etl_source
    , 'youtube'                                              as platform
    , 'video_playlist'                                       as resource_type
    , 'anytime'                                              as availability
    , true                                                   as published
    , playlists.playlist_dlt_load_id                         as dlt_load_id
from playlists
inner join channels on playlists.channel_id = channels.channel_id
where playlists.playlist_dlt_load_id = (
    select max(playlist_loads.playlist_dlt_load_id) from playlists as playlist_loads
)
