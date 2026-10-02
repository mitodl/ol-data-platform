{#
  integrations__learn__youtube_videos
  Exposes the videos of the playlists in integrations__learn__youtube_playlists
  for MIT Learn (webhook delivery). Mirrors transform_video in
  learning_resources/etl/youtube.py. One row per video, however many playlists
  it is in; integrations__learn__youtube_playlist_videos says which.

  A video is here only if it is in integrations__learn__youtube_playlist_videos,
  which applies the staleness rules.

  Left to the delivery asset, which does not exist yet:
  - offered_by. It is not a video attribute: transform_video takes it from the
    playlist being loaded, so it has to be set per playlist.
  - Cleaning description_raw, YouTube's raw text. MIT Learn runs it through
    clean_data (nh3) and clean_youtube_description, which drops boilerplate,
    timestamp, MIT course title and URL lines. The podcast delivery asset
    normalizes durations the same way.

  For a create_videos = false playlist MIT Learn does not create these as
  YouTube videos. It merges them into the matching OCW ContentFile's video, so
  url, title and description there come from the content file.
#}

with videos as (
    select * from {{ ref('stg__youtube__api__videos') }}
)

, current_videos as (
    select distinct video_readable_id
    from {{ ref('integrations__learn__youtube_playlist_videos') }}
)

select
    videos.video_id                                          as readable_id
    , videos.video_id                                        as youtube_id
    , videos.video_title                                     as title
    , videos.video_description                               as description_raw
    , 'https://www.youtube.com/watch?v=' || videos.video_id  as url
    , videos.video_image_url                                 as image_url
    , {{ cast_timestamp_to_iso8601('videos.video_published_at') }} as last_modified
    , videos.video_duration                                  as duration
    , 'youtube'                                              as etl_source
    , 'youtube'                                              as platform
    , 'video'                                                as resource_type
    , 'anytime'                                              as availability
    , true                                                   as published
from videos
inner join current_videos on videos.video_id = current_videos.video_readable_id
