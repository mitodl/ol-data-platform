{#
  integrations__learn__youtube_videos
  Exposes the videos of the playlists in integrations__learn__youtube_playlists
  for MIT Learn (webhook delivery). Mirrors transform_video in
  learning_resources/etl/youtube.py. One row per video, however many playlists
  it is in; integrations__learn__youtube_playlist_videos says which.

  A video is here only if it is in a delivered playlist, so the same staleness
  rule applies. offered_by is not a video attribute: transform_video takes it
  from the playlist being loaded, so the delivery asset sets it per playlist.

  description is YouTube's raw text. MIT Learn runs it through clean_data (nh3)
  and clean_youtube_description, which drops boilerplate, timestamp, MIT course
  title and URL lines; the delivery asset applies the same cleaning, as the
  podcast delivery asset normalizes durations.

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
