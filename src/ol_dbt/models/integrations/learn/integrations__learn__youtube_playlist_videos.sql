{#
  integrations__learn__youtube_playlist_videos
  The videos of each playlist in integrations__learn__youtube_playlists, in
  playlist order. MIT Learn stores this as PLAYLIST_VIDEOS relationships with a
  position.

  STALENESS: playlist items and videos are each taken from their own table's
  newest load. Every load of a table is a full snapshot of it, including a
  Dagster run that materializes only some of the youtube assets, so the newest
  load is the current set. A video removed from a playlist, or made private,
  is not in it. A playlist that became empty has no rows in it, so it gets no
  videos here.

  position is zero-based and dense over the videos that exist. For a
  create_videos = true playlist that matches load_playlist's enumerate() over
  the videos YouTube returned. For create_videos = false, Learn enumerates only
  the videos it matched to OCW content files, so its positions are dense over
  those and differ from position here wherever a video went unmatched.
#}

with playlist_items as (
    select * from {{ ref('stg__youtube__api__playlist_items') }}
    where
        playlist_item_dlt_load_id = (
            select max(playlist_item_loads.playlist_item_dlt_load_id)
            from {{ ref('stg__youtube__api__playlist_items') }} as playlist_item_loads
        )
)

, videos as (
    select * from {{ ref('stg__youtube__api__videos') }}
    where
        video_dlt_load_id = (
            select max(video_loads.video_dlt_load_id)
            from {{ ref('stg__youtube__api__videos') }} as video_loads
        )
)

, playlists as (
    select * from {{ ref('integrations__learn__youtube_playlists') }}
)

select
    playlists.readable_id                                    as playlist_readable_id
    , playlist_items.video_id                                as video_readable_id
    , row_number() over (
        partition by playlists.readable_id
        order by playlist_items.playlist_item_position
    ) - 1                                                    as position
from playlist_items
inner join playlists on playlist_items.playlist_id = playlists.readable_id
inner join videos on playlist_items.video_id = videos.video_id
