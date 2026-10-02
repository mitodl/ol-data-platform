{#
  integrations__learn__youtube_playlist_videos
  The videos of each playlist in integrations__learn__youtube_playlists, in
  playlist order. MIT Learn stores this as PLAYLIST_VIDEOS relationships with a
  position.

  Every resource of one dlt run shares a _dlt_load_id, so a membership or video
  is current exactly when it carries its playlist's load id. A video removed from
  a playlist, or made private, keeps an older load id and drops out. Filtering on
  the playlist's own load rather than the newest membership load matters for a
  playlist that becomes empty: it has no rows in the newest load, and its newest
  remaining rows are the stale ones.

  position is dense and zero-based over the videos that exist, matching
  load_playlist's enumerate() over the videos YouTube returned. The raw position
  also counts private and deleted entries.
#}

with playlist_items as (
    select * from {{ ref('stg__youtube__api__playlist_items') }}
)

, videos as (
    select * from {{ ref('stg__youtube__api__videos') }}
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
inner join playlists
    on
        playlist_items.playlist_id = playlists.readable_id
        and playlist_items.playlist_item_dlt_load_id = playlists.dlt_load_id
inner join videos
    on
        playlist_items.video_id = videos.video_id
        and videos.video_dlt_load_id = playlists.dlt_load_id
