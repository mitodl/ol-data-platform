with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__youtube__api__playlists') }}
)

select
    playlist_id
    , channel_id
    , snippet__title                            as playlist_title
    , snippet__description                      as playlist_description
    , snippet__thumbnails__high__url            as playlist_image_url
    , snippet__published_at                     as playlist_published_at
    , content_details__item_count               as playlist_item_count
    , offered_by                                as playlist_offered_by
    , create_videos                             as playlist_create_videos
    , _dlt_load_id                              as playlist_dlt_load_id
from source
where playlist_id is not null
