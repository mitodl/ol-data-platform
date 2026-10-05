with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__youtube__api__playlist_items') }}
)

select
    playlist_id
    , video_id
    , position                                  as playlist_item_position
    , _dlt_load_id                              as playlist_item_dlt_load_id
from source
where playlist_id is not null and video_id is not null
