with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__youtube__api__videos') }}
)

select
    video_id
    , snippet__channel_id                       as video_channel_id
    -- MIT Learn titles a video with snippet.localized.title, not snippet.title.
    , snippet__localized__title                 as video_title
    , snippet__description                      as video_description
    , snippet__thumbnails__high__url            as video_image_url
    , snippet__published_at                     as video_published_at
    , content_details__duration                 as video_duration
    , _dlt_load_id                              as video_dlt_load_id
from source
where video_id is not null
