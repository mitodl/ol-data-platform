with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__youtube__api__channels') }}
)

select
    channel_id
    , snippet__title                            as channel_title
    , snippet__description                      as channel_description
    , snippet__custom_url                       as channel_custom_url
    , snippet__published_at                     as channel_published_at
    , snippet__thumbnails__high__url            as channel_image_url
    , offered_by                                as channel_offered_by
    , config_file                               as channel_config_file
    -- Merge-loaded: a channel dropped from mitodl/open-video-data keeps the
    -- _dlt_load_id of the last load that saw it. See
    -- integrations__learn__youtube_channels.
    , _dlt_load_id                              as channel_dlt_load_id
from source
where channel_id is not null
