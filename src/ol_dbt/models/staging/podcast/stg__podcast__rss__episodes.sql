with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__podcast__rss__episodes') }}
)

select
    readable_id                                 as episode_readable_id
    , channel_readable_id                       as podcast_dlt_readable_id
    , channel_rss_url                           as podcast_rss_url
    , title                                     as episode_title
    , description                               as episode_description
    , url                                       as episode_url
    , audio_url                                 as episode_audio_url
    , episode_link                              as episode_link
    , duration                                  as episode_duration_raw
    , pub_date                                  as episode_published_on_raw
    , image_url                                 as episode_image_url
from source
where readable_id is not null and audio_url is not null
