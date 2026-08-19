with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__podcast__rss__channels') }}
)

select
    readable_id                                 as podcast_dlt_readable_id
    , rss_url                                   as podcast_rss_url
    , website                                   as podcast_website
    , title                                     as podcast_title
    , description                               as podcast_description
    , language                                  as podcast_language
    , last_build_date                           as podcast_last_build_date_raw
    , image_url                                 as podcast_image_url
    , offered_by                                as podcast_offered_by
    , topics                                    as podcast_topics_raw
    , apple_podcasts_url                        as podcast_apple_podcasts_url
    , google_podcasts_url                       as podcast_google_podcasts_url
from source
where rss_url is not null
