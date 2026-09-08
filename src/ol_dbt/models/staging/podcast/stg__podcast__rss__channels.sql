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
    -- The raw table is loaded with write_disposition="merge", so a podcast
    -- dropped from mitodl/open-podcast-data is never deleted -- it is simply
    -- left un-upserted and its _dlt_load_id stays behind at the last load that
    -- saw it. Carried through so the integrations layer can tell "still
    -- configured" from "merge left this behind". See
    -- integrations__learn__podcasts.
    , _dlt_load_id                              as podcast_dlt_load_id
from source
where rss_url is not null
