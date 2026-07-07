with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mit_climate__api__articles') }}
)

select
    uuid                                        as article_uuid
    , url                                       as article_url
    , title                                     as article_title
    , summary                                   as article_summary
    , image_src                                 as article_image_src
    , image_alt                                 as article_image_alt
    , topics                                    as article_topics_raw
    , created                                   as article_created_on_raw
    , footnotes                                 as article_footnotes
    , byline                                    as article_byline
    , feed_type                                 as article_feed_type
from source
