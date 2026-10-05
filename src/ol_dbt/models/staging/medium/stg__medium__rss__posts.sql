with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__medium__rss__posts') }}
)

select
    guid                    as post_guid
    , title                 as post_title
    , link                  as post_url
    , description           as post_description
    , content_encoded       as post_content
    , creators              as post_creators_json
    , categories            as post_categories_json
    , pub_date              as post_published_on_raw
    , updated               as post_updated_on_raw
    , feed_url
    , feed_title
    , feed_description
    , feed_image_url
    , feed_image_title
    , retrieved_at
from source
where guid is not null
