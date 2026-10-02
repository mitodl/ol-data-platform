with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitpe__api__news') }}
)

select
    id                  as news_id
    , title             as news_title
    , date              as news_date_raw
    , author            as news_author_raw
    , summary           as news_summary
    , image             as news_image_src
    , url               as news_url
    , retrieved_at
from source
where id is not null
