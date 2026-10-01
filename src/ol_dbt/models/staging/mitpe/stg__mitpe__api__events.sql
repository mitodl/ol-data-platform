with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitpe__api__events') }}
)

select
    id                  as event_id
    , title             as event_title
    , start_date        as event_start_date_raw
    , end_date          as event_end_date_raw
    , time_range        as event_time_range_raw
    , summary           as event_summary
    , image             as event_image_src
    , url               as event_url
    , retrieved_at
from source
where id is not null
