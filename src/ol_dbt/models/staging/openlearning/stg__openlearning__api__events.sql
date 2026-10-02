with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__openlearning__api__events') }}
)

select
    id                  as event_id
    , title             as event_title
    , path_alias        as event_path_alias
    , event_date        as event_start_on_raw
    , event_end_date    as event_end_on_raw
    , body_value        as event_body
    , body_summary      as event_body_summary
    , status = 'true'   as event_is_published
    , created           as event_created_on_raw
    , changed           as event_updated_on_raw
    , event_audience    as event_audience_json
    , event_category    as event_category_json
    , location_tag      as event_location_json
    , image_url         as event_image_src
    , image_alt         as event_image_alt
    , image_title       as event_image_title
    , retrieved_at
from source
where id is not null
