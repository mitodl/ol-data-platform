with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__see__api__courses') }}
)

select
    course_id
    , title                         as course_title
    , description                   as course_description
    , url                           as course_url
    , certification_type            as course_certification_type
    , topics                        as course_topic_raw
    , image_src                     as course_image_url
    , source_create_date            as course_created_on
    , source_last_modified_date     as course_updated_on
    , api_position                  as course_api_position
    , retrieved_at
from source
where course_id is not null
