with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__edxorg__discovery__api__programs') }}
)

select
    uuid                                        as program_uuid
    , title                                     as program_title
    , coalesce(subtitle, overview)              as program_description
    , marketing_url                             as program_marketing_url
    , status                                    as program_status
    , type                                      as program_type
    , card_image_url                            as program_image_url
    -- JSON array strings — downstream models extract individual fields
    , subjects                                  as program_subjects_json
    , courses                                   as program_courses_json
from source
where uuid is not null
