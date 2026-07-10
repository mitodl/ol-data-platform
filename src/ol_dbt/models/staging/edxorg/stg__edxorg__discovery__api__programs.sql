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
    -- dlt's per-row load watermark. raw__edxorg__discovery__api__programs is
    -- merge (not replace): a program that drops out of the API response is
    -- never deleted, only left un-upserted, so its _dlt_load_id stays behind
    -- the most recent load. Downstream models use this to tell "still active"
    -- rows from "merge left this behind" rows.
    , _dlt_load_id                               as program_dlt_load_id
from source
where uuid is not null
