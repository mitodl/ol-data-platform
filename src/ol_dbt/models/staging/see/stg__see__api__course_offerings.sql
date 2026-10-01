with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__see__api__course_offerings') }}
)

select
    co_title                        as courseoffering_title
    , course_id
    , start_date                    as courseoffering_start_date
    , end_date                      as courseoffering_end_date
    , delivery                      as courseoffering_delivery
    , format                        as courseoffering_format
    , duration                      as courseoffering_duration
    , price                         as courseoffering_price
    , continuing_ed_credits         as courseoffering_continuing_ed_credits
    , time_commitment               as courseoffering_time_commitment
    , location                      as courseoffering_location
    , currency                      as courseoffering_currency
    , faculty_name                  as courseoffering_faculty_names_raw
    , api_position                  as courseoffering_api_position
    , retrieved_at
from source
where course_id is not null
