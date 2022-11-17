with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__courses_programrun') }}
)

, renamed as (

    select
        id as programrun_id
        , run_tag as programrun_tag
        , program_id
        , to_iso8601(from_iso8601_timestamp(start_date)) as programrun_start_on
        , to_iso8601(from_iso8601_timestamp(end_date)) as programrun_end_on
        , to_iso8601(from_iso8601_timestamp(updated_on)) as programrun_updated_on
        , to_iso8601(from_iso8601_timestamp(created_on)) as programrun_created_on
    from source

)

select * from renamed
