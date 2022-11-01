-- MITxPro Program Run Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__courses_programrun') }}
)

, cleaned as (
    select
        id as programrun_id
        , program_id
        , run_tag as programrun_tag
        , to_iso8601(from_iso8601_timestamp(start_date)) as programrun_start_on
        , to_iso8601(from_iso8601_timestamp(end_date)) as programrun_end_on
        , to_iso8601(from_iso8601_timestamp(created_on)) as programrun_created_on
        , to_iso8601(from_iso8601_timestamp(updated_on)) as programrun_updated_on
    from source
)

select * from cleaned
