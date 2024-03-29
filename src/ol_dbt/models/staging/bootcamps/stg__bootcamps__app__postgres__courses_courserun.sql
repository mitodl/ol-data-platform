-- Bootcamps Course Run Information

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__bootcamps__app__postgres__klasses_bootcamprun') }}
)

, cleaned as (
    select
        id as courserun_id
        , bootcamp_id as course_id
        , title as courserun_title
        , bootcamp_run_id as courserun_readable_id
        ,{{ cast_timestamp_to_iso8601('start_date') }} as courserun_start_on
        ,{{ cast_timestamp_to_iso8601('end_date') }} as courserun_end_on
    from source
)

select * from cleaned
