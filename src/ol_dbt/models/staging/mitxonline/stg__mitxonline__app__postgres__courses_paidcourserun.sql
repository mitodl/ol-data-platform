-- MITx Online's record of which course run an order pays for

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__courses_paidcourserun') }}
)

{{ deduplicate_raw_table(raw_table='raw__mitxonline__app__postgres__courses_paidcourserun', partition_columns='id') }}

, cleaned as (
    select
        id as paidcourserun_id
        , user_id
        , course_run_id as courserun_id
        , order_id
        , {{ cast_timestamp_to_iso8601('created_on') }} as paidcourserun_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as paidcourserun_updated_on
    from most_recent_source
)

select * from cleaned
