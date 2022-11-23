-- MITx Online Course Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__courses_course') }}
)

, cleaned as (
    select
        id as course_id
        , live as course_is_live
        , title as course_title
        , readable_id as course_readable_id
        , to_iso8601(from_iso8601_timestamp(created_on)) as course_created_on
        , to_iso8601(from_iso8601_timestamp(updated_on)) as course_updated_on
    from source
)

select * from cleaned
