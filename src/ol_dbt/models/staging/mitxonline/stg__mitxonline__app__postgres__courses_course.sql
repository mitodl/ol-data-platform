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
        , replace(replace(readable_id, 'course-v1:', ''), '+', '/') as course_edx_readable_id
        , split(readable_id, '+')[2] as course_number
        , {{ cast_timestamp_to_iso8601('created_on') }} as course_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as course_updated_on
    from source
)

select * from cleaned
