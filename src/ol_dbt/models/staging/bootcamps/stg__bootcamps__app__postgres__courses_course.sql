-- Bootcamps Course Information

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__bootcamps__app__postgres__klasses_bootcamp') }}
)

, cleaned as (
    select
        id as course_id
        , title as course_title
        , '' as course_readable_id --- placeholder to add course_readable_id from bootcamps application
    from source
)

select * from cleaned
