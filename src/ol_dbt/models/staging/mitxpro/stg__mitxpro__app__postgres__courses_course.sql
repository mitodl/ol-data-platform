-- xPro Online Course Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__courses_course') }}
)

, cleaned as (
    select
        id
        , live
        , title
        , program_id
        , readable_id as course_readable_id
        , position_in_program
        , created_on
        , updated_on
    from source
)

select * from cleaned
