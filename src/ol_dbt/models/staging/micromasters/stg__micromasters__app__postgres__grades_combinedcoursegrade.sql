-- DEDP course final grades combined from course run and exam from MicroMaster DB

with source as (
    select *
    from {{ source('ol_warehouse_raw_data','raw__micromasters__app__postgres__grades_combinedfinalgrade') }}
)

, cleaned as (
    select
        id as coursegrade_id
        , user_id
        , course_id
        , grade as coursegrade_grade
        , {{ cast_timestamp_to_iso8601('created_on') }} as coursegrade_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as coursegrade_updated_on


    from source
)

select * from cleaned
