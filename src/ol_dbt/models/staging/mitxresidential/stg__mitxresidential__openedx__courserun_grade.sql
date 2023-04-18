with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__grades_persistentcoursegrade') }}
)

, cleaned as (
    select
        id as courserungrade_id
        , course_id as courserun_readable_id
        , user_id
        , percent_grade as courserungrade_grade
        , letter_grade as courserungrade_letter_grade
        ,{{ cast_timestamp_to_iso8601('passed_timestamp') }} as courserungrade_passed_on
        ,{{ cast_timestamp_to_iso8601('created') }} as courserungrade_created_on
        ,{{ cast_timestamp_to_iso8601('modified') }} as courserungrade_updated_on
    from source
)

select * from cleaned
