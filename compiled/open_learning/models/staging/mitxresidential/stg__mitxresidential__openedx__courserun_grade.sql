with source as (
    select * from dev.main_raw.raw__mitx__openedx__mysql__grades_persistentcoursegrade
)

, cleaned as (
    select
        id as courserungrade_id
        , course_id as courserun_readable_id
        , user_id
        , percent_grade as courserungrade_grade
        , letter_grade as courserungrade_letter_grade
        ,
        to_iso8601(from_iso8601_timestamp(passed_timestamp))
        as courserungrade_passed_on
        ,
        to_iso8601(from_iso8601_timestamp(created))
        as courserungrade_created_on
        ,
        to_iso8601(from_iso8601_timestamp(modified))
        as courserungrade_updated_on
    from source
)

select * from cleaned
