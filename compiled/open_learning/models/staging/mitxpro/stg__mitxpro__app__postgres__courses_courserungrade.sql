-- MIT xPro Users Course Grades Information

with source as (
    select * from dev.main_raw.raw__xpro__app__postgres__courses_courserungrade
)

, cleaned as (
    select
        id as courserungrade_id
        , course_run_id as courserun_id
        , user_id
        , grade as courserungrade_grade
        , letter_grade as courserungrade_letter_grade
        , set_by_admin as courserungrade_is_set_by_admin
        , passed as courserungrade_is_passing
        ,
        to_iso8601(from_iso8601_timestamp(created_on))
        as courserungrade_created_on
        ,
        to_iso8601(from_iso8601_timestamp(updated_on))
        as courserungrade_updated_on
    from source
)

select * from cleaned
