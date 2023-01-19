-- MITx Online Users Course Grades Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__courses_courserungrade') }}
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
        , {{ cast_timestamp_to_iso8601('created_on') }} as courserungrade_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as courserungrade_updated_on
    from source
)

select * from cleaned
