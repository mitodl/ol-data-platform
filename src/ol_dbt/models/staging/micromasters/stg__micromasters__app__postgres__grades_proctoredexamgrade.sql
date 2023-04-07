-- DEDP proctor exam grades from MicroMasters DB

with source as (
    select *
    from {{ source('ol_warehouse_raw_data','raw__micromasters__app__postgres__grades_proctoredexamgrade') }}
)

, cleaned as (
    select
        id as proctoredexamgrade_id
        , user_id
        , course_id
        , passing_score as proctoredexamgrade_passing_score
        , passed as proctoredexamgrade_is_passing
        , score as proctoredexamgrade_score
        , grade as proctoredexamgrade_letter_grade
        , percentage_grade as proctoredexamgrade_percentage_grade
        , exam_run_id as examrun_id
        , {{ cast_timestamp_to_iso8601('exam_date') }} as proctoredexamgrade_exam_on
        , {{ cast_timestamp_to_iso8601('created_on') }} as proctoredexamgrade_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as proctoredexamgrade_updated_on

    from source
)

select * from cleaned
