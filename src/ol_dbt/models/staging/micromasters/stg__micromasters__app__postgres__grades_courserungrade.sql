-- course run final grades from MicroMaster DB
with
    source as (
        select * from {{ source("ol_warehouse_raw_data", "raw__micromasters__app__postgres__grades_finalgrade") }}
    ),
    cleaned as (
        select
            id as courserungrade_id,
            user_id,
            course_run_id as courserun_id,
            grade as courserungrade_grade,
            passed as courserungrade_is_passing,
            status as courserungrade_status,
            course_run_paid_on_edx as courserungrade_courserun_paid_on_edx,
            {{ cast_timestamp_to_iso8601("created_on") }} as coursegrade_created_on,
            {{ cast_timestamp_to_iso8601("updated_on") }} as coursegrade_updated_on
        from source
    )

select *
from cleaned
