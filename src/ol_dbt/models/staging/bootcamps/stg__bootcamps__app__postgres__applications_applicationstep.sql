with
    source as (
        select *
        from {{ source("ol_warehouse_raw_data", "raw__bootcamps__app__postgres__applications_applicationstep") }}
    ),
    cleaned as (

        select
            id as applicationstep_id,
            bootcamp_id as course_id,
            step_order as applicationstep_step_order,
            submission_type as applicationstep_submission_type
        from source
    )

select *
from cleaned
