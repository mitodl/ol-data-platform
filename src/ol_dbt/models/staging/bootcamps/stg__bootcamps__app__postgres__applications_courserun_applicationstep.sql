with
    source as (
        select *
        from
            {{
                source(
                    "ol_warehouse_raw_data", "raw__bootcamps__app__postgres__applications_bootcamprunapplicationstep"
                )
            }}
    ),
    cleaned as (

        select
            id as courserun_applicationstep_id,
            bootcamp_run_id as courserun_id,
            application_step_id as applicationstep_id,
            {{ cast_timestamp_to_iso8601("due_date") }} as courserun_applicationstep_due_date
        from source
    )

select *
from cleaned
