with
    source as (
        select *
        from {{ source("ol_warehouse_raw_data", "raw__bootcamps__app__postgres__applications_bootcampapplication") }}
    ),
    cleaned as (

        select
            id as application_id,
            user_id,
            bootcamp_run_id as courserun_id,
            resume_file as application_resume_file,
            linkedin_url as application_linkedin_url,
            lower(state) as application_state,
            {{ cast_timestamp_to_iso8601("resume_upload_date") }} as application_resume_uploaded_on,
            {{ cast_timestamp_to_iso8601("created_on") }} as application_created_on,
            {{ cast_timestamp_to_iso8601("updated_on") }} as application_updated_on
        from source
    )

select *
from cleaned
