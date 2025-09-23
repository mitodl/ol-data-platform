with
    source as (select * from {{ source("ol_warehouse_raw_data", "raw__irx__edxorg__bigquery__email_opt_in") }}),
    cleaned as (

        select
            user_id,
            email as user_email,
            username as user_username,
            full_name as user_full_name,
            is_opted_in_for_email as user_is_opted_in_for_email,
            course_id as courserun_readable_id,
            {{ translate_course_id_to_platform("course_id") }} as courserun_platform,
            {{ cast_timestamp_to_iso8601("preference_set_datetime") }} as user_email_opt_in_updated_on
        from source

    )

select *
from cleaned
