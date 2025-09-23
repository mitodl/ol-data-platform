with
    source as (
        select *
        from {{ source("ol_warehouse_raw_data", "raw__mitxonline__openedx__mysql__grades_persistentcoursegrade") }}
    )

    {{ deduplicate_raw_table(order_by="modified", partition_columns="id") }}
select course_id, user_id, grading_policy_hash, percent_grade, letter_grade, passed_timestamp, created, modified
from most_recent_source
order by user_id
