with
    source as (
        select * from {{ source("ol_warehouse_raw_data", "raw__mitx__openedx__mysql__courseware_studentmodule") }}
    )

    {{ deduplicate_raw_table(order_by="modified", partition_columns="course_id, student_id, module_id") }},
    cleaned as (

        select
            id as studentmodule_id,
            course_id as courserun_readable_id,
            module_id as coursestructure_block_id,
            module_type as coursestructure_block_category,
            student_id as user_id,
            state as studentmodule_state_data,
            grade as studentmodule_problem_grade,
            max_grade as studentmodule_problem_max_grade,
            to_iso8601(created) as studentmodule_created_on,
            to_iso8601(modified) as studentmodule_updated_on
        from most_recent_source
    )

select *
from cleaned
