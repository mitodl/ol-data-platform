with
    source as (select * from {{ source("ol_warehouse_raw_data", "raw__edxorg__s3__tables__courseware_studentmodule") }})

    {{ deduplicate_raw_table(order_by="_airbyte_extracted_at", partition_columns="course_id, student_id, module_id") }},
    cleaned as (

        select
            cast(id as bigint) as studentmodule_id,
            course_id as courserun_readable_id,
            module_id as coursestructure_block_id,
            module_type as coursestructure_block_category,
            cast(student_id as integer) as user_id,
            state as studentmodule_state_data,
            try_cast(grade as double) as studentmodule_problem_grade,
            try_cast(max_grade as double) as studentmodule_problem_max_grade,
            to_iso8601(date_parse(created, '%Y-%m-%d %H:%i:%s')) as studentmodule_created_on,
            to_iso8601(date_parse(modified, '%Y-%m-%d %H:%i:%s')) as studentmodule_updated_on
        from most_recent_source
    )

select *
from cleaned
