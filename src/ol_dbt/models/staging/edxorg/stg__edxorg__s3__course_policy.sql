with
    source as (select * from {{ source("ol_warehouse_raw_data", "raw__edxorg__s3__course_structure__course_policy") }})

    {{ deduplicate_raw_table(order_by="_airbyte_extracted_at", partition_columns="course_id") }},
    cleaned as (

        select
            course_id as courserun_readable_id,
            trim(both '"' from display_coursenumber) as display_coursenumber,  -- noqa: PRS
            self_paced as courserun_is_self_paced,
            nullif(tabs, '[]') as courserun_tabs_array,
            nullif(advanced_modules, '[]') as course_advanced_module_array,
            discussions_settings as course_discussion_settings_object
        from most_recent_source
    )

select *
from cleaned
