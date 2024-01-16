with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__openedx__mysql__courseware_studentmodule') }}
)

, cleaned as (

    select
        id as studentmodule_id
        , course_id as courserun_readable_id
        , module_id as coursestructure_block_id
        , module_type as coursestructure_block_category
        , student_id as openedx_user_id
        , state as studentmodule_state_data
        , grade as studentmodule_problem_grade
        , max_grade as studentmodule_problem_max_grade
        , to_iso8601(from_iso8601_timestamp_nanos(created)) as studentmodule_created_on
        , to_iso8601(from_iso8601_timestamp_nanos(modified)) as studentmodule_updated_on
    from source
)

select * from cleaned
