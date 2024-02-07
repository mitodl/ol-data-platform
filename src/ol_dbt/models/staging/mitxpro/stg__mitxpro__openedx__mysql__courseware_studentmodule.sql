with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__openedx__mysql__courseware_studentmodule') }}
)

--- guard against duplications by airbyte Incremental Sync - Append
, source_sorted as (
    select
        *
        , row_number() over (
            partition by id order by _airbyte_emitted_at desc
        ) as row_num
    from source
)

, most_recent_source as (
    select *
    from source_sorted
    where row_num = 1
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
    from most_recent_source
)

select * from cleaned
