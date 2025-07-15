with source as (
    select * from
    {{
      source('ol_warehouse_raw_data'
      ,'raw__mitx__openedx__mysql__courseware_studentmodulehistory')
    }}
)

--- this is needed to deduplicate the data from raw table
{{ deduplicate_raw_table(order_by='_airbyte_extracted_at' , partition_columns = 'id') }}
, cleaned as (
    select
        id as studentmodulehistory_id
        , student_module_id as studentmodule_id
        , state as studentmodule_state_data
        , grade as studentmodule_problem_grade
        , max_grade as studentmodule_problem_max_grade
        , created as studentmodule_created_on
    from most_recent_source
)

select * from cleaned
