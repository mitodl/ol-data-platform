{{ config(
    materialized='incremental',
    unique_key = 'studentmodulehistoryextended_id',
    incremental_strategy='delete+insert',
    views_enabled=false,
  )
}}

with source as (

    select * from
    {{
      source('ol_warehouse_raw_data'
      ,'raw__mitx__openedx__mysql__coursewarehistoryextended_studentmodulehistoryextended')
    }}

    {% if is_incremental() %}
        where created >= (select max(this.studentmodule_created_on) from {{ this }} as this)
    {% endif %}


    {% if not is_incremental() %}

        union all
         --- ONLY run on full-refresh / first build to backfill data from the old raw table
        select * from
        {{
          source('ol_warehouse_raw_data', 'raw__mitx__openedx__mysql__courseware_studentmodulehistory')
        }}
    {% endif %}

)

--- this is needed for the initial dbt run to deduplicate the data from raw table
{{ deduplicate_raw_table(order_by='_airbyte_extracted_at' , partition_columns = 'id') }}
, cleaned as (

    select
        id as studentmodulehistoryextended_id
        , student_module_id as studentmodule_id
        , state as studentmodule_state_data
        , grade as studentmodule_problem_grade
        , max_grade as studentmodule_problem_max_grade
        , created as studentmodule_created_on
    from most_recent_source
)

select * from cleaned
