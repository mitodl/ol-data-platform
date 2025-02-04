{{ config(
    materialized='incremental',
    unique_key = ['studentmodulehistoryextended_id'],
    incremental_strategy='delete+insert',
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
)

, cleaned as (

    select
        id as studentmodulehistoryextended_id
        , student_module_id as studentmodule_id
        , state as studentmodule_state_data
        , grade as studentmodule_problem_grade
        , max_grade as studentmodule_problem_max_grade
        , created as studentmodule_created_on
    from source
)

select * from cleaned
