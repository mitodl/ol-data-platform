with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__courses_program_departments') }}
)

{{ deduplicate_raw_table(raw_table='raw__mitxonline__app__postgres__courses_program_departments', partition_columns='id') }}

, cleaned as (
    select
        id as programtodepartment_id
        , department_id as coursedepartment_id
        , program_id
    from most_recent_source
)

select * from cleaned
