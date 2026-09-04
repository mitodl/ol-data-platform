with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__courses_course_departments') }}
)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at', partition_columns='id') }}

, cleaned as (
    select
        id as coursetodepartment_id
        , department_id as coursedepartment_id
        , course_id
    from most_recent_source
)

select * from cleaned
