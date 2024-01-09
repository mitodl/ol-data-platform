with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__courses_course_departments') }}
)

, cleaned as (
    select
        id as coursetodepartment_id
        , department_id as coursedepartment_id
        , course_id
    from source
)

select * from cleaned
