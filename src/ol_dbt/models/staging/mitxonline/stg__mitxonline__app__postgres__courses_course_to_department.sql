-- MITx Online Course to Department Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__courses_department') }}
)

, cleaned as (
    select
        id as coursetodepartment_id
        , course_id
    from source
)

select * from cleaned
