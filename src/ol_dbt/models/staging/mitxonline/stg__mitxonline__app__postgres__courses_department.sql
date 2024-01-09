-- MITx Online Course Department Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__courses_department') }}
)

, cleaned as (
    select
        id as coursedepartment_id
        , name as coursedepartment_name
        ,{{ cast_timestamp_to_iso8601('created_on') }} as coursedepartment_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as coursedepartment_updated_on
    from source
)

select * from cleaned
