-- MITx Online Course Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','mitxonline__app__postgres__courses_course') }}
)

, cleaned as (
    select
        id
        , live
        , title
        , program_id
        , readable_id
        , position_in_program
        , cast(created_on[1] as timestamp(6)) as created_on
        , cast(updated_on[1] as timestamp(6)) as updated_on
    from source
)

select * from cleaned
