with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__bootcamps__app__postgres__klasses_personalprice') }}
)

, cleaned as (

    select
        id as personalprice_id
        , user_id
        , bootcamp_run_id as courserun_id
        , cast(price as decimal(38, 2)) as personalprice
    from source
)

select * from cleaned
