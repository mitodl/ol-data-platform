-- MITx Online User Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','mitxonline__app__postgres__users_user') }}
)

, cleaned as (
    select
        id
        , username
        , email
        , is_active
        , cast(created_on[1] as timestamp(6)) as created_on
        , cast(updated_on[1] as timestamp(6)) as updated_on
    from source
)

select * from cleaned
