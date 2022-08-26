-- MITx Online User Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__users_user') }}
)

, cleaned as (
    select
        id
        , username
        , email
        , is_active
        , created_on
        , updated_on
    from source
)

select * from cleaned
