-- MITx Online User Information

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__users_legaladdress') }}
)

, cleaned as (

    select
        id as user_address_id
        , country as user_address_country
        , state as user_address_state
        , user_id
    from source
)

select * from cleaned
