-- MITx Online User Information

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__users_legaladdress') }}
)

, cleaned as (

    select
        id
        , country as user_address_country
        , user_id
        , first_name
        , last_name
    from source
)

select * from cleaned
