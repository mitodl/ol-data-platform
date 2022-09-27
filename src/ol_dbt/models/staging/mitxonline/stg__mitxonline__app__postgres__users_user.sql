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
        , name as full_name
        , to_iso8601(from_iso8601_timestamp(created_on)) as joined_on
        , to_iso8601(from_iso8601_timestamp(last_login)) as last_login
    from source
)

select * from cleaned
