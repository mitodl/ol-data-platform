-- MITx Online User Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__users_user') }}
)

, cleaned as (

    select
        id as user_id
        , username as user_username
        , email as user_email
        , is_active as user_is_active
        , replace(replace(replace(name, ' ', '<>'), '><', ''), '<>', ' ') as user_full_name
        ,{{ cast_timestamp_to_iso8601('created_on') }} as user_joined_on
        ,{{ cast_timestamp_to_iso8601('last_login') }} as user_last_login
    from source
)

select * from cleaned
