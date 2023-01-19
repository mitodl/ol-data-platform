-- MicroMasters User Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__micromasters__app__postgres__auth_user') }}
)

, cleaned as (

    select
        id as user_id
        , username as user_username
        , email as user_email
        , is_active as user_is_active
        , {{ cast_timestamp_to_iso8601('date_joined') }} as user_joined_on
        , {{ cast_timestamp_to_iso8601('last_login') }} as user_last_login
    from source
)

select * from cleaned
