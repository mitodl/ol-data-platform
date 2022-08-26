with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__bootcamps__app__postgres__auth_user') }}
)

, renamed as (
    select
        id
        , email
        , username
        , is_active
        , last_login
        , date_joined
    from source
)

select * from renamed
