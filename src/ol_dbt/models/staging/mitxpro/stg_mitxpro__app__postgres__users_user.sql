-- xPro User Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','mitxpro__app__postgres__users_user') }}
)

, cleaned as (
    select
        -- int, sequential ID representing one user in xPro
        id
        -- boolean, ...
        , is_active
        -- timestamp, specifying when a user account was initially created
        , cast(created_on[1] as timestamp(6)) as created_on
        -- timestamp, specifying when a user account was most recently updated
        , cast(updated_on[1] as timestamp(6)) as updated_on
    from source
)

select * from cleaned
