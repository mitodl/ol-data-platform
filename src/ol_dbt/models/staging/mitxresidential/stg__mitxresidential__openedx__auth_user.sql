with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__auth_user') }}
)

, cleaned as (
    select
        id as user_id
        , username as user_username
        , email as user_email
        , is_active as user_is_active
        , is_staff as user_is_staff
        , is_superuser as user_is_superuser
        , nullif(first_name, '') as user_first_name
        , nullif(last_name, '') as user_last_name
        , replace(
            replace(
                replace(
                    (concat(nullif(first_name, ''), ' ', nullif(last_name, '')))
                    , ' ', '<>'
                ), '><', ''
            ), '<>', ' '
        ) as user_full_name
        , to_iso8601(date_joined) as user_joined_on
        , to_iso8601(last_login) as user_last_login
    from source
)

select * from cleaned
