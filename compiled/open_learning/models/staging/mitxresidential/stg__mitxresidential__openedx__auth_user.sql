with source as (
    select * from dev.main_raw.raw__mitx__openedx__mysql__auth_user
)

, cleaned as (
    select
        id as user_id
        , username as user_username
        , email as user_email
        , first_name as user_first_name
        , last_name as user_last_name
        , is_active as user_is_active
        , is_staff as user_is_staff
        , is_superuser as user_is_superuser
        ,
        to_iso8601(from_iso8601_timestamp(date_joined))
        as user_joined_on
        ,
        to_iso8601(from_iso8601_timestamp(last_login))
        as user_last_login
    from source
)

select * from cleaned
