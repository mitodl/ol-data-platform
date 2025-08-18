-- MIT Learn User Information

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitlearn__app__postgres__users_user') }}
)

, cleaned as (
    select
        id as user_id
        , username as user_username
        , email as user_email
        , is_active as user_is_active
        , is_staff as user_is_staff
        , is_superuser as user_is_superuser
        , first_name as user_first_name
        , last_name as user_last_name
        , scim_id as user_scim_id
        , scim_username as user_scim_username
        , scim_external_id as user_scim_external_id
        , coalesce(nullif(global_id, ''), scim_external_id) as user_global_id
        , {{ cast_timestamp_to_iso8601('created_on') }} as user_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as user_updated_on
        , {{ cast_timestamp_to_iso8601('date_joined') }} as user_joined_on
        , {{ cast_timestamp_to_iso8601('last_login') }} as user_last_login
    from source
)

select * from cleaned
