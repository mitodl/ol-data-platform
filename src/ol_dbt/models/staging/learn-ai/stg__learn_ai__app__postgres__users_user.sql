with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__learn_ai__app__postgres__users_user') }}
)

{{ deduplicate_raw_table(order_by='updated_on' , partition_columns = 'id') }}
, cleaned as (
    select
        id as user_id
        , global_id as user_global_id
        , username as user_username
        , email as user_email
        , name as user_full_name
        , is_active as user_is_active
        , is_staff as user_is_staff
        , is_superuser as user_is_superuser
        , {{ cast_timestamp_to_iso8601('created_on') }} as user_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as user_updated_on
        , {{ cast_timestamp_to_iso8601('last_login') }} as user_last_login
    from most_recent_source
)

select * from cleaned
