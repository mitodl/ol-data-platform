with source as (
    select *
    from {{ source('ol_warehouse_raw_data', 'raw__mitlearn__app__postgres__learning_resources_userlist') }}
)

, cleaned as (
    select
        id as userlist_id
        , title as userlist_title
        , author_id as user_id
        , description as userlist_description
        , privacy_level as userlist_privacy_level
        , {{ cast_timestamp_to_iso8601('created_on') }} as userlist_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as userlist_updated_on
    from source
)

select * from cleaned
