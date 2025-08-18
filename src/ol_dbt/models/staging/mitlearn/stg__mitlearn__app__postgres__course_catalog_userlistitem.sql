with source as (
    select *
    from {{ source('ol_warehouse_raw_data', 'raw__mitlearn__app__postgres__course_catalog_userlistitem') }}
)

, cleaned as (
    select
        id as userlistitem_id
        , position as userlistitem_position
        , object_id as userlistitem_object_id
        , user_list_id as userlist_id
        , content_type_id as userlistitem_content_type_id
        , created_on as userlistitem_created_on
        , updated_on as userlistitem_updated_on
    from source
)

select * from cleaned
