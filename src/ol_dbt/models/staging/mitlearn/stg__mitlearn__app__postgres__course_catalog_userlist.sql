with source as (
    select *
    from {{ source('ol_warehouse_raw_data', 'raw__mitlearn__app__postgres__course_catalog_userlist') }}
)

, cleaned as (
    select
        id as userlist_id
        , title as userlist_title
        , author_id as userlist_author_user_id
        , image_src as userlist_image_url
        , list_type as userlist_type
        , privacy_level as userlist_privacy_level
        , image_description as userlist_image_description
        , short_description as userlist_short_description
        , created_on as userlist_created_on
        , updated_on as userlist_updated_on
    from source
)

select * from cleaned
