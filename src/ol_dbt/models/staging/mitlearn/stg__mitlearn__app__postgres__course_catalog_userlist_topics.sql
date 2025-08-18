with source as (
    select *
    from {{ source('ol_warehouse_raw_data', 'raw__mitlearn__app__postgres__course_catalog_userlist_topics') }}
)

, cleaned as (
    select
        id as userlisttopics_id
        , userlist_id
        , coursetopic_id
    from source
)

select * from cleaned
