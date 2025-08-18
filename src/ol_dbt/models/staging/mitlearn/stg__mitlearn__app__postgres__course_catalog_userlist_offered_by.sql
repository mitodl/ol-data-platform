with source as (
    select *
    from {{ source('ol_warehouse_raw_data', 'raw__mitlearn__app__postgres__course_catalog_userlist_offered_by') }}
)

, cleaned as (
    select
        id as userlistofferedby_id
        , userlist_id
        , learningresourceofferor_id
    from source
)

select * from cleaned
