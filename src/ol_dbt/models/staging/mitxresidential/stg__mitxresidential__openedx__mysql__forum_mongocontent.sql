-- Residential MITx open edX bridge from forum-v2 rows to their pre-migration mongo objectids

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitx__openedx__mysql__forum_mongocontent') }}
)

, cleaned as (

    select
        id as forummongocontent_id
        , mongo_id as forumcontent_mongo_id
        , content_type_id as forumcontent_type_id
        , content_object_id as forumcontent_object_id
    from source
)

select * from cleaned
