-- MITx Pro open edX votes cast on discussion forum content

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__openedx__mysql__forum_uservote') }}
)

, cleaned as (

    select
        id as forumuservote_id
        , user_id as user_id
        , vote as forumuservote_value
        , content_type_id as forumcontent_type_id
        , content_object_id as forumcontent_object_id
    from source
)

select * from cleaned
