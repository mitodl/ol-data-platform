-- MITx Online open edX abuse flags that were raised on discussion forum content and later cleared

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__openedx__mysql__forum_historicalabuseflagger') }}
)

, cleaned as (

    select
        id as forumhistoricalabuseflag_id
        , user_id as user_id
        , {{ cast_timestamp_to_iso8601('flagged_at') }} as forumhistoricalabuseflag_flagged_on
        , content_type_id as forumcontent_type_id
        , content_object_id as forumcontent_object_id
    from source
)

select * from cleaned
