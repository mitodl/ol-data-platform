with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__ovs__postgres__ui_collection') }}

)

, renamed as (

    select
        id as collection_id
        , key as collection_uuid
        , title as collection_title
        , slug as collection_slug
        , description as collection_description
        , edx_course_id as courserun_readable_id
        , owner as collection_user_id
        , is_logged_in_only as collection_is_logged_in_only
        , allow_share_openedx as collection_is_allowed_to_share
        , stream_source as collection_stream_source
        ,{{ cast_timestamp_to_iso8601('created_on') }} as collection_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as collection_updated_on

    from source

)

select * from renamed
