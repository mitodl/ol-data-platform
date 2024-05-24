with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__ovs__postgres__ui_video') }}

)

, renamed as (

    select
        id as video_id
        , key as video_uuid
        , title as video_title
        , description as video_description
        , collection_id
        , source_url as video_source_url
        , status as video_status
        , multiangle as video_is_multiangle
        , is_logged_in_only as video_is_logged_in_only
        , is_public as video_is_public
        , is_private as video_is_private
        ,{{ cast_timestamp_to_iso8601('created_on') }} as video_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as video_updated_on

    from source

)

select * from renamed
