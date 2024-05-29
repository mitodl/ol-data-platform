with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__openedx__mysql__edxval_video') }}
)

, cleaned as (

    select
        id as video_id
        , edx_video_id as video_edx_uuid
        , client_video_id as video_client_id
        , status as video_status
        , duration as video_duration
        , {{ cast_timestamp_to_iso8601('created') }} as video_created_on

    from source
)

select * from cleaned
