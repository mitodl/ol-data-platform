with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__ovs__postgres__ui_edxendpoint') }}

)

, renamed as (

    select
        id as edxendpoint_id
        , name as edxendpoint_name
        , base_url as edxendpoint_base_url
        , edx_video_api_path as edxendpoint_edx_video_api_path
        ,{{ cast_timestamp_to_iso8601('created_at') }} as edxendpoint_created_on
        ,{{ cast_timestamp_to_iso8601('updated_at') }} as edxendpoint_updated_on
    from source

)

select * from renamed
