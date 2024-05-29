with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__ovs__postgres__ui_collectionedxendpoint') }}

)

, renamed as (

    select
        id as collectionedxendpoint_id
        , collection_id
        , edx_endpoint_id as edxendpoint_id
    from source

)

select * from renamed
