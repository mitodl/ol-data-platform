with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__bootcamps__app__postgres__ecommerce_wiretransferreceipt') }}

)

, renamed as (
    select
        id as wiretransferreceipt_id
        , order_id
        , data as wiretransferreceipt_data
        ,{{ cast_timestamp_to_iso8601('created_on') }} as wiretransferreceipt_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as wiretransferreceipt_updated_on
        , wire_transfer_id as wiretransferreceipt_import_id
    from source

)

select * from renamed
