with source as (

    select * from dev.main_raw.raw__bootcamps__app__postgres__ecommerce_wiretransferreceipt

)

, renamed as (
    select
        id as wiretransferreceipt_id
        , order_id
        , data as wiretransferreceipt_data
        ,
        wire_transfer_id as wiretransferreceipt_import_id
        ,
        to_iso8601(from_iso8601_timestamp(created_on))
        as wiretransferreceipt_created_on
        , to_iso8601(from_iso8601_timestamp(updated_on))
        as wiretransferreceipt_updated_on
    from source

)

select * from renamed
