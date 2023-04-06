with source as (

    select * from dev.main_raw.raw__xpro__app__postgres__ecommerce_receipt

)

, renamed as (

    select
        id as receipt_id
        , order_id
        , data as receipt_data
        ,
        to_iso8601(from_iso8601_timestamp(created_on))
        as receipt_created_on
        ,
        to_iso8601(from_iso8601_timestamp(updated_on))
        as receipt_updated_on
    from source

)

select * from renamed
