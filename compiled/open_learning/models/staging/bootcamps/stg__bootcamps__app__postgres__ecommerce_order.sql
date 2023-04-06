with source as (

    select * from dev.main_raw.raw__bootcamps__app__postgres__ecommerce_order

)

, renamed as (
    select
        id as order_id
        , status as order_state
        , user_id as order_purchaser_user_id
        , application_id as application_id
        , payment_type as order_payment_type
        , total_price_paid as order_total_price_paid
        ,
        to_iso8601(from_iso8601_timestamp(created_on))
        as order_created_on
        ,
        to_iso8601(from_iso8601_timestamp(updated_on))
        as order_updated_on
    from source

)

select * from renamed
