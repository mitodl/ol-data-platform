with source as (

    select * from dev.main_raw.raw__micromasters__app__postgres__ecommerce_order

)

, renamed as (

    select
        id as order_id
        , status as order_state
        , user_id
        , reference_number as order_reference_number
        , total_price_paid as order_total_price_paid
        ,
        to_iso8601(from_iso8601_timestamp(created_at))
        as order_created_on
        ,
        to_iso8601(from_iso8601_timestamp(modified_at))
        as order_updated_on
    from source

)

select * from renamed
