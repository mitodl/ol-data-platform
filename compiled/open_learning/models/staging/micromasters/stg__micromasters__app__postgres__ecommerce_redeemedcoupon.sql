with source as (

    select * from dev.main_raw.raw__micromasters__app__postgres__ecommerce_redeemedcoupon

)

, renamed as (

    select
        id as redeemedcoupon_id
        , order_id
        , coupon_id
        ,
        to_iso8601(from_iso8601_timestamp(created_on))
        as redeemedcoupon_created_on
        ,
        to_iso8601(from_iso8601_timestamp(updated_on))
        as redeemedcoupon_updated_on

    from source

)

select * from renamed
