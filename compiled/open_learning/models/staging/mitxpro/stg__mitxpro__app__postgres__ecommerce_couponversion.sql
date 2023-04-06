with source as (

    select * from dev.main_raw.raw__xpro__app__postgres__ecommerce_couponversion

)

, renamed as (
    select
        id as couponversion_id
        , coupon_id
        , payment_version_id as couponpaymentversion_id
        ,
        to_iso8601(from_iso8601_timestamp(updated_on))
        as couponversion_updated_on
        ,
        to_iso8601(from_iso8601_timestamp(created_on))
        as couponversion_created_on
    from source
)

select * from renamed
