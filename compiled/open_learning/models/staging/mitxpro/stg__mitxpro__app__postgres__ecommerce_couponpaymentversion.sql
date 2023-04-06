with source as (

    select * from dev.main_raw.raw__xpro__app__postgres__ecommerce_couponpaymentversion

)

, renamed as (

    select
        id as couponpaymentversion_id
        , company_id
        , num_coupon_codes as couponpaymentversion_num_coupon_codes
        , coupon_type as couponpaymentversion_coupon_type
        , amount as couponpaymentversion_discount_amount
        , max_redemptions_per_user as couponpaymentversion_max_redemptions_per_user
        , payment_id as couponpayment_id
        , automatic as couponpaymentversion_is_automatic
        , payment_type as couponpaymentversion_discount_source
        , payment_transaction as couponpaymentversion_payment_transaction
        , tag as couponpaymentversion_tag
        , discount_type as couponpaymentversion_discount_type
        , max_redemptions as couponpaymentversion_max_redemptions
        ,
        to_iso8601(from_iso8601_timestamp(expiration_date))
        as couponpaymentversion_expires_on
        ,
        to_iso8601(from_iso8601_timestamp(activation_date))
        as couponpaymentversion_activated_on
        ,
        to_iso8601(from_iso8601_timestamp(created_on))
        as couponpaymentversion_created_on
        ,
        to_iso8601(from_iso8601_timestamp(updated_on))
        as couponpaymentversion_updated_on

    from source

)

select * from renamed
