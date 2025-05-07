with coupon as (
    select *
    from {{ ref('int__mitxpro__ecommerce_coupon') }}
)

, ecommerce_couponversion as (
    select *
    from {{ ref('int__mitxpro__ecommerce_couponversion') }}
)

, ecommerce_couponpaymentversion as (
    select *
    from {{ ref('int__mitxpro__ecommerce_couponpaymentversion') }}
)

, b2becommerce_b2bcoupon as (
    select *
    from {{ ref('int__mitxpro__b2becommerce_b2bcoupon') }}
)

, latest_couponversion as (
    select
        coupon_id
        , max(couponversion_updated_on) as max_couponversion_updated_on
    from ecommerce_couponversion
    group by coupon_id
)

, reg_coupon_fields as (
    select
        coupon.coupon_code
        , coupon.couponpayment_name as coupon_name
        , coupon.coupon_created_on
        , ecommerce_couponpaymentversion.couponpaymentversion_payment_transaction as payment_transaction
        , ecommerce_couponpaymentversion.couponpaymentversion_coupon_type as coupon_type
        , ecommerce_couponpaymentversion.couponpaymentversion_discount_source as discount_source
        , ecommerce_couponpaymentversion.couponpaymentversion_activated_on as activated_on
        , ecommerce_couponpaymentversion.couponpaymentversion_expires_on as expires_on
        , 'ecommerce_coupon' as coupon_source_table
        , null as b2bcoupon_id
        , coupon.coupon_id
        , ecommerce_couponpaymentversion.couponpaymentversion_discount_amount_text as discount_amount
    from coupon
    inner join latest_couponversion
        on coupon.coupon_id = latest_couponversion.coupon_id
    inner join ecommerce_couponversion
        on
            latest_couponversion.coupon_id = ecommerce_couponversion.coupon_id
            and latest_couponversion.max_couponversion_updated_on = ecommerce_couponversion.couponversion_updated_on
    inner join ecommerce_couponpaymentversion
        on ecommerce_couponversion.couponpaymentversion_id = ecommerce_couponpaymentversion.couponpaymentversion_id
)

, b2b_coupon_fields as (
    select
        b2bcoupon_coupon_code as coupon_code
        , b2bcoupon_name as coupon_name
        , b2bcoupon_created_on as coupon_created_on
        , null as payment_transaction
        , null as coupon_type
        , null as discount_source
        , b2bcoupon_activated_on as activated_on
        , b2bcoupon_expires_on as expires_on
        , 'b2bcoupon' as coupon_source_table
        , b2bcoupon_id
        , null as coupon_id
        , cast(cast((b2bcoupon_discount_percent * 100) as integer) as varchar) || '% off' as discount_amount
    from b2becommerce_b2bcoupon
)

select
    coupon_code
    , coupon_name
    , coupon_created_on
    , payment_transaction
    , discount_amount
    , coupon_type
    , discount_source
    , activated_on
    , expires_on
    , coupon_source_table
    , b2bcoupon_id
    , coupon_id
from reg_coupon_fields

union all

select
    coupon_code
    , coupon_name
    , coupon_created_on
    , payment_transaction
    , discount_amount
    , coupon_type
    , discount_source
    , activated_on
    , expires_on
    , coupon_source_table
    , b2bcoupon_id
    , coupon_id
from b2b_coupon_fields
