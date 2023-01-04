with couponpaymentversion as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_couponpaymentversion') }}
)

, couponpayment as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_couponpayment') }}
)

select
    couponpaymentversion.couponpaymentversion_id
    , couponpaymentversion.couponpaymentversion_expires_on
    , couponpaymentversion.company_id
    , couponpaymentversion.couponpaymentversion_num_coupon_codes
    , couponpaymentversion.couponpaymentversion_activated_on
    , couponpaymentversion.couponpaymentversion_coupon_type
    , couponpaymentversion.couponpaymentversion_created_on
    , couponpaymentversion.couponpaymentversion_discount_amount
    , couponpaymentversion.couponpaymentversion_updated_on
    , couponpaymentversion.couponpaymentversion_max_redemptions_per_user
    , couponpayment.couponpayment_name
    , couponpaymentversion.couponpaymentversion_is_automatic
    , couponpaymentversion.couponpaymentversion_payment_type
    , couponpaymentversion.couponpaymentversion_payment_transaction
    , couponpaymentversion.couponpaymentversion_tag
    , couponpaymentversion.couponpaymentversion_discount_type
    , couponpaymentversion.couponpaymentversion_max_redemptions
from couponpaymentversion
inner join couponpayment on couponpayment.couponpayment_id = couponpaymentversion.couponpayment_id
