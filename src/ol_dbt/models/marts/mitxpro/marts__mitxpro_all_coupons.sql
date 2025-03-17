with allcoupons as (
    select *
    from {{ ref('int__mitxpro__ecommerce_allcoupons') }}
)

, allorders as (
    select *
    from {{ ref('int__mitxpro__ecommerce_allorders') }}
)

, ecommerce_coupon as (
    select *
    from {{ ref('int__mitxpro__ecommerce_coupon') }}
)

, ecommerce_couponpaymentversion as (
    select *
    from {{ ref('int__mitxpro__ecommerce_couponpaymentversion') }}
)

, ecommerce_company as (
    select *
    from {{ ref('int__mitxpro__ecommerce_company') }}
)

, redeemed_coupons as (
    select coupon_id
    from allorders
    where redeemed = true
    group by coupon_id
)

, redeemed_b2b_coupons as (
    select 
        b2bcoupon_id
        , b2border_contract_number
    from allorders
    where
        redeemed = true
        and coupon_id is null
    group by 
        b2bcoupon_id
        , b2border_contract_number
)

select
    allcoupons.coupon_code
    , allcoupons.coupon_name
    , allcoupons.coupon_created_on
    , allcoupons.payment_transaction
    , allcoupons.discount_amount
    , allcoupons.coupon_type
    , allcoupons.discount_source
    , ecommerce_couponpaymentversion.couponpaymentversion_activated_on as activated_on
    , ecommerce_couponpaymentversion.couponpaymentversion_expires_on as expires_on
    , allcoupons.coupon_source_table
    , allcoupons.b2bcoupon_id
    , allcoupons.coupon_id
    , ecommerce_couponpaymentversion.couponpaymentversion_num_coupon_codes
    , ecommerce_couponpaymentversion.couponpaymentversion_max_redemptions
    , ecommerce_couponpaymentversion.couponpayment_name
    , ecommerce_couponpaymentversion.couponpaymentversion_id
    , ecommerce_couponpaymentversion.couponpaymentversion_created_on
    , ecommerce_couponpaymentversion.couponpaymentversion_discount_amount_text
    , ecommerce_company.company_name
    , redeemed_b2b_coupons.b2border_contract_number
    , case
        when
            redeemed_coupons.coupon_id is not null
            or redeemed_b2b_coupons.b2bcoupon_id is not null
            then true
        when
            redeemed_coupons.coupon_id is null
            and redeemed_b2b_coupons.b2bcoupon_id is null
            then false
    end as redeemed
from allcoupons
left join redeemed_coupons
    on allcoupons.coupon_id = redeemed_coupons.coupon_id
left join redeemed_b2b_coupons
    on allcoupons.b2bcoupon_id = redeemed_b2b_coupons.b2bcoupon_id
left join ecommerce_coupon
    on allcoupons.coupon_id = ecommerce_coupon.coupon_id
left join ecommerce_couponpaymentversion
    on ecommerce_coupon.couponpayment_name = ecommerce_couponpaymentversion.couponpayment_name
left join ecommerce_company
    on ecommerce_couponpaymentversion.company_id = ecommerce_company.company_id
