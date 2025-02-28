with allcoupons as (
    select *
    from {{ ref('int__mitxpro__ecommerce_allcoupons') }}
)

, allorders as (
    select *
    from {{ ref('int__mitxpro__ecommerce_allorders') }}
)

, redeemed_coupons as (
    select
        coupon_id
        , b2bcoupon_id
    from allorders
    where redeemed = true
    group by
        coupon_id
        , b2bcoupon_id
)

select
    allcoupons.coupon_code
    , allcoupons.coupon_name
    , allcoupons.coupon_created_on
    , allcoupons.payment_transaction
    , allcoupons.discount_amount
    , allcoupons.coupon_type
    , allcoupons.discount_source
    , allcoupons.activated_on
    , allcoupons.expires_on
    , allcoupons.coupon_source_table
    , allcoupons.b2bcoupon_id
    , allcoupons.coupon_id
    , case
        when
            redeemed_coupons.coupon_id is not null
            or redeemed_coupons.b2bcoupon_id is not null
            then true
        when
            redeemed_coupons.coupon_id is null
            and redeemed_coupons.b2bcoupon_id is null
            then false
    end as redeemed
from allcoupons
left join redeemed_coupons
    on
        allcoupons.coupon_id = redeemed_coupons.coupon_id
        or allcoupons.b2bcoupon_id = redeemed_coupons.b2bcoupon_id
