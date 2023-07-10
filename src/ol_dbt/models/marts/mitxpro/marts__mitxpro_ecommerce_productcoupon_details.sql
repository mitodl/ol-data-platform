with commerce_coupon as (
    select *
    from {{ ref('int__mitxpro__ecommerce_coupon') }}
)

, ecommerce_productcouponassignment as (
    select *
    from {{ ref('int__mitxpro__ecommerce_productcouponassignment') }}
)

, ecommerce_productversion as (
    select *
    from {{ ref('int__mitxpro__ecommerce_productversion') }}
)

select
    commerce_coupon.coupon_id
    , ecommerce_productcouponassignment.product_id
    , commerce_coupon.coupon_code
    , commerce_coupon.couponpayment_name
    , commerce_coupon.coupon_created_on
    , max(ecommerce_productcouponassignment.productcouponassignment_created_on) as maxproductcouponassignment_created_on
    , ecommerce_productcouponassignment.productcouponassignment_is_redeemed
    , ecommerce_productcouponassignment.productcouponassignment_email
    , ecommerce_productversion.productversion_readable_id
from commerce_coupon
left join ecommerce_productcouponassignment
    on commerce_coupon.coupon_id = ecommerce_productcouponassignment.coupon_id
left join ecommerce_productversion
    on
        ecommerce_productversion.product_id = ecommerce_productcouponassignment.product_id
        and productversion_readable_id is not null
group by 1, 2, 3, 4, 5, 7, 8, 9
