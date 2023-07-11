with commerce_coupon as (
    select *
    from {{ ref('int__mitxpro__ecommerce_coupon') }}
)

, ecommerce_productcouponassignment as (
    select *
    from {{ ref('int__mitxpro__ecommerce_productcouponassignment') }}
)

, productversion as (
    select *
    from {{ ref('int__mitxpro__ecommerce_productversion') }}
)

, ecommerce_productversion as (
    select
        product_id
        , productversion_readable_id
    from productversion
    where productversion_readable_id is not null
)




select
    commerce_coupon.coupon_id
    , ecommerce_productcouponassignment.product_id
    , commerce_coupon.coupon_code
    , commerce_coupon.couponpayment_name
    , commerce_coupon.coupon_created_on
    , ecommerce_productcouponassignment.productcouponassignment_is_redeemed
    , ecommerce_productcouponassignment.productcouponassignment_email
    , ecommerce_productversion.productversion_readable_id
    , max(ecommerce_productcouponassignment.productcouponassignment_created_on)
        as maxproductcouponassignment_created_on
from commerce_coupon
left join ecommerce_productcouponassignment
    on commerce_coupon.coupon_id = ecommerce_productcouponassignment.coupon_id
left join ecommerce_productversion
    on ecommerce_productversion.product_id = ecommerce_productcouponassignment.product_id
group by
    commerce_coupon.coupon_id
    , ecommerce_productcouponassignment.product_id
    , commerce_coupon.coupon_code
    , commerce_coupon.couponpayment_name
    , commerce_coupon.coupon_created_on
    , ecommerce_productcouponassignment.productcouponassignment_is_redeemed
    , ecommerce_productcouponassignment.productcouponassignment_email
    , ecommerce_productversion.productversion_readable_id
