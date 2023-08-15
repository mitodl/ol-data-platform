with productcouponassignment as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_productcouponassignment') }}
)

, couponproduct as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_couponproduct') }}
)


select
    productcouponassignment.productcouponassignment_id
    , productcouponassignment.productcouponassignment_message_status_updated_on
    , productcouponassignment.productcouponassignment_is_redeemed
    , productcouponassignment.productcouponassignment_message_status
    , productcouponassignment.productcouponassignment_email
    , productcouponassignment.productcouponassignment_original_email
    , productcouponassignment.productcouponassignment_created_on
    , productcouponassignment.productcouponassignment_updated_on
    , couponproduct.coupon_id
    , couponproduct.product_id
    , couponproduct.programrun_id
from productcouponassignment
inner join couponproduct on productcouponassignment.couponproduct_id = couponproduct.couponproduct_id
