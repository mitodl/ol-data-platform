with orders as (
    select *
    from dev.main_staging.stg__mitxpro__app__postgres__ecommerce_order
)

, couponredemption as (
    select *
    from dev.main_staging.stg__mitxpro__app__postgres__ecommerce_couponredemption
)

, couponversion as (
    select *
    from dev.main_staging.stg__mitxpro__app__postgres__ecommerce_couponversion
)

select
    orders.order_id
    , orders.order_state
    , orders.order_purchaser_user_id
    , orders.order_total_price_paid
    , orders.order_created_on
    , orders.order_updated_on
    , couponversion.coupon_id
    , couponversion.couponpaymentversion_id
from orders
left join couponredemption on couponredemption.order_id = orders.order_id
left join couponversion on couponversion.couponversion_id = couponredemption.couponversion_id
