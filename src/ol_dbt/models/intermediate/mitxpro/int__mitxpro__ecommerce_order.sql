with orders as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_order') }}
)

, couponredemption as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_couponredemption') }}
)

, couponversion as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_couponversion') }}
)

, couponpaymentversion as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_couponpaymentversion') }}
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
    , orders.order_tax_country_code
    , orders.order_tax_rate
    , orders.order_tax_rate_name
    , (orders.order_total_price_paid * (orders.order_tax_rate/100)) as order_tax_amount
    , (orders.order_total_price_paid * (orders.order_tax_rate/100)) 
        + orders.order_total_price_paid as order_total_price_paid_plus_tax
from orders
left join couponredemption on orders.order_id = couponredemption.order_id
left join couponversion on couponredemption.couponversion_id = couponversion.couponversion_id
left join couponpaymentversion on couponversion.couponpaymentversion_id = couponpaymentversion.couponpaymentversion_id