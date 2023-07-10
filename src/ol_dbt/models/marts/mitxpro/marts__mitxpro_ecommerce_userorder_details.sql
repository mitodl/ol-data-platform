with ecommerce_order as (
    select *
    from {{ ref('int__mitxpro__ecommerce_order') }}
)

, users as (
    select *
    from {{ ref('int__mitxpro__users') }}
)

, b2becommerce_b2border as (
    select *
    from {{ ref('int__mitxpro__b2becommerce_b2border') }}
)

, b2becommerce_b2breceipt as (
    select *
    from {{ ref('int__mitxpro__b2becommerce_b2breceipt') }}
)

, couponpaymentversion as (
    select *
    from {{ ref('int__mitxpro__ecommerce_couponpaymentversion') }}
)

select
    ecommerce_order.coupon_id
    , b2becommerce_b2border.product_id
    , ecommerce_order.order_id
    , ecommerce_order.order_state
    , users.user_email
    , b2becommerce_b2border.b2border_email
    , couponpaymentversion.couponpaymentversion_payment_transaction
    , couponpaymentversion.couponpaymentversion_coupon_type
    , couponpaymentversion.couponpaymentversion_discount_amount
    , couponpaymentversion.couponpayment_name
    , couponpaymentversion.couponpaymentversion_id
    , b2becommerce_b2border.b2border_contract_number
    , JSON_EXTRACT_SCALAR(b2becommerce_b2breceipt.b2breceipt_data, '$.req_reference_number') as req_reference_number
from ecommerce_order
inner join users
    on ecommerce_order.order_purchaser_user_id = users.user_id
inner join couponpaymentversion
    on ecommerce_order.couponpaymentversion_id = couponpaymentversion.couponpaymentversion_id
left join b2becommerce_b2border
    on b2becommerce_b2border.couponpaymentversion_id = couponpaymentversion.couponpaymentversion_id
left join b2becommerce_b2breceipt
    on b2becommerce_b2border.b2border_id = b2becommerce_b2breceipt.b2border_id
