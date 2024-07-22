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

, coupon as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_coupon') }}
)

, couponpaymentversion as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_couponpaymentversion') }}
)

, receipts as (
    select *
    from {{ ref('int__mitxpro__ecommerce_receipt') }}
    where receipt_transaction_status != 'ERROR'
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
    , coupon.coupon_code
    , couponpaymentversion.couponpaymentversion_payment_transaction
    , couponpaymentversion.couponpaymentversion_coupon_type
    , couponpaymentversion.couponpaymentversion_discount_source
    , couponpaymentversion.couponpaymentversion_discount_amount_text
    , couponpaymentversion.couponpaymentversion_discount_type
    , couponpaymentversion.couponpaymentversion_discount_amount
    , couponredemption.couponredemption_created_on
    , receipts.receipt_reference_number
    , receipts.receipt_authorization_code
    , receipts.receipt_payment_method
    , receipts.receipt_transaction_id
    , receipts.receipt_bill_to_address_state
    , receipts.receipt_bill_to_address_country
    , orders.order_tax_amount
    , orders.order_total_price_paid_plus_tax
from orders
left join couponredemption on orders.order_id = couponredemption.order_id
left join couponversion on couponredemption.couponversion_id = couponversion.couponversion_id
left join coupon on couponversion.coupon_id = coupon.coupon_id
left join couponpaymentversion on couponversion.couponpaymentversion_id = couponpaymentversion.couponpaymentversion_id
left join receipts on orders.order_id = receipts.order_id
