with orders as (
    select * from {{ ref('stg__micromasters__app__postgres__ecommerce_order') }}
)

, lines as (
    select * from {{ ref('stg__micromasters__app__postgres__ecommerce_line') }}
)

, coupons as (
    select * from {{ ref('stg__micromasters__app__postgres__ecommerce_coupon') }}
)

, redeemedcoupons as (
    select * from {{ ref('stg__micromasters__app__postgres__ecommerce_redeemedcoupon') }}
)

, receipts as (
    select
        *
        , row_number() over (partition by order_id order by receipt_created_on desc) as row_num
    from {{ ref('stg__micromasters__app__postgres__ecommerce_receipt') }}
    where receipt_transaction_status != 'ERROR'
)

select
    orders.order_id
    , orders.user_id
    , orders.order_state
    , orders.order_reference_number
    , orders.order_total_price_paid
    , orders.order_created_on
    , lines.line_price
    , lines.courserun_readable_id
    , lines.courserun_edxorg_readable_id
    , receipts.receipt_reference_number
    , receipts.receipt_transaction_id
    , receipts.receipt_payment_method
    , receipts.receipt_authorization_code
    , receipts.receipt_bill_to_address_state
    , receipts.receipt_bill_to_address_country
    , coupons.coupon_code
    , coupons.coupon_discount_amount_text
    , redeemedcoupons.redeemedcoupon_created_on
from orders
inner join lines on orders.order_id = lines.order_id
left join redeemedcoupons on orders.order_id = redeemedcoupons.order_id
left join coupons on redeemedcoupons.coupon_id = coupons.coupon_id
left join receipts on orders.order_id = receipts.order_id and receipts.row_num = 1
