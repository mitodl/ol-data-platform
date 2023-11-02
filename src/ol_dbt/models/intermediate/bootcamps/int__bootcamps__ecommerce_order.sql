with orders as (
    select *
    from {{ ref('stg__bootcamps__app__postgres__ecommerce_order') }}
)

, lines as (
    select *
    from {{ ref('stg__bootcamps__app__postgres__ecommerce_line') }}
)

, receipts as (
    select * from {{ ref('stg__bootcamps__app__postgres__ecommerce_receipt') }}
)

select
    orders.order_id
    , orders.order_reference_number
    , orders.order_state
    , orders.order_purchaser_user_id
    , orders.application_id
    , orders.order_payment_type
    , orders.order_total_price_paid
    , orders.order_created_on
    , orders.order_updated_on
    , lines.line_id
    , lines.line_description
    , lines.courserun_id
    , lines.line_price
    , receipts.receipt_transaction_id
    , receipts.receipt_reference_number
    , receipts.receipt_payment_method
    , receipts.receipt_authorization_code
    , receipts.receipt_bill_to_address_state
    , receipts.receipt_bill_to_address_country
from lines
inner join orders on lines.order_id = orders.order_id
left join receipts on orders.order_id = receipts.order_id
