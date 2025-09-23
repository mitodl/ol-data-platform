with
    orders as (select * from {{ ref("stg__bootcamps__app__postgres__ecommerce_order") }}),
    lines as (select * from {{ ref("stg__bootcamps__app__postgres__ecommerce_line") }}),
    receipts as (select * from {{ ref("int__bootcamps__ecommerce_receipt") }}),
    users as (select * from {{ ref("int__bootcamps__users") }}),
    runs as (select * from {{ ref("stg__bootcamps__app__postgres__courses_courserun") }})

select
    orders.order_id,
    orders.order_reference_number,
    orders.order_state,
    orders.order_purchaser_user_id,
    orders.application_id,
    orders.order_payment_type,
    orders.order_total_price_paid,
    orders.order_created_on,
    orders.order_updated_on,
    lines.line_id,
    lines.line_description,
    lines.courserun_id,
    runs.courserun_readable_id,
    lines.line_price,
    receipts.receipt_transaction_id,
    receipts.receipt_reference_number,
    receipts.receipt_payment_method,
    receipts.receipt_authorization_code,
    receipts.receipt_bill_to_address_state,
    receipts.receipt_bill_to_address_country,
    users.user_username,
    users.user_email,
    users.user_full_name
from lines
inner join orders on lines.order_id = orders.order_id
inner join users on orders.order_purchaser_user_id = users.user_id
inner join runs on lines.courserun_id = runs.courserun_id
left join receipts on orders.order_id = receipts.order_id
