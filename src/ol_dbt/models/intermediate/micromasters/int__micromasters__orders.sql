with
    orders as (select * from {{ ref("stg__micromasters__app__postgres__ecommerce_order") }}),
    lines as (select * from {{ ref("stg__micromasters__app__postgres__ecommerce_line") }}),
    coupons as (select * from {{ ref("stg__micromasters__app__postgres__ecommerce_coupon") }}),
    redeemedcoupons as (select * from {{ ref("stg__micromasters__app__postgres__ecommerce_redeemedcoupon") }}),
    receipts as (
        select *, row_number() over (partition by order_id order by receipt_created_on desc) as row_num
        from {{ ref("stg__micromasters__app__postgres__ecommerce_receipt") }}
        where receipt_transaction_status != 'ERROR'
    ),
    users as (select * from {{ ref("__micromasters__users") }}),
    courseruns as (select * from {{ ref("stg__micromasters__app__postgres__courses_courserun") }}),
    edxorg_users as (select * from {{ ref("int__edxorg__mitx_users") }})

select
    orders.order_id,
    orders.user_id,
    users.user_username,
    users.user_mitxonline_username,
    users.user_edxorg_username,
    edxorg_users.user_id as user_edxorg_id,
    edxorg_users.user_email as user_edxorg_email,
    users.user_full_name,
    users.user_email,
    orders.order_state,
    orders.order_reference_number,
    orders.order_total_price_paid,
    orders.order_created_on,
    lines.line_id,
    lines.line_price,
    lines.courserun_readable_id,
    lines.courserun_edxorg_readable_id,
    courseruns.courserun_platform,
    receipts.receipt_reference_number,
    receipts.receipt_transaction_uuid,
    receipts.receipt_transaction_id,
    receipts.receipt_transaction_type,
    receipts.receipt_payment_method,
    receipts.receipt_authorization_code,
    receipts.receipt_bill_to_address_state,
    receipts.receipt_bill_to_address_country,
    receipts.receipt_payer_name,
    receipts.receipt_payer_email,
    receipts.receipt_payer_ip_address,
    receipts.receipt_payment_amount,
    receipts.receipt_payment_currency,
    receipts.receipt_payment_card_number,
    receipts.receipt_payment_card_type,
    receipts.receipt_payment_timestamp,
    coupons.coupon_id,
    coupons.coupon_type,
    coupons.coupon_code,
    coupons.coupon_discount_amount_text,
    case
        when orders.order_state in ('fulfilled', 'refunded') then redeemedcoupons.redeemedcoupon_created_on
    end as redeemedcoupon_created_on,
    case
        when coupons.coupon_amount_type = 'percent-discount'
        then cast(lines.line_price * coupons.coupon_amount as decimal(38, 2))
        else cast(coupons.coupon_amount as decimal(38, 2))
    end as coupon_amount
from orders
inner join users on orders.user_id = users.user_id
inner join lines on orders.order_id = lines.order_id
left join courseruns on lines.courserun_readable_id = courseruns.courserun_readable_id
left join redeemedcoupons on orders.order_id = redeemedcoupons.order_id
left join coupons on redeemedcoupons.coupon_id = coupons.coupon_id
left join receipts on orders.order_id = receipts.order_id and receipts.row_num = 1
left join edxorg_users on users.user_edxorg_username = edxorg_users.user_username
