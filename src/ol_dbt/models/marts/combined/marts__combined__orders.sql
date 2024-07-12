--- This model combines intermediate orders from different platforms

with bootcamps__ecommerce_order as (
    select * from {{ ref('int__bootcamps__ecommerce_order') }}
)

, mitxpro__ecommerce_allorders as (
    select * from {{ ref('int__mitxpro__ecommerce_allorders') }}
)

, mitxonline__ecommerce_order as (
    select * from {{ ref('int__mitxonline__ecommerce_order') }}
)

, mitxpro_lines as (
    select * from {{ ref('int__mitxpro__ecommerce_line') }}
)

, mitxpro__ecommerce_order as (
    select * from {{ ref('int__mitxpro__ecommerce_order') }}
)

, mitxpro__ecommerce_allcoupons as (
    select * from {{ ref('int__mitxpro__ecommerce_allcoupons') }}
)

, micromasters_orders as (
    select * from {{ ref('int__micromasters__orders') }}
)

, mitxpro_orders as (
    select
        mitxpro__ecommerce_allorders.line_id
        , mitxpro__ecommerce_allcoupons.coupon_code
        , mitxpro__ecommerce_allcoupons.coupon_name
        , mitxpro__ecommerce_allcoupons.coupon_type
        , mitxpro__ecommerce_allorders.coupon_redeemed_on
        , mitxpro__ecommerce_allorders.order_created_on
        , mitxpro__ecommerce_allorders.order_state
        , mitxpro__ecommerce_allorders.product_id
        , mitxpro__ecommerce_allorders.product_type
        , mitxpro_lines.product_price
        , mitxpro__ecommerce_allorders.user_email
        , mitxpro__ecommerce_allorders.courserun_id
        , mitxpro__ecommerce_allorders.courserun_readable_id
        , mitxpro__ecommerce_order.order_purchaser_user_id
        , mitxpro__ecommerce_order.receipt_authorization_code
        , mitxpro__ecommerce_order.receipt_bill_to_address_state
        , mitxpro__ecommerce_order.receipt_bill_to_address_country
        , mitxpro__ecommerce_order.receipt_transaction_uuid
        , mitxpro__ecommerce_order.receipt_transaction_type
        , mitxpro__ecommerce_order.receipt_payment_amount
        , mitxpro__ecommerce_order.receipt_payment_currency
        , mitxpro__ecommerce_order.receipt_payer_email
        , mitxpro__ecommerce_order.receipt_payment_card_number
        , mitxpro__ecommerce_order.receipt_payer_ip_address
        , mitxpro__ecommerce_order.receipt_payment_card_type
        , mitxpro__ecommerce_order.receipt_payer_name
        , mitxpro__ecommerce_order.receipt_payment_method
        , mitxpro__ecommerce_order.receipt_transaction_id
        , mitxpro__ecommerce_order.req_reference_number
        , mitxpro__ecommerce_order.order_tax_country_code
        , mitxpro__ecommerce_order.order_tax_rate
        , mitxpro__ecommerce_order.order_tax_rate_name
        , mitxpro__ecommerce_order.order_tax_amount
        , mitxpro__ecommerce_order.order_total_price_paid_plus_tax
        , mitxpro__ecommerce_allorders.coupon_id
        , mitxpro__ecommerce_allorders.order_id
        , mitxpro__ecommerce_order.order_total_price_paid
        , mitxpro__ecommerce_order.couponpaymentversion_discount_amount_text as discount
        , concat('xpro-b2c-production-', cast(mitxpro__ecommerce_allorders.order_id as varchar))
        as order_reference_number
    from mitxpro__ecommerce_allorders
    left join mitxpro__ecommerce_order
        on mitxpro__ecommerce_allorders.order_id = mitxpro__ecommerce_order.order_id
    left join mitxpro_lines
        on mitxpro__ecommerce_order.order_id = mitxpro_lines.order_id
    left join mitxpro__ecommerce_allcoupons
        on mitxpro__ecommerce_allorders.coupon_id = mitxpro__ecommerce_allcoupons.coupon_id
    where mitxpro__ecommerce_allorders.order_id is not null
)

, combined_orders as (
    select
        '{{ var("mitxonline") }}' as platform
        , order_id
        , line_id
        , courserun_id
        , courserun_readable_id
        , product_id
        , product_type
        , product_price as unit_price
        , user_email
        , user_id
        , discount_code as coupon_code
        , null as coupon_id
        , null as coupon_name
        , discount_redemption_type as coupon_type
        , discountredemption_timestamp as coupon_redeemed_on
        , discount_amount_text as discount
        , payment_authorization_code as receipt_authorization_code
        , payment_bill_to_address_state as receipt_bill_to_address_state
        , payment_bill_to_address_country as receipt_bill_to_address_country
        , payment_transaction_uuid as receipt_transaction_uuid
        , payment_transaction_type as receipt_transaction_type
        , payment_amount as receipt_payment_amount
        , payment_currency as receipt_payment_currency
        , payment_card_number as receipt_payment_card_number
        , payment_card_type as receipt_payment_card_type
        , payment_payer_name as receipt_payer_name
        , payment_payer_email as receipt_payer_email
        , payment_payer_ip_address as receipt_payer_ip_address
        , payment_method as receipt_payment_method
        , payment_transaction_id as receipt_transaction_id
        , payment_req_reference_number as req_reference_number
        , order_created_on
        , order_reference_number
        , order_state
        , null as order_tax_country_code
        , null as order_tax_rate
        , null as order_tax_rate_name
        , null as order_tax_amount
        , order_total_price_paid as order_total_price_paid_plus_tax
        , order_total_price_paid
    from mitxonline__ecommerce_order

    union all

    select
        '{{ var("mitxpro") }}' as platform
        , order_id
        , line_id
        , courserun_id
        , courserun_readable_id
        , product_id
        , product_type
        , product_price as unit_price
        , user_email
        , order_purchaser_user_id as user_id
        , coupon_code
        , coupon_id
        , coupon_name
        , coupon_type
        , coupon_redeemed_on
        , discount
        , receipt_authorization_code
        , receipt_bill_to_address_state
        , receipt_bill_to_address_country
        , receipt_transaction_uuid
        , receipt_transaction_type
        , receipt_payment_amount
        , receipt_payment_currency
        , receipt_payment_card_number
        , receipt_payment_card_type
        , receipt_payer_name
        , receipt_payer_email
        , receipt_payer_ip_address
        , receipt_payment_method
        , receipt_transaction_id
        , req_reference_number
        , order_created_on
        , order_reference_number
        , order_state
        , order_tax_country_code
        , order_tax_rate
        , order_tax_rate_name
        , order_tax_amount
        , order_total_price_paid_plus_tax
        , order_total_price_paid
    from mitxpro_orders

    union all

    select
        '{{ var("bootcamps") }}' as platform
        , order_id
        , line_id
        , courserun_id
        , courserun_readable_id
        , null as product_id
        , null as product_type
        , line_price as unit_price
        , user_email
        , order_purchaser_user_id as user_id
        , null as coupon_code
        , null as coupon_id
        , null as coupon_name
        , null as coupon_type
        , null as coupon_redeemed_on
        , null as discount
        , receipt_authorization_code
        , receipt_bill_to_address_state
        , receipt_bill_to_address_country
        , receipt_transaction_uuid
        , receipt_transaction_type
        , receipt_payment_amount
        , receipt_payment_currency
        , receipt_payment_card_number
        , receipt_payment_card_type
        , receipt_payer_name
        , receipt_payer_email
        , receipt_payer_ip_address
        , receipt_payment_method
        , receipt_transaction_id
        , receipt_reference_number as req_reference_number
        , order_created_on
        , order_reference_number
        , order_state
        , null as order_tax_country_code
        , null as order_tax_rate
        , null as order_tax_rate_name
        , null as order_tax_amount
        , order_total_price_paid as order_total_price_paid_plus_tax
        , order_total_price_paid
    from bootcamps__ecommerce_order

    union all

    select
        '{{ var("edxorg") }}' as platform
        , order_id
        , line_id
        , null as courserun_id
        , courserun_readable_id
        , null as product_id
        , null as product_type
        , line_price as unit_price
        , user_edxorg_email as user_email
        , user_edxorg_id as user_id
        , coupon_code
        , coupon_id
        , null as coupon_name
        , coupon_type
        , redeemedcoupon_created_on as coupon_redeemed_on
        , coupon_discount_amount_text as discount
        , receipt_authorization_code
        , receipt_bill_to_address_state
        , receipt_bill_to_address_country
        , receipt_transaction_uuid
        , receipt_transaction_type
        , receipt_payment_amount
        , receipt_payment_currency
        , receipt_payment_card_number
        , receipt_payment_card_type
        , receipt_payer_name
        , receipt_payer_email
        , receipt_payer_ip_address
        , receipt_payment_method
        , receipt_transaction_id
        , receipt_reference_number as req_reference_number
        , order_created_on
        , order_reference_number
        , order_state
        , null as order_tax_country_code
        , null as order_tax_rate
        , null as order_tax_rate_name
        , null as order_tax_amount
        , order_total_price_paid as order_total_price_paid_plus_tax
        , order_total_price_paid
    from micromasters_orders
    where courserun_platform = '{{ var("edxorg") }}'

)

select
    {{ generate_hash_id('cast(order_id as varchar)
        || cast(coalesce(line_id, 9) as varchar)
        || platform') }} as combined_orders_hash_id
    , platform
    , order_id
    , line_id
    , coupon_code
    , coupon_id
    , coupon_name
    , coupon_redeemed_on
    , coupon_type
    , courserun_id
    , courserun_readable_id
    , discount
    , order_created_on
    , order_reference_number
    , order_state
    , order_tax_amount
    , order_tax_country_code
    , order_tax_rate
    , order_tax_rate_name
    , order_total_price_paid_plus_tax
    , order_total_price_paid
    , product_id
    , product_type
    , receipt_authorization_code
    , receipt_bill_to_address_state
    , receipt_bill_to_address_country
    , receipt_payment_method
    , receipt_transaction_id
    , req_reference_number
    , unit_price
    , user_email
    , user_id
from combined_orders
