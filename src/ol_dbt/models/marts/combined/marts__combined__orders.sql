--- This model combines intermediate orders from different platforms

with bootcamps__ecommerce_order as (
    select * from {{ ref('int__bootcamps__ecommerce_order') }}
)

, bootcamps__receipt as (
    select * from {{ ref('int__bootcamps__ecommerce_receipt') }}
)

, mitxpro__ecommerce_allorders as (
    select * from {{ ref('int__mitxpro__ecommerce_allorders') }}
)

, mitxonline__ecommerce_order as (
    select * from {{ ref('int__mitxonline__ecommerce_order') }}
)

, mitxonline__transaction as (
    select * from {{ ref('int__mitxonline__ecommerce_transaction') }}
)

, mitxpro__lines as (
    select * from {{ ref('int__mitxpro__ecommerce_line') }}
)

, mitxpro__programruns as (
    select * from {{ ref('int__mitxpro__program_runs') }}
)

, mitxpro__receipts as (
    select *
    from {{ ref('int__mitxpro__ecommerce_receipt') }}
    where receipt_transaction_status != 'ERROR'
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

, mitxpro__users as (
    select * from {{ ref('int__mitxpro__users') }}
)

, mitxonline_orders as (
    select
        mitxonline__ecommerce_order.order_id
        , mitxonline__ecommerce_order.line_id
        , mitxonline__ecommerce_order.courserun_id
        , mitxonline__ecommerce_order.courserun_readable_id
        , mitxonline__ecommerce_order.product_id
        , mitxonline__ecommerce_order.product_type
        , mitxonline__ecommerce_order.product_price
        , mitxonline__ecommerce_order.user_email
        , mitxonline__ecommerce_order.user_id
        , mitxonline__ecommerce_order.discount_code
        , mitxonline__ecommerce_order.discount_redemption_type
        , mitxonline__ecommerce_order.discountredemption_timestamp
        , mitxonline__transaction.transaction_bill_to_address_state
        , mitxonline__transaction.transaction_bill_to_address_country
        , mitxonline__transaction.transaction_uuid
        , mitxonline__transaction.transaction_req_type
        , mitxonline__transaction.transaction_payment_amount
        , mitxonline__transaction.transaction_payment_currency
        , mitxonline__transaction.transaction_payment_card_number
        , mitxonline__transaction.transaction_payment_card_type
        , mitxonline__transaction.transaction_payer_name
        , mitxonline__transaction.transaction_payer_email
        , mitxonline__transaction.transaction_payer_ip_address
        , mitxonline__transaction.transaction_payment_method
        , mitxonline__transaction.transaction_timestamp as transaction_payment_timestamp
        , mitxonline__transaction.transaction_reference_number
        , mitxonline__ecommerce_order.order_created_on
        , mitxonline__ecommerce_order.order_reference_number
        , mitxonline__ecommerce_order.order_state
        , mitxonline__ecommerce_order.order_total_price_paid
        , mitxonline__ecommerce_order.discount_amount_text
        ----In MITx Online, there are two transactions for an refunded order.
        ----Here we picked the refund transaction ID and auth code until we change the unique key
        , coalesce(
            mitxonline__refund.transaction_readable_identifier
            , mitxonline__transaction.transaction_readable_identifier
        ) as transaction_readable_identifier
        , coalesce(
            mitxonline__refund.transaction_authorization_code
            , mitxonline__transaction.transaction_authorization_code
        ) as transaction_authorization_code
    from mitxonline__ecommerce_order
    left join mitxonline__transaction
        on mitxonline__ecommerce_order.transaction_id = mitxonline__transaction.transaction_id
    left join mitxonline__transaction as mitxonline__refund
        on
            mitxonline__ecommerce_order.order_id = mitxonline__refund.order_id
            and mitxonline__refund.transaction_type = 'refund'
)

, bootcamps_orders as (
    select
        bootcamps__ecommerce_order.order_id
        , bootcamps__ecommerce_order.line_id
        , bootcamps__ecommerce_order.courserun_id
        , bootcamps__ecommerce_order.courserun_readable_id
        , bootcamps__ecommerce_order.line_price
        , bootcamps__ecommerce_order.user_email
        , bootcamps__ecommerce_order.order_purchaser_user_id
        , bootcamps__receipt.receipt_authorization_code
        , bootcamps__receipt.receipt_bill_to_address_state
        , bootcamps__receipt.receipt_bill_to_address_country
        , bootcamps__receipt.receipt_transaction_uuid
        , bootcamps__receipt.receipt_transaction_type
        , bootcamps__receipt.receipt_payment_amount
        , bootcamps__receipt.receipt_payment_currency
        , bootcamps__receipt.receipt_payment_card_number
        , bootcamps__receipt.receipt_payment_card_type
        , bootcamps__receipt.receipt_payer_name
        , bootcamps__receipt.receipt_payer_email
        , bootcamps__receipt.receipt_payer_ip_address
        , bootcamps__receipt.receipt_payment_method
        , bootcamps__receipt.receipt_transaction_id
        , bootcamps__receipt.receipt_reference_number
        , bootcamps__receipt.receipt_payment_timestamp
        , bootcamps__ecommerce_order.order_created_on
        , bootcamps__ecommerce_order.order_reference_number
        , bootcamps__ecommerce_order.order_state
        , bootcamps__ecommerce_order.order_total_price_paid
    from bootcamps__ecommerce_order
    left join bootcamps__receipt
        on bootcamps__ecommerce_order.order_id = bootcamps__receipt.order_id
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
        , mitxpro__lines.product_price
        , mitxpro__ecommerce_allorders.user_email
        , mitxpro__ecommerce_allorders.courserun_id
        , mitxpro__ecommerce_allorders.courserun_readable_id
        , mitxpro__ecommerce_order.order_purchaser_user_id
        , mitxpro__receipts.receipt_authorization_code
        , mitxpro__receipts.receipt_bill_to_address_state
        , mitxpro__receipts.receipt_bill_to_address_country
        , mitxpro__receipts.receipt_transaction_uuid
        , mitxpro__receipts.receipt_transaction_type
        , mitxpro__receipts.receipt_payment_amount
        , mitxpro__receipts.receipt_payment_currency
        , mitxpro__receipts.receipt_payer_email
        , mitxpro__receipts.receipt_payment_card_number
        , mitxpro__receipts.receipt_payer_ip_address
        , mitxpro__receipts.receipt_payment_card_type
        , mitxpro__receipts.receipt_payer_name
        , mitxpro__receipts.receipt_payment_method
        , mitxpro__receipts.receipt_transaction_id
        , mitxpro__receipts.receipt_payment_timestamp
        , mitxpro__ecommerce_order.receipt_reference_number
        , mitxpro__ecommerce_order.order_tax_country_code
        , mitxpro__ecommerce_order.order_tax_rate
        , mitxpro__ecommerce_order.order_tax_rate_name
        , mitxpro__ecommerce_order.order_tax_amount
        , mitxpro__ecommerce_order.order_total_price_paid_plus_tax
        , mitxpro__ecommerce_allorders.coupon_id
        , mitxpro__ecommerce_allorders.b2bcoupon_id
        , mitxpro__ecommerce_allorders.b2border_contract_number
        , mitxpro__ecommerce_allorders.order_id
        , mitxpro__users.user_email as redeemed_emails
        , mitxpro__ecommerce_order.couponpaymentversion_payment_transaction
        , mitxpro__ecommerce_order.order_total_price_paid
        , mitxpro__ecommerce_order.couponpaymentversion_discount_amount_text as discount
        , concat('xpro-b2c-production-', cast(mitxpro__ecommerce_allorders.order_id as varchar))
            as order_reference_number
        , coalesce(
            mitxpro__ecommerce_allorders.courserun_readable_id
            , mitxpro__programruns.programrun_readable_id
            , mitxpro__ecommerce_allorders.program_readable_id
        ) as product_readable_id
    from mitxpro__ecommerce_allorders
    left join mitxpro__ecommerce_order
        on mitxpro__ecommerce_allorders.order_id = mitxpro__ecommerce_order.order_id
    left join mitxpro__receipts
        on mitxpro__ecommerce_order.order_id = mitxpro__receipts.order_id
    left join mitxpro__lines
        on mitxpro__ecommerce_allorders.line_id = mitxpro__lines.line_id
    left join mitxpro__ecommerce_allcoupons
        on mitxpro__ecommerce_allorders.coupon_id = mitxpro__ecommerce_allcoupons.coupon_id
    left join mitxpro__programruns
        on mitxpro__lines.programrun_id = mitxpro__programruns.programrun_id
    left join mitxpro__users
        on mitxpro__ecommerce_order.order_purchaser_user_id = mitxpro__users.user_id
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
        , courserun_readable_id as product_readable_id
        , product_type
        , product_price as unit_price
        , user_email
        , user_id
        , discount_code as coupon_code
        , null as coupon_id
        , null as b2bcoupon_id
        , null as b2border_contract_number
        , null as coupon_name
        , discount_redemption_type as coupon_type
        , discountredemption_timestamp as coupon_redeemed_on
        , transaction_authorization_code as receipt_authorization_code
        , transaction_bill_to_address_state as receipt_bill_to_address_state
        , transaction_bill_to_address_country as receipt_bill_to_address_country
        , transaction_uuid as receipt_payment_transaction_uuid
        , transaction_req_type as receipt_payment_transaction_type
        , transaction_payment_amount as receipt_payment_amount
        , transaction_payment_currency as receipt_payment_currency
        , transaction_payment_card_number as receipt_payment_card_number
        , transaction_payment_card_type as receipt_payment_card_type
        , transaction_payer_name as receipt_payer_name
        , transaction_payer_email as receipt_payer_email
        , transaction_payer_ip_address as receipt_payer_ip_address
        , transaction_payment_method as receipt_payment_method
        , transaction_payment_timestamp as receipt_payment_timestamp
        , transaction_readable_identifier as receipt_transaction_id
        , transaction_reference_number as req_reference_number
        , order_created_on
        , order_reference_number
        , order_state
        , null as redeemed_emails
        , null as order_tax_country_code
        , null as order_tax_rate
        , null as order_tax_rate_name
        , null as order_tax_amount
        , null as couponpaymentversion_payment_transaction
        , order_total_price_paid as order_total_price_paid_plus_tax
        , order_total_price_paid
        , case
            when discount_amount_text like '%Fixed%'
                then cast(
                    product_price - cast(
                        substring(discount_amount_text, 14)
                        as decimal(38, 2)
                    ) as varchar
                )
            else discount_amount_text
        end as discount
    from mitxonline_orders

    union all

    select
        '{{ var("mitxpro") }}' as platform
        , order_id
        , line_id
        , courserun_id
        , courserun_readable_id
        , product_id
        , product_readable_id
        , product_type
        , product_price as unit_price
        , user_email
        , order_purchaser_user_id as user_id
        , coupon_code
        , coupon_id
        , b2bcoupon_id
        , b2border_contract_number
        , coupon_name
        , coupon_type
        , coupon_redeemed_on
        , receipt_authorization_code
        , receipt_bill_to_address_state
        , receipt_bill_to_address_country
        , receipt_transaction_uuid as receipt_payment_transaction_uuid
        , receipt_transaction_type as receipt_payment_transaction_type
        , receipt_payment_amount
        , receipt_payment_currency
        , receipt_payment_card_number
        , receipt_payment_card_type
        , receipt_payer_name
        , receipt_payer_email
        , receipt_payer_ip_address
        , receipt_payment_method
        , receipt_payment_timestamp
        , receipt_transaction_id
        , receipt_reference_number as req_reference_number
        , order_created_on
        , order_reference_number
        , order_state
        , redeemed_emails
        , order_tax_country_code
        , order_tax_rate
        , order_tax_rate_name
        , order_tax_amount
        , couponpaymentversion_payment_transaction
        , order_total_price_paid_plus_tax
        , order_total_price_paid
        , discount
    from mitxpro_orders

    union all

    select
        '{{ var("bootcamps") }}' as platform
        , order_id
        , line_id
        , courserun_id
        , courserun_readable_id
        , null as product_id
        , null as product_readable_id
        , null as product_type
        , line_price as unit_price
        , user_email
        , order_purchaser_user_id as user_id
        , null as coupon_code
        , null as coupon_id
        , null as b2bcoupon_id
        , null as b2border_contract_number
        , null as coupon_name
        , null as coupon_type
        , null as coupon_redeemed_on
        , receipt_authorization_code
        , receipt_bill_to_address_state
        , receipt_bill_to_address_country
        , receipt_transaction_uuid as receipt_payment_transaction_uuid
        , receipt_transaction_type as receipt_payment_transaction_type
        , receipt_payment_amount
        , receipt_payment_currency
        , receipt_payment_card_number
        , receipt_payment_card_type
        , receipt_payer_name
        , receipt_payer_email
        , receipt_payer_ip_address
        , receipt_payment_method
        , receipt_payment_timestamp
        , receipt_transaction_id
        , receipt_reference_number as req_reference_number
        , order_created_on
        , order_reference_number
        , order_state
        , null as redeemed_emails
        , null as order_tax_country_code
        , null as order_tax_rate
        , null as order_tax_rate_name
        , null as order_tax_amount
        , null as couponpaymentversion_payment_transaction
        , order_total_price_paid as order_total_price_paid_plus_tax
        , order_total_price_paid
        , null as discount
    from bootcamps_orders

    union all

    select
        '{{ var("edxorg") }}' as platform
        , order_id
        , line_id
        , null as courserun_id
        , courserun_readable_id
        , null as product_id
        , null as product_readable_id
        , null as product_type
        , line_price as unit_price
        , user_edxorg_email as user_email
        , user_edxorg_id as user_id
        , coupon_code
        , coupon_id
        , null as b2bcoupon_id
        , null as b2border_contract_number
        , null as coupon_name
        , coupon_type
        , redeemedcoupon_created_on as coupon_redeemed_on
        , receipt_authorization_code
        , receipt_bill_to_address_state
        , receipt_bill_to_address_country
        , receipt_transaction_uuid as receipt_payment_transaction_uuid
        , receipt_transaction_type as receipt_payment_transaction_type
        , receipt_payment_amount
        , receipt_payment_currency
        , receipt_payment_card_number
        , receipt_payment_card_type
        , receipt_payer_name
        , receipt_payer_email
        , receipt_payer_ip_address
        , receipt_payment_method
        , receipt_payment_timestamp
        , receipt_transaction_id
        , receipt_reference_number as req_reference_number
        , order_created_on
        , order_reference_number
        , order_state
        , null as redeemed_emails
        , null as order_tax_country_code
        , null as order_tax_rate
        , null as order_tax_rate_name
        , null as order_tax_amount
        , null as couponpaymentversion_payment_transaction
        , order_total_price_paid as order_total_price_paid_plus_tax
        , order_total_price_paid
        , case
            when coupon_discount_amount_text like '%Fixed%'
                then cast(
                    line_price - cast(
                        substring(coupon_discount_amount_text, 14)
                        as decimal(38, 2)
                    ) as varchar
                )
            else coupon_discount_amount_text
        end as discount
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
    , b2bcoupon_id
    , b2border_contract_number
    , coupon_name
    , coupon_redeemed_on
    , coupon_type
    , couponpaymentversion_payment_transaction
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
    , product_readable_id
    , product_type
    , receipt_authorization_code
    , receipt_bill_to_address_state
    , receipt_bill_to_address_country
    , receipt_payment_amount
    , receipt_payment_currency
    , receipt_payment_card_number
    , receipt_payment_card_type
    , receipt_payment_method
    , receipt_payment_timestamp
    , receipt_payment_transaction_type
    , receipt_payment_transaction_uuid
    , receipt_payer_name
    , receipt_payer_email
    , receipt_payer_ip_address
    , receipt_transaction_id
    , redeemed_emails
    , req_reference_number
    , unit_price
    , user_email
    , user_id
    , {{ generate_hash_id('cast(user_id as varchar) || platform') }} as user_hashed_id
from combined_orders
