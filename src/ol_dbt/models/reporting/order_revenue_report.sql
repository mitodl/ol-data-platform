-- Order lines with what was bought, on which day, and what was paid, and nothing that
-- identifies the learner or payer. orders_with_product_report carries those columns for
-- the people who look up a specific order; this model is for everyone else.
-- It has no order or coupon key and no time finer than a day, because other datasets
-- put those next to learner emails (combined_orders_hash_id, order_reference_number and
-- order_id in the enrollment reports), and a join on any of them would undo the point.
with orders as (
    select * from {{ ref('marts__combined__orders') }}
)

, products as (
    select * from {{ ref('marts__combined__products') }}
)

-- marts__combined__products has no row for some xPro programs sold without a run
-- (e.g. program-v1:xPRO+MLx), so their order lines take the title from dim_program.
, programs as (
    select
        program_readable_id
        , program_title
        , case platform_code
            when 'mitxpro' then '{{ var("mitxpro") }}'
            when 'mitxonline' then '{{ var("mitxonline") }}'
        end as platform
    from {{ ref('dim_program') }}
    where program_readable_id is not null
)

select
    orders.platform
    , orders.order_state
    , orders.order_type
    , cast(substring(orders.order_created_on, 1, 10) as date) as order_created_date
    , orders.order_payment_processor
    , orders.product_id
    , orders.product_readable_id
    , orders.product_type
    , coalesce(products.product_name, programs.program_title) as product_name
    , orders.courserun_readable_id
    , orders.unit_price
    , orders.discount
    , orders.discount_amount
    , orders.order_total_price_paid
    , orders.order_tax_amount
    , orders.order_tax_rate
    , orders.order_tax_rate_name
    , orders.order_tax_country_code
    , orders.order_total_price_paid_plus_tax
    , orders.coupon_name
    , orders.coupon_type
    , orders.b2border_contract_number
    , orders.receipt_payment_amount
    , orders.receipt_payment_currency
    , orders.receipt_payment_method
    , orders.receipt_payment_card_type
    , orders.receipt_payment_transaction_type
    , orders.receipt_bill_to_address_country
    , orders.receipt_bill_to_address_state
from orders
left join products
    on
        orders.product_readable_id = products.product_readable_id
        and orders.platform = products.platform
left join programs
    on
        orders.product_type = 'program'
        and orders.product_readable_id = programs.program_readable_id
        and orders.platform = programs.platform
