-- Order lines with what was bought, on which day, and what was paid, and nothing that
-- identifies the learner or payer. orders_with_product_report carries those columns for
-- the people who look up a specific order; this model is for everyone else.
-- It has no order or coupon key and no time finer than a day, because other datasets
-- put those next to learner emails (combined_orders_hash_id, order_reference_number and
-- order_id in the enrollment reports), and a join on any of them would undo the point.
-- The mart's paid, tax and payment amounts are the whole order's, repeated on each line.
-- With no order key nobody downstream could undo that, so each line carries its even
-- share and a sum over lines adds up to the orders' totals.
with orders as (
    select
        *
        , count(*) over (partition by platform, order_id) as order_line_count
    from {{ ref('marts__combined__orders') }}
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
    -- xPro's mart value is the line's price minus the whole order's total, so it is
    -- worked out again from the line's share. Other platforms' are already per line.
    , case
        when orders.platform = '{{ var("mitxpro") }}'
            then orders.unit_price - orders.order_total_price_paid / orders.order_line_count
        else orders.discount_amount
    end as discount_amount
    , orders.order_line_count
    , orders.order_total_price_paid / orders.order_line_count as line_price_paid
    , orders.order_tax_amount / orders.order_line_count as line_tax_amount
    , orders.order_total_price_paid_plus_tax / orders.order_line_count as line_price_paid_plus_tax
    , orders.receipt_payment_amount / orders.order_line_count as line_payment_amount
    , orders.order_tax_rate
    , orders.order_tax_rate_name
    , orders.order_tax_country_code
    , orders.coupon_name
    , orders.coupon_type
    , orders.b2border_contract_number
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
