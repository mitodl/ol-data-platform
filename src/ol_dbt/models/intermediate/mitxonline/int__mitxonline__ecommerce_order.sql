with lines as (
    select * from {{ ref('stg__mitxonline__app__postgres__ecommerce_line') }}
)

, contenttypes as (
    select * from {{ ref('stg__mitxonline__app__postgres__django_contenttype') }}
)

, versions as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__reversion_version') }}
    where
        contenttype_id in (
            select contenttype_id
            from
                contenttypes
            where contenttype_full_name = 'ecommerce_product'
        )
)

, orders as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__ecommerce_order') }}
)

, users as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__users_user') }}
)

, intermediate_products_view as (
    select * from {{ ref('int__mitxonline__ecommerce_product') }}
)

, discounts as (
    select * from {{ ref('stg__mitxonline__app__postgres__ecommerce_discount') }}
)

---- this table doesn't have constraint so apply additional logic here as there should be only one discount
---- that's actually applied to an order

, discountredemptions as (
    select
        *
        , row_number() over (partition by order_id order by discountredemption_timestamp desc) as row_num
    from {{ ref('stg__mitxonline__app__postgres__ecommerce_discountredemption') }}

)

---- small amounts of duplicated payments for the same order. For those, we pick the most recent one
, payments as (
    select
        *
        , row_number() over (partition by order_id order by transaction_created_on desc) as row_num
    from {{ ref('stg__mitxonline__app__postgres__ecommerce_transaction') }}
    where transaction_type = 'payment'
)

select
    orders.order_id
    , orders.order_state
    , orders.order_created_on
    , orders.order_reference_number
    , orders.order_total_price_paid
    , users.user_id
    , users.user_username
    , users.user_full_name
    , users.user_email
    , lines.line_id
    , lines.product_version_id
    , intermediate_products_view.product_price
    , intermediate_products_view.product_type
    , intermediate_products_view.product_id
    , intermediate_products_view.courserun_id
    , intermediate_products_view.programrun_id
    , discounts.discount_code
    , discounts.discount_amount_text
    , discountredemptions.discountredemption_timestamp
    , payments.transaction_authorization_code as payment_authorization_code
    , payments.transaction_payment_method as payment_method
    , payments.transaction_readable_identifier as payment_transaction_id
    , payments.transaction_bill_to_address_state as payment_bill_to_address_state
    , payments.transaction_bill_to_address_country as payment_bill_to_address_country
    , json_extract_scalar(payments.transaction_data, '$.req_reference_number') as req_reference_number
from lines
inner join orders on lines.order_id = orders.order_id
inner join users on orders.order_purchaser_user_id = users.user_id
inner join versions on lines.product_version_id = versions.version_id
inner join intermediate_products_view on versions.version_object_id = intermediate_products_view.product_id
left join payments
    on orders.order_id = payments.order_id and payments.row_num = 1
left join discountredemptions
    on orders.order_id = discountredemptions.order_id and discountredemptions.row_num = 1
left join discounts on discountredemptions.discount_id = discounts.discount_id
