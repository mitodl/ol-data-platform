--- This model combines intermediate orders from different platforms
{{ config(materialized='view') }}

with bootcamps__ecommerce_order as (
    select * from {{ ref('int__bootcamps__ecommerce_order') }}
)

, mitxpro__ecommerce_allorders as (
    select * from {{ ref('int__mitxpro__ecommerce_allorders') }}
)

, mitxonline__ecommerce_order as (
    select * from {{ ref('int__mitxonline__ecommerce_order') }}
)

, combined_orders as (
    select
        '{{ var("mitxonline") }}' as platform
        , order_id
        , line_id
        , order_created_on
        , order_state
        , courserun_id
        , order_total_price_paid
        , product_id
        , product_type
        , user_email
        , user_id
    from mitxonline__ecommerce_order

    union all

    select
        '{{ var("mitxpro") }}' as platform
        , order_id
        , line_id
        , order_created_on
        , order_state
        , null as courserun_id
        , null as order_total_price_paid
        , product_id
        , product_type
        , user_email
        , null as user_id
    from mitxpro__ecommerce_allorders

    union all

    select
        '{{ var("bootcamps") }}' as platform
        , order_id
        , line_id
        , order_created_on
        , order_state
        , courserun_id
        , order_total_price_paid
        , null as product_id
        , null as product_type
        , null as user_email
        , order_purchaser_user_id as user_id
    from bootcamps__ecommerce_order

)

select * from combined_orders
