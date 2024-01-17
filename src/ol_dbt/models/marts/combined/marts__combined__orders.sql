--- This model combines intermediate orders from different platforms
---{{ config(materialized='view') }}

with bootcamps__ecommerce_order as (
    select * from {{ ref('int__bootcamps__ecommerce_order') }}
)

, mitxpro__ecommerce_allorders as (
    select * from {{ ref('int__mitxpro__ecommerce_allorders') }}
)

, mitxpro__course_runs as (
    select * from {{ ref('int__mitxpro__course_runs') }}
)

, mitxonline__ecommerce_order as (
    select * from {{ ref('int__mitxonline__ecommerce_order') }}
)

, bootcamps__users as (
    select * from {{ ref('int__bootcamps__users') }}
)

, mitxpro__b2becommerce_b2border as (
    select * from {{ ref('int__mitxpro__b2becommerce_b2border') }}
)

, mitxpro__ecommerce_order as (
    select * from {{ ref('int__mitxpro__ecommerce_order') }}
)

, mitxpro__ecommerce_allcoupons as (
    select * from {{ ref('int__mitxpro__ecommerce_allcoupons') }}
)

, mitxpro_orders as (
    select
        mitxpro__ecommerce_allorders.line_id
        , mitxpro__ecommerce_allcoupons.coupon_name
        , mitxpro__ecommerce_allorders.order_created_on
        , mitxpro__ecommerce_allorders.order_state
        , mitxpro__ecommerce_allorders.product_id
        , mitxpro__ecommerce_allorders.product_type
        , mitxpro__ecommerce_allorders.user_email
        , mitxpro__course_runs.courserun_id
        , mitxpro__ecommerce_order.order_purchaser_user_id
        , mitxpro__ecommerce_order.order_total_price_paid
        , mitxpro__b2becommerce_b2border.b2border_total_price
        , mitxpro__ecommerce_allorders.receipt_authorization_code
        , mitxpro__ecommerce_allorders.receipt_transaction_id
        , coalesce(mitxpro__ecommerce_allorders.coupon_id, mitxpro__ecommerce_allorders.b2bcoupon_id) as coupon_id
        , coalesce(mitxpro__ecommerce_allorders.order_id, mitxpro__ecommerce_allorders.b2border_id) as order_id
        , case
            when mitxpro__ecommerce_allorders.order_id is null
                then 'Y'
        end as b2b_only_indicator
    from mitxpro__ecommerce_allorders
    left join mitxpro__course_runs
        on mitxpro__ecommerce_allorders.courserun_readable_id = mitxpro__course_runs.courserun_readable_id
    left join mitxpro__ecommerce_order
        on mitxpro__ecommerce_allorders.order_id = mitxpro__ecommerce_order.order_id
    left join mitxpro__b2becommerce_b2border
        on mitxpro__ecommerce_allorders.b2border_id = mitxpro__b2becommerce_b2border.b2border_id
    left join mitxpro__ecommerce_allcoupons
        on mitxpro__ecommerce_allorders.coupon_id = mitxpro__ecommerce_allcoupons.coupon_id
)

, bootcamps_orders as (
    select
        bootcamps__ecommerce_order.order_id
        , bootcamps__ecommerce_order.line_id
        , bootcamps__ecommerce_order.order_created_on
        , bootcamps__ecommerce_order.order_state
        , bootcamps__ecommerce_order.courserun_id
        , bootcamps__ecommerce_order.order_total_price_paid
        , bootcamps__ecommerce_order.order_purchaser_user_id
        , bootcamps__users.user_email
        , bootcamps__ecommerce_order.receipt_authorization_code
        , bootcamps__ecommerce_order.receipt_transaction_id
    from bootcamps__ecommerce_order
    left join bootcamps__users
        on bootcamps__ecommerce_order.order_purchaser_user_id = bootcamps__users.user_id
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
        , null as b2b_only_indicator
        , null as coupon_id
        , null as coupon_name
        , payment_authorization_code as receipt_authorization_code
        , payment_transaction_id as receipt_transaction_id
    from mitxonline__ecommerce_order

    union all

    select
        '{{ var("mitxpro") }}' as platform
        , order_id
        , line_id
        , order_created_on
        , order_state
        , courserun_id
        , coalesce(order_total_price_paid, b2border_total_price) as order_total_price_paid
        , product_id
        , product_type
        , user_email
        , order_purchaser_user_id as user_id
        , b2b_only_indicator
        , coupon_id
        , coupon_name
        , receipt_authorization_code
        , receipt_transaction_id
    from mitxpro_orders

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
        , user_email
        , order_purchaser_user_id as user_id
        , null as b2b_only_indicator
        , null as coupon_id
        , null as coupon_name
<<<<<<< Updated upstream
        , receipt_authorization_code
        , receipt_transaction_id
=======
        , receipt_authorization_code 
        , receipt_transaction_id 
>>>>>>> Stashed changes
    from bootcamps_orders

)

select * from combined_orders
