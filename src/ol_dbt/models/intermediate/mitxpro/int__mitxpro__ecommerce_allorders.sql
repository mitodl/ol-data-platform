with b2becommerce_b2border as (
    select *
    from {{ ref('int__mitxpro__b2becommerce_b2border') }}
)

, ecommerce_couponpaymentversion as (
    select *
    from {{ ref('int__mitxpro__ecommerce_couponpaymentversion') }}
)

, ecommerce_coupon as (
    select *
    from {{ ref('int__mitxpro__ecommerce_coupon') }}
)

, ecommerce_couponversion as (
    select *
    from {{ ref('int__mitxpro__ecommerce_couponversion') }}
)

, ecommerce_couponredemption as (
    select *
    from {{ ref('int__mitxpro__ecommerce_couponredemption') }}
)

, ecommerce_order as (
    select *
    from {{ ref('int__mitxpro__ecommerce_order') }}
)

, ecommerce_line as (
    select *
    from {{ ref('int__mitxpro__ecommerce_line') }}
)

, users as (
    select *
    from {{ ref('int__mitxpro__users') }}
)

, course_runs as (
    select *
    from {{ ref('int__mitxpro__course_runs') }}
)

, programs as (
    select *
    from {{ ref('int__mitxpro__programs') }}
)

, latest_ecommerce_couponversion as (
    select 
        coupon_id
        , max(couponversion_updated_on) as max_couponversion_updated_on
    from ecommerce_couponversion
    group by coupon_id
)

, b2b_order_fields as (
    select 
        ecommerce_order.order_id
        , b2becommerce_b2border.b2border_id
        , ecommerce_order.order_state
        , ecommerce_line.line_id
        , ecommerce_line.product_id
        , ecommerce_couponpaymentversion.couponpaymentversion_payment_transaction
        , ecommerce_couponpaymentversion.couponpaymentversion_coupon_type
        , ecommerce_couponpaymentversion.couponpaymentversion_discount_amount
        , users.user_email
        , ecommerce_line.product_type
        , course_runs.courserun_readable_id
        , programs.program_readable_id
        , ecommerce_coupon.coupon_id
        , b2becommerce_b2border.b2bcoupon_id
        , case when ecommerce_couponredemption.couponredemption_id is not null then 'Y' else 'N' end as redeemed
    from b2becommerce_b2border 
    left join ecommerce_couponpaymentversion
        on b2becommerce_b2border.couponpaymentversion_id = ecommerce_couponpaymentversion.couponpaymentversion_id
    left join ecommerce_coupon
        on ecommerce_couponpaymentversion.couponpayment_name = ecommerce_coupon.couponpayment_name
    left join latest_ecommerce_couponversion
        on ecommerce_coupon.coupon_id = latest_ecommerce_couponversion.coupon_id
    left join ecommerce_couponversion
        on 
            latest_ecommerce_couponversion.coupon_id = ecommerce_couponversion.coupon_id
            and latest_ecommerce_couponversion.max_couponversion_updated_on 
            = ecommerce_couponversion.couponversion_updated_on
    left join ecommerce_couponredemption
        on ecommerce_couponversion.couponversion_id = ecommerce_couponredemption.couponversion_id
    left join ecommerce_order
        on ecommerce_couponredemption.order_id = ecommerce_order.order_id
    left join ecommerce_line
        on ecommerce_order.order_id = ecommerce_line.order_id
    left join users
        on ecommerce_order.order_purchaser_user_id = users.user_id
    left join course_runs
        on ecommerce_line.courserun_id = course_runs.courserun_id
    left join programs
        on ecommerce_line.program_id = programs.program_id
)

, order_id_test as (
    select order_id
    from b2b_order_fields
    group by order_id
)

, reg_order_fields as (
    select   
        ecommerce_order.order_id
        , null as b2border_id
        , ecommerce_order.order_state
        , ecommerce_line.line_id
        , ecommerce_line.product_id
        , ecommerce_couponpaymentversion.couponpaymentversion_payment_transaction
        , ecommerce_couponpaymentversion.couponpaymentversion_coupon_type
        , ecommerce_couponpaymentversion.couponpaymentversion_discount_amount
        , users.user_email
        , ecommerce_line.product_type
        , course_runs.courserun_readable_id
        , programs.program_readable_id
        , ecommerce_order.coupon_id
        , null as b2bcoupon_id
        , case when ecommerce_couponredemption.couponredemption_id is not null then 'Y' else 'N' end as redeemed
    from ecommerce_order
    inner join ecommerce_line
        on ecommerce_order.order_id = ecommerce_line.order_id
    left join ecommerce_couponpaymentversion
        on ecommerce_order.couponpaymentversion_id = ecommerce_couponpaymentversion.couponpaymentversion_id
    left join ecommerce_couponredemption
        on ecommerce_order.order_id = ecommerce_couponredemption.order_id
    left join users
        on ecommerce_order.order_purchaser_user_id = users.user_id
    left join course_runs
        on ecommerce_line.courserun_id = course_runs.courserun_id
    left join programs
        on ecommerce_line.program_id = programs.program_id
    left join order_id_test
        on ecommerce_order.order_id = order_id_test.order_id
    where order_id_test.order_id is null
)

select 
    order_id
    , b2border_id
    , order_state
    , line_id
    , product_id
    , couponpaymentversion_payment_transaction
    , couponpaymentversion_coupon_type
    , couponpaymentversion_discount_amount
    , redeemed
    , user_email
    , product_type
    , courserun_readable_id
    , program_readable_id
    , coupon_id
    , b2bcoupon_id
from b2b_order_fields

union distinct

select 
    order_id
    , b2border_id
    , order_state
    , line_id
    , product_id
    , couponpaymentversion_payment_transaction
    , couponpaymentversion_coupon_type
    , couponpaymentversion_discount_amount
    , redeemed
    , user_email
    , product_type
    , courserun_readable_id
    , program_readable_id
    , coupon_id
    , b2bcoupon_id
from reg_order_fields
