with b2becommerce_b2border as (
    select *
    from {{ ref('int__mitxpro__b2becommerce_b2border') }}
)

, b2becommerce_b2breceipt as (
    select *
    from {{ ref('int__mitxpro__b2becommerce_b2breceipt') }}
)

, b2becommerce_b2bcouponredemption as (
    select *
    from {{ ref('int__mitxpro__b2becommerce_b2bcouponredemption') }}
)

, ecommerce_couponpaymentversion as (
    select *
    from {{ ref('int__mitxpro__ecommerce_couponpaymentversion') }}
)

, ecommerce_coupon as (
    select *
    from {{ ref('int__mitxpro__ecommerce_coupon') }}
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

, productversion as (
    select *
    from {{ ref('int__mitxpro__ecommerce_productversion') }}
)

, b2b_order_fields as (
    select
        ecommerce_order.order_id
        , b2becommerce_b2border.b2border_id
        , ecommerce_line.line_id
        , b2becommerce_b2border.product_id
        , ecommerce_couponpaymentversion.couponpaymentversion_payment_transaction
        , ecommerce_couponpaymentversion.couponpaymentversion_coupon_type
        , b2becommerce_b2border.b2border_discount
        , course_runs.courserun_readable_id
        , programs.program_readable_id
        , ecommerce_coupon.coupon_id
        , b2becommerce_b2border.b2bcoupon_id
        , b2becommerce_b2border.b2border_created_on as order_created_on
        , b2becommerce_b2border.b2border_status as order_state
        , b2becommerce_b2border.product_type as product_type
        , b2becommerce_b2border.b2border_email as user_email
        , b2becommerce_b2border.b2border_contract_number
        , productversion.productversion_readable_id
        , json_extract_scalar(b2becommerce_b2breceipt.b2breceipt_data, '$.req_reference_number') as req_reference_number
        , json_extract_scalar(b2becommerce_b2breceipt.b2breceipt_data, '$.auth_code') as receipt_authorization_code
        , json_extract_scalar(b2becommerce_b2breceipt.b2breceipt_data, '$.transaction_id') as receipt_transaction_id
        , case when b2becommerce_b2bcouponredemption.b2bcouponredemption_id is not null then true end as redeemed
    from b2becommerce_b2border
    left join ecommerce_couponpaymentversion
        on b2becommerce_b2border.couponpaymentversion_id = ecommerce_couponpaymentversion.couponpaymentversion_id
    left join ecommerce_coupon
        on ecommerce_couponpaymentversion.couponpayment_name = ecommerce_coupon.couponpayment_name
    left join ecommerce_order
        on ecommerce_couponpaymentversion.couponpaymentversion_id = ecommerce_order.couponpaymentversion_id
    left join ecommerce_line
        on ecommerce_order.order_id = ecommerce_line.order_id
    left join course_runs
        on ecommerce_line.courserun_id = course_runs.courserun_id
    left join programs
        on ecommerce_line.program_id = programs.program_id
    left join b2becommerce_b2breceipt
        on b2becommerce_b2border.b2border_id = b2becommerce_b2breceipt.b2border_id
    left join productversion
        on b2becommerce_b2border.productversion_id = productversion.productversion_id
    left join b2becommerce_b2bcouponredemption
        on
            b2becommerce_b2border.b2border_id = b2becommerce_b2bcouponredemption.b2border_id
            and b2becommerce_b2border.b2bcoupon_id = b2becommerce_b2bcouponredemption.b2bcoupon_id
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
        , null as b2border_discount
        , users.user_email
        , ecommerce_line.product_type
        , course_runs.courserun_readable_id
        , programs.program_readable_id
        , ecommerce_order.coupon_id
        , ecommerce_order.order_created_on
        , productversion.productversion_readable_id
        , null as b2bcoupon_id
        , null as b2border_contract_number
        , null as req_reference_number
<<<<<<< Updated upstream
        , ecommerce_order.receipt_authorization_code
        , ecommerce_order.receipt_transaction_id
=======
        , ecommerce_order.receipt_authorization_code
        , ecommerce_order.receipt_transaction_id
>>>>>>> Stashed changes
        , case when ecommerce_couponredemption.couponredemption_id is not null then true end as redeemed
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
    left join productversion
        on ecommerce_line.productversion_id = productversion.productversion_id
    where order_id_test.order_id is null
)

select
    order_id
    , b2border_id
    , order_created_on
    , order_state
    , line_id
    , product_id
    , couponpaymentversion_payment_transaction
    , couponpaymentversion_coupon_type
    , b2border_discount
    , redeemed
    , user_email
    , product_type
    , courserun_readable_id
    , program_readable_id
    , coupon_id
    , b2bcoupon_id
    , b2border_contract_number
    , req_reference_number
    , productversion_readable_id
    , receipt_authorization_code
    , receipt_transaction_id
from reg_order_fields

union distinct

select
    order_id
    , b2border_id
    , order_created_on
    , order_state
    , line_id
    , product_id
    , couponpaymentversion_payment_transaction
    , couponpaymentversion_coupon_type
    , b2border_discount
    , redeemed
    , user_email
    , product_type
    , courserun_readable_id
    , program_readable_id
    , coupon_id
    , b2bcoupon_id
    , b2border_contract_number
    , req_reference_number
    , productversion_readable_id
    , receipt_authorization_code
    , receipt_transaction_id
from b2b_order_fields
