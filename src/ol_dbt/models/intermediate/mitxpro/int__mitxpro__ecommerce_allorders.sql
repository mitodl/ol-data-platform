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

, reg_order_fields as (
    select
        ecommerce_order.order_id
        , b2becommerce_b2border.b2border_id
        , ecommerce_order.order_state
        , ecommerce_line.line_id
        , ecommerce_line.product_id
        , ecommerce_couponpaymentversion.couponpaymentversion_payment_transaction
        , ecommerce_couponpaymentversion.couponpaymentversion_coupon_type
        , b2becommerce_b2border.b2border_discount
        , users.user_email
        , ecommerce_line.product_type
        , course_runs.courserun_id
        , course_runs.courserun_readable_id
        , programs.program_readable_id
        , ecommerce_order.coupon_id
        , ecommerce_order.order_created_on
        , productversion.productversion_readable_id
        , b2becommerce_b2border.b2bcoupon_id
        , b2becommerce_b2border.b2border_contract_number
        , ecommerce_order.receipt_reference_number as req_reference_number
        , ecommerce_order.receipt_authorization_code
        , ecommerce_order.receipt_transaction_id
        , ecommerce_order.receipt_payment_method
        , ecommerce_order.receipt_bill_to_address_state
        , ecommerce_order.receipt_bill_to_address_country
        , ecommerce_couponredemption.couponredemption_created_on as coupon_redeemed_on
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
    left join productversion
        on ecommerce_line.productversion_id = productversion.productversion_id
    left join b2becommerce_b2border
        on ecommerce_couponpaymentversion.couponpaymentversion_id = b2becommerce_b2border.couponpaymentversion_id
)

, reg_order_test as (
    select b2border_id
    from reg_order_fields
    group by b2border_id
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
        , course_runs.courserun_id
        , course_runs.courserun_readable_id
        , programs.program_readable_id
        , ecommerce_coupon.coupon_id
        , b2becommerce_b2border.b2bcoupon_id
        , b2becommerce_b2border.b2border_created_on as order_created_on
        , b2becommerce_b2border.b2border_status as order_state
        , b2becommerce_b2border.product_type
        , b2becommerce_b2border.b2border_email as user_email
        , b2becommerce_b2border.b2border_contract_number
        , productversion.productversion_readable_id
        , b2becommerce_b2breceipt.b2breceipt_reference_number as req_reference_number
        , b2becommerce_b2breceipt.b2breceipt_authorization_code as receipt_authorization_code
        , b2becommerce_b2breceipt.b2breceipt_transaction_id as receipt_transaction_id
        , b2becommerce_b2breceipt.b2breceipt_payment_method as receipt_payment_method
        , b2becommerce_b2breceipt.b2breceipt_bill_to_address_state as receipt_bill_to_address_state
        , b2becommerce_b2breceipt.b2breceipt_bill_to_address_country as receipt_bill_to_address_country
        , b2becommerce_b2bcouponredemption.b2bcouponredemption_created_on as coupon_redeemed_on
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
    left join reg_order_test
        on b2becommerce_b2border.b2border_id = reg_order_test.b2border_id
    where reg_order_test.b2border_id is null
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
    , coupon_redeemed_on
    , user_email
    , product_type
    , courserun_id
    , courserun_readable_id
    , program_readable_id
    , coupon_id
    , b2bcoupon_id
    , b2border_contract_number
    , req_reference_number
    , productversion_readable_id
    , receipt_authorization_code
    , receipt_transaction_id
    , receipt_payment_method
    , receipt_bill_to_address_state
    , receipt_bill_to_address_country
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
    , coupon_redeemed_on
    , user_email
    , product_type
    , courserun_id
    , courserun_readable_id
    , program_readable_id
    , coupon_id
    , b2bcoupon_id
    , b2border_contract_number
    , req_reference_number
    , productversion_readable_id
    , receipt_authorization_code
    , receipt_transaction_id
    , receipt_payment_method
    , receipt_bill_to_address_state
    , receipt_bill_to_address_country
from b2b_order_fields
