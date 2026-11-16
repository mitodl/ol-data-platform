with orders as (
    select * from {{ ref('marts__combined__orders') }}
)

, products as (
    select * from {{ ref('marts__combined__products') }}
)

, program as (
    select * from {{ ref('dim_program') }}
)

, program_course as (
    select * from {{ ref('bridge_program_course') }}
)

, course as (
    select * from {{ ref('dim_course') }}
)

, course_enrollment_detail as (
    select * from {{ ref('marts__combined_course_enrollment_detail') }}
)

, verified_enrollment_counts as (
    select
        course_enrollment_detail.user_email
        , program.program_readable_id
        , course_enrollment_detail.platform
        , count(distinct course_enrollment_detail.course_readable_id) as verified_course_count
    from program
    inner join program_course
        on program.program_pk = program_course.program_fk
    inner join course
        on 
            program_course.course_fk = course.course_pk
            and course.is_current = true
            and course.course_is_live = true
    inner join course_enrollment_detail
        on 
            course.course_readable_id = course_enrollment_detail.course_readable_id
            and course_enrollment_detail.enrollment_mode = 'verified'
    group by
        course_enrollment_detail.user_email
        , program.program_readable_id
        , course_enrollment_detail.platform
)

select
    orders.combined_orders_hash_id
    , orders.platform
    , orders.order_id
    , orders.line_id
    , orders.coupon_code
    , orders.coupon_id
    , orders.b2bcoupon_id
    , orders.b2border_contract_number
    , orders.coupon_name
    , orders.coupon_redeemed_on
    , orders.coupon_type
    , orders.couponpaymentversion_payment_transaction
    , orders.courserun_id
    , orders.courserun_readable_id
    , orders.discount
    , orders.order_created_on
    , orders.order_reference_number
    , orders.order_state
    , orders.order_payment_processor
    , orders.order_tax_amount
    , orders.order_tax_country_code
    , orders.order_tax_rate
    , orders.order_tax_rate_name
    , orders.order_total_price_paid_plus_tax
    , orders.order_total_price_paid
    , orders.order_type
    , orders.product_id
    , orders.product_readable_id
    , orders.product_type
    , orders.receipt_authorization_code
    , orders.receipt_bill_to_address_state
    , orders.receipt_bill_to_address_country
    , orders.receipt_payment_amount
    , orders.receipt_payment_currency
    , orders.receipt_payment_card_number
    , orders.receipt_payment_card_type
    , orders.receipt_payment_method
    , orders.receipt_payment_timestamp
    , orders.receipt_payment_transaction_type
    , orders.receipt_payment_transaction_uuid
    , orders.receipt_payer_name
    , orders.receipt_payer_email
    , orders.receipt_payer_ip_address
    , orders.receipt_transaction_id
    , orders.redeemed_email
    , orders.req_reference_number
    , orders.unit_price
    , orders.user_email
    , orders.user_id
    , orders.user_hashed_id
    , products.product_name
    , verified_enrollment_counts.verified_enrollment_count
from orders
left join products
    on orders.product_readable_id = products.product_readable_id
    and orders.platform = products.platform
left join verified_enrollment_counts
    on orders.user_email = verified_enrollment_counts.user_email
    and orders.product_readable_id = verified_enrollment_counts.program_readable_id
    and orders.platform = verified_enrollment_counts.platform
