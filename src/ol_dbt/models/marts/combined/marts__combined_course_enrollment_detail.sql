with mitx_enrollments as (
    select * from {{ ref('int__mitx__courserun_enrollments') }}
)

, mitxpro_enrollments as (
    select * from {{ ref('int__mitxpro__courserunenrollments') }}
)

, bootcamps_enrollments as (
    select * from {{ ref('int__bootcamps__courserunenrollments') }}
)

, mitx_certificates as (
    select * from {{ ref('int__mitx__courserun_certificates') }}
)

, mitxpro_certificates as (
    select * from {{ ref('int__mitxpro__courserun_certificates') }}
)

, bootcamps_certificates as (
    select * from {{ ref('int__bootcamps__courserun_certificates') }}
)

, mitxonline_completed_orders as (
    select
        *
        , row_number() over (
            partition by user_id, courserun_id order by order_created_on desc
        ) as row_num
    from {{ ref('int__mitxonline__ecommerce_order') }}
    where order_state in ('fulfilled', 'refunded')
)

, micromasters_completed_orders as (
    select
        *
        , row_number() over (
            partition by user_id, courserun_readable_id order by order_created_on desc, order_id desc
        ) as row_num
    from {{ ref('int__micromasters__orders') }}
    where order_state in ('fulfilled', 'refunded', 'partially_refunded')
)

, micromasters_users as (
    select * from {{ ref('int__micromasters__users') }}
)

, mitxpro_completed_orders as (
    select * from {{ ref('int__mitxpro__ecommerce_order') }}
    where order_state in ('fulfilled', 'refunded')
)

---Unlike other platforms, xPro lines and orders are not 1 to 1, so we can't combined them into one table
, mitxpro_lines as (
    select * from {{ ref('int__mitxpro__ecommerce_line') }}
)

, bootcamps_completed_orders as (
    select * from {{ ref('int__bootcamps__ecommerce_order') }}
    where order_state in ('fulfilled', 'refunded')
)

, combined_enrollment_detail as (
    select
        mitx_enrollments.platform
        , mitx_enrollments.courserunenrollment_id
        , mitx_enrollments.courserunenrollment_is_active
        , mitx_enrollments.courserunenrollment_created_on
        , mitx_enrollments.courserunenrollment_enrollment_mode
        , mitx_enrollments.courserunenrollment_enrollment_status
        , mitx_enrollments.user_id
        , mitx_enrollments.courserun_id
        , mitx_enrollments.courserun_title
        , mitx_enrollments.courserun_readable_id
        , mitx_enrollments.user_username
        , mitx_enrollments.user_email
        , mitx_enrollments.user_full_name
        , mitx_certificates.courseruncertificate_created_on
        , mitx_certificates.courseruncertificate_url
        , mitxonline_completed_orders.order_state
        , mitxonline_completed_orders.order_reference_number
        , mitxonline_completed_orders.order_total_price_paid
        , mitxonline_completed_orders.order_created_on
        , mitxonline_completed_orders.product_price as line_price
        , mitxonline_completed_orders.discount_code as coupon_code
        , mitxonline_completed_orders.discountredemption_timestamp as coupon_redeemed_on
        , mitxonline_completed_orders.discount_amount_text as coupon_discount_amount
        , mitxonline_completed_orders.payment_transaction_id
        , mitxonline_completed_orders.payment_authorization_code
        , mitxonline_completed_orders.payment_method
        , mitxonline_completed_orders.payment_bill_to_address_state
        , mitxonline_completed_orders.payment_bill_to_address_country
    from mitx_enrollments
    left join mitx_certificates
        on
            mitx_enrollments.user_mitxonline_username = mitx_certificates.user_mitxonline_username
            and mitx_enrollments.courserun_readable_id = mitx_certificates.courserun_readable_id
    left join mitxonline_completed_orders
        on
            mitx_enrollments.user_id = mitxonline_completed_orders.user_id
            and mitx_enrollments.courserun_id = mitxonline_completed_orders.courserun_id
            and mitxonline_completed_orders.row_num = 1
    where mitx_enrollments.platform = '{{ var("mitxonline") }}'

    union all

    select
        mitx_enrollments.platform
        , mitx_enrollments.courserunenrollment_id
        , mitx_enrollments.courserunenrollment_is_active
        , mitx_enrollments.courserunenrollment_created_on
        , mitx_enrollments.courserunenrollment_enrollment_mode
        , mitx_enrollments.courserunenrollment_enrollment_status
        , mitx_enrollments.user_id
        , mitx_enrollments.courserun_id
        , mitx_enrollments.courserun_title
        , mitx_enrollments.courserun_readable_id
        , mitx_enrollments.user_username
        , mitx_enrollments.user_email
        , mitx_enrollments.user_full_name
        , mitx_certificates.courseruncertificate_created_on
        , mitx_certificates.courseruncertificate_url
        , micromasters_completed_orders.order_state
        , micromasters_completed_orders.order_reference_number
        , micromasters_completed_orders.order_total_price_paid
        , micromasters_completed_orders.order_created_on
        , micromasters_completed_orders.line_price
        , micromasters_completed_orders.coupon_code
        , micromasters_completed_orders.redeemedcoupon_created_on as coupon_redeemed_on
        , micromasters_completed_orders.coupon_discount_amount_text as coupon_discount_amount
        , micromasters_completed_orders.receipt_transaction_id as payment_transaction_id
        , micromasters_completed_orders.receipt_authorization_code as payment_authorization_code
        , micromasters_completed_orders.receipt_payment_method as payment_method
        , micromasters_completed_orders.receipt_bill_to_address_state as payment_bill_to_address_state
        , micromasters_completed_orders.receipt_bill_to_address_country as payment_bill_to_address_country
    from mitx_enrollments
    left join mitx_certificates
        on
            mitx_enrollments.user_edxorg_username = mitx_certificates.user_edxorg_username
            and mitx_enrollments.courserun_readable_id = mitx_certificates.courserun_readable_id
    left join micromasters_users on mitx_enrollments.user_edxorg_username = micromasters_users.user_edxorg_username
    left join micromasters_completed_orders
        on
            micromasters_users.user_id = micromasters_completed_orders.user_id
            and mitx_enrollments.courserun_readable_id = micromasters_completed_orders.courserun_edxorg_readable_id
            and micromasters_completed_orders.row_num = 1
    where mitx_enrollments.platform = '{{ var("edxorg") }}'


    union all


    select
        '{{ var("mitxpro") }}' as platform
        , mitxpro_enrollments.courserunenrollment_id
        , mitxpro_enrollments.courserunenrollment_is_active
        , mitxpro_enrollments.courserunenrollment_created_on
        , null as courserunenrollment_enrollment_mode
        , mitxpro_enrollments.courserunenrollment_enrollment_status
        , mitxpro_enrollments.user_id
        , mitxpro_enrollments.courserun_id
        , mitxpro_enrollments.courserun_title
        , mitxpro_enrollments.courserun_readable_id
        , mitxpro_enrollments.user_username
        , mitxpro_enrollments.user_email
        , mitxpro_enrollments.user_full_name
        , mitxpro_certificates.courseruncertificate_created_on
        , mitxpro_certificates.courseruncertificate_url
        , mitxpro_completed_orders.order_state
        , mitxpro_completed_orders.receipt_reference_number as order_reference_number
        , mitxpro_completed_orders.order_total_price_paid
        , mitxpro_completed_orders.order_created_on
        , mitxpro_lines.product_price as line_price
        , mitxpro_completed_orders.coupon_code
        , mitxpro_completed_orders.couponredemption_created_on as coupon_redeemed_on
        , mitxpro_completed_orders.couponpaymentversion_discount_amount_text as coupon_discount_amount
        , mitxpro_completed_orders.receipt_transaction_id as payment_transaction_id
        , mitxpro_completed_orders.receipt_authorization_code as payment_authorization_code
        , mitxpro_completed_orders.receipt_payment_method as payment_method
        , mitxpro_completed_orders.receipt_bill_to_address_state as payment_bill_to_address_state
        , mitxpro_completed_orders.receipt_bill_to_address_country as payment_bill_to_address_country
    from mitxpro_enrollments
    left join mitxpro_certificates
        on
            mitxpro_enrollments.user_id = mitxpro_certificates.user_id
            and mitxpro_enrollments.courserun_id = mitxpro_certificates.courserun_id
    left join mitxpro_completed_orders
        on mitxpro_enrollments.ecommerce_order_id = mitxpro_completed_orders.order_id
    left join mitxpro_lines
        on
            mitxpro_completed_orders.order_id = mitxpro_lines.order_id
            and mitxpro_enrollments.courserun_id = mitxpro_lines.courserun_id

    union all

    select
        '{{ var("bootcamps") }}' as platform
        , bootcamps_enrollments.courserunenrollment_id
        , bootcamps_enrollments.courserunenrollment_is_active
        , bootcamps_enrollments.courserunenrollment_created_on
        , null as courserunenrollment_enrollment_mode
        , bootcamps_enrollments.courserunenrollment_enrollment_status
        , bootcamps_enrollments.user_id
        , bootcamps_enrollments.courserun_id
        , bootcamps_enrollments.courserun_title
        , bootcamps_enrollments.courserun_readable_id
        , bootcamps_enrollments.user_username
        , bootcamps_enrollments.user_email
        , bootcamps_enrollments.user_full_name
        , bootcamps_certificates.courseruncertificate_created_on
        , bootcamps_certificates.courseruncertificate_url
        , bootcamps_completed_orders.order_state
        , bootcamps_completed_orders.order_reference_number
        , bootcamps_completed_orders.order_total_price_paid
        , bootcamps_completed_orders.order_created_on
        , bootcamps_completed_orders.line_price
        , null as coupon_code
        , null as coupon_redeemed_on
        , null as coupon_discount_amount
        , bootcamps_completed_orders.receipt_transaction_id as payment_transaction_id
        , bootcamps_completed_orders.receipt_authorization_code as payment_authorization_code
        , bootcamps_completed_orders.receipt_payment_method as payment_method
        , bootcamps_completed_orders.receipt_bill_to_address_state as payment_bill_to_address_state
        , bootcamps_completed_orders.receipt_bill_to_address_country as payment_bill_to_address_country
    from bootcamps_enrollments
    left join bootcamps_certificates
        on
            bootcamps_enrollments.user_id = bootcamps_certificates.user_id
            and bootcamps_enrollments.courserun_id = bootcamps_certificates.courserun_id
    left join bootcamps_completed_orders
        on
            bootcamps_enrollments.user_id = bootcamps_completed_orders.order_purchaser_user_id
            and bootcamps_enrollments.courserun_id = bootcamps_completed_orders.courserun_id
)

select * from combined_enrollment_detail