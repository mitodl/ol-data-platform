with combined_enrollments as (
    select * from {{ ref('int__combined__courserun_enrollments') }}
)

, mitxpro_enrollments as (
    select * from {{ ref('int__mitxpro__courserunenrollments') }}
)

, combined_courseruns as (
    select * from {{ ref('int__combined__course_runs') }}
)

, combined_users as (
    select * from {{ ref('int__combined__users') }}
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
        combined_enrollments.platform
        , combined_enrollments.courserunenrollment_id
        , combined_enrollments.courserunenrollment_is_active
        , combined_enrollments.courserunenrollment_created_on
        , combined_enrollments.courserunenrollment_enrollment_mode
        , combined_enrollments.courserunenrollment_enrollment_status
        , combined_enrollments.courserunenrollment_is_edx_enrolled
        , combined_enrollments.user_id
        , combined_enrollments.courserun_id
        , combined_enrollments.courserun_title
        , combined_enrollments.courserun_readable_id
        , combined_courseruns.courserun_start_on
        , combined_courseruns.courserun_end_on
        , combined_courseruns.courserun_is_current
        , combined_enrollments.user_username
        , combined_enrollments.user_email
        , combined_enrollments.user_full_name
        , combined_users.user_address_country as user_country_code
        , combined_users.user_highest_education
        , combined_users.user_company
        , combined_enrollments.courseruncertificate_is_earned
        , combined_enrollments.courseruncertificate_created_on
        , combined_enrollments.courseruncertificate_url
        , combined_enrollments.courseruncertificate_uuid
        , mitxonline_completed_orders.order_id
        , mitxonline_completed_orders.order_state
        , mitxonline_completed_orders.order_reference_number
        , mitxonline_completed_orders.order_created_on
        , mitxonline_completed_orders.discount_source as payment_type
        , mitxonline_completed_orders.discount_redemption_type as coupon_type
        , mitxonline_completed_orders.discount_code as coupon_code
        , mitxonline_completed_orders.discountredemption_timestamp as coupon_redeemed_on
        , mitxonline_completed_orders.payment_transaction_id
        , mitxonline_completed_orders.payment_authorization_code
        , mitxonline_completed_orders.payment_method
        , mitxonline_completed_orders.payment_req_reference_number
        , mitxonline_completed_orders.payment_bill_to_address_state
        , mitxonline_completed_orders.payment_bill_to_address_country
        , null as order_tax_country_code
        , null as order_tax_rate
        , null as order_tax_rate_name
        , mitxonline_completed_orders.product_price as unit_price
        , mitxonline_completed_orders.order_total_price_paid
        , null as order_tax_amount
        , mitxonline_completed_orders.order_total_price_paid as order_total_price_paid_plus_tax
        , mitxonline_completed_orders.product_id
        , mitxonline_completed_orders.program_readable_id as product_program_readable_id
        , mitxonline_completed_orders.discount_amount_text as discount
        , mitxonline_completed_orders.discount_amount
        , combined_enrollments.courserungrade_grade
        , combined_enrollments.courserungrade_is_passing
        , combined_courseruns.course_title
        , combined_courseruns.course_readable_id
    from combined_enrollments
    left join combined_users
        on
            combined_enrollments.user_username = combined_users.user_username
            and combined_enrollments.platform = combined_users.platform
    left join mitxonline_completed_orders
        on
            combined_enrollments.user_id = mitxonline_completed_orders.user_id
            and combined_enrollments.courserun_id = mitxonline_completed_orders.courserun_id
            and mitxonline_completed_orders.row_num = 1
    left join combined_courseruns
        on combined_enrollments.courserun_readable_id = combined_courseruns.courserun_readable_id
    where combined_enrollments.platform = '{{ var("mitxonline") }}'

    union all

    select
        combined_enrollments.platform
        , combined_enrollments.courserunenrollment_id
        , combined_enrollments.courserunenrollment_is_active
        , combined_enrollments.courserunenrollment_created_on
        , combined_enrollments.courserunenrollment_enrollment_mode
        , combined_enrollments.courserunenrollment_enrollment_status
        , combined_enrollments.courserunenrollment_is_edx_enrolled
        , combined_enrollments.user_id
        , combined_enrollments.courserun_id
        , combined_enrollments.courserun_title
        , combined_enrollments.courserun_readable_id
        , combined_courseruns.courserun_start_on
        , combined_courseruns.courserun_end_on
        , if(combined_courseruns.courserun_is_current is null, false, combined_courseruns.courserun_is_current)
        as courserun_is_current
        , combined_enrollments.user_username
        , combined_enrollments.user_email
        , combined_enrollments.user_full_name
        , combined_users.user_address_country as user_country_code
        , combined_users.user_highest_education
        , combined_users.user_company
        , combined_enrollments.courseruncertificate_is_earned
        , combined_enrollments.courseruncertificate_created_on
        , combined_enrollments.courseruncertificate_url
        , combined_enrollments.courseruncertificate_uuid
        , micromasters_completed_orders.order_id
        , micromasters_completed_orders.order_state
        , micromasters_completed_orders.order_reference_number
        , micromasters_completed_orders.order_created_on
        , null as payment_type
        , micromasters_completed_orders.coupon_type
        , micromasters_completed_orders.coupon_code
        , micromasters_completed_orders.redeemedcoupon_created_on as coupon_redeemed_on
        , micromasters_completed_orders.receipt_transaction_id
        , micromasters_completed_orders.receipt_authorization_code
        , micromasters_completed_orders.receipt_payment_method
        , micromasters_completed_orders.receipt_reference_number as receipt_req_reference_number
        , micromasters_completed_orders.receipt_bill_to_address_state
        , micromasters_completed_orders.receipt_bill_to_address_country
        , null as order_tax_country_code
        , null as order_tax_rate
        , null as order_tax_rate_name
        , micromasters_completed_orders.line_price as unit_price
        , micromasters_completed_orders.order_total_price_paid
        , null as order_tax_amount
        , micromasters_completed_orders.order_total_price_paid as order_total_price_paid_plus_tax
        , null as product_id
        , null as product_program_readable_id
        , micromasters_completed_orders.coupon_discount_amount_text as discount
        , micromasters_completed_orders.coupon_amount as discount_amount
        , combined_enrollments.courserungrade_grade
        , combined_enrollments.courserungrade_is_passing
        , combined_courseruns.course_title
        , combined_courseruns.course_readable_id
    from combined_enrollments
    left join combined_users
        on
            combined_enrollments.user_username = combined_users.user_username
            and combined_enrollments.platform = combined_users.platform
    left join micromasters_users on combined_enrollments.user_username = micromasters_users.user_edxorg_username
    left join micromasters_completed_orders
        on
            micromasters_users.user_id = micromasters_completed_orders.user_id
            and combined_enrollments.courserun_readable_id = micromasters_completed_orders.courserun_edxorg_readable_id
            and micromasters_completed_orders.row_num = 1
    left join combined_courseruns
        on combined_enrollments.courserun_readable_id = combined_courseruns.courserun_readable_id
    where combined_enrollments.platform = '{{ var("edxorg") }}'


    union all


    select
        '{{ var("mitxpro") }}' as platform
        , combined_enrollments.courserunenrollment_id
        , combined_enrollments.courserunenrollment_is_active
        , combined_enrollments.courserunenrollment_created_on
        , combined_enrollments.courserunenrollment_enrollment_mode
        , combined_enrollments.courserunenrollment_enrollment_status
        , combined_enrollments.courserunenrollment_is_edx_enrolled
        , combined_enrollments.user_id
        , combined_enrollments.courserun_id
        , combined_enrollments.courserun_title
        , combined_enrollments.courserun_readable_id
        , combined_courseruns.courserun_start_on
        , combined_courseruns.courserun_end_on
        , combined_courseruns.courserun_is_current
        , combined_enrollments.user_username
        , combined_enrollments.user_email
        , combined_enrollments.user_full_name
        , combined_users.user_address_country as user_country_code
        , combined_users.user_highest_education
        , combined_users.user_company
        , combined_enrollments.courseruncertificate_is_earned
        , combined_enrollments.courseruncertificate_created_on
        , combined_enrollments.courseruncertificate_url
        , combined_enrollments.courseruncertificate_uuid
        , mitxpro_completed_orders.order_id
        , mitxpro_completed_orders.order_state
        , mitxpro_completed_orders.receipt_reference_number as order_reference_number
        , mitxpro_completed_orders.order_created_on
        , mitxpro_completed_orders.couponpaymentversion_discount_source as payment_type
        , mitxpro_completed_orders.couponpaymentversion_coupon_type as coupon_type
        , mitxpro_completed_orders.coupon_code
        , mitxpro_completed_orders.couponredemption_created_on as coupon_redeemed_on
        , mitxpro_completed_orders.receipt_transaction_id
        , mitxpro_completed_orders.receipt_authorization_code
        , mitxpro_completed_orders.receipt_payment_method
        , mitxpro_completed_orders.receipt_reference_number as receipt_req_reference_number
        , mitxpro_completed_orders.receipt_bill_to_address_state
        , mitxpro_completed_orders.receipt_bill_to_address_country
        , mitxpro_completed_orders.order_tax_country_code
        , mitxpro_completed_orders.order_tax_rate
        , mitxpro_completed_orders.order_tax_rate_name
        , mitxpro_lines.product_price as unit_price
        , mitxpro_completed_orders.order_total_price_paid
        , mitxpro_completed_orders.order_tax_amount
        , mitxpro_completed_orders.order_total_price_paid_plus_tax
        , mitxpro_lines.product_id
        , mitxpro_lines.program_readable_id as product_program_readable_id
        , mitxpro_completed_orders.couponpaymentversion_discount_amount_text as discount
        , case
            when mitxpro_completed_orders.couponpaymentversion_discount_type = 'percent-off'
                then mitxpro_lines.product_price * mitxpro_completed_orders.couponpaymentversion_discount_amount
            else mitxpro_completed_orders.couponpaymentversion_discount_amount
        end as discount_amount
        , combined_enrollments.courserungrade_grade
        , combined_enrollments.courserungrade_is_passing
        , combined_courseruns.course_title
        , combined_courseruns.course_readable_id
    from mitxpro_enrollments
    inner join combined_enrollments
        on
            mitxpro_enrollments.user_id = combined_enrollments.user_id
            and mitxpro_enrollments.courserun_readable_id = combined_enrollments.courserun_readable_id
    left join combined_users
        on
            mitxpro_enrollments.user_username = combined_users.user_username
            and combined_enrollments.platform = combined_users.platform
    left join mitxpro_completed_orders
        on mitxpro_enrollments.ecommerce_order_id = mitxpro_completed_orders.order_id
    left join mitxpro_lines
        on mitxpro_completed_orders.order_id = mitxpro_lines.order_id
    left join combined_courseruns
        on mitxpro_enrollments.courserun_readable_id = combined_courseruns.courserun_readable_id
    where combined_enrollments.platform = '{{ var("mitxpro") }}'


    union all

    select
        '{{ var("bootcamps") }}' as platform
        , combined_enrollments.courserunenrollment_id
        , combined_enrollments.courserunenrollment_is_active
        , combined_enrollments.courserunenrollment_created_on
        , combined_enrollments.courserunenrollment_enrollment_mode
        , combined_enrollments.courserunenrollment_enrollment_status
        , combined_enrollments.courserunenrollment_is_edx_enrolled
        , combined_enrollments.user_id
        , combined_enrollments.courserun_id
        , combined_enrollments.courserun_title
        , combined_enrollments.courserun_readable_id
        , combined_courseruns.courserun_start_on
        , combined_courseruns.courserun_end_on
        , combined_courseruns.courserun_is_current
        , combined_enrollments.user_username
        , combined_enrollments.user_email
        , combined_enrollments.user_full_name
        , combined_users.user_address_country as user_country_code
        , combined_users.user_highest_education
        , combined_users.user_company
        , combined_enrollments.courseruncertificate_is_earned
        , combined_enrollments.courseruncertificate_created_on
        , combined_enrollments.courseruncertificate_url
        , combined_enrollments.courseruncertificate_uuid
        , bootcamps_completed_orders.order_id
        , bootcamps_completed_orders.order_state
        , bootcamps_completed_orders.order_reference_number
        , bootcamps_completed_orders.order_created_on
        , null as payment_type
        , null as coupon_type
        , null as coupon_code
        , null as coupon_redeemed_on
        , bootcamps_completed_orders.receipt_transaction_id
        , bootcamps_completed_orders.receipt_authorization_code
        , bootcamps_completed_orders.receipt_payment_method
        , bootcamps_completed_orders.receipt_reference_number as receipt_req_reference_number
        , bootcamps_completed_orders.receipt_bill_to_address_state
        , bootcamps_completed_orders.receipt_bill_to_address_country
        , null as order_tax_country_code
        , null as order_tax_rate
        , null as order_tax_rate_name
        , bootcamps_completed_orders.line_price as unit_price
        , bootcamps_completed_orders.order_total_price_paid
        , null as order_tax_amount
        , bootcamps_completed_orders.order_total_price_paid as order_total_price_paid_plus_tax
        , null as product_id
        , null as product_program_readable_id
        , null as discount
        , null as discount_amount
        , combined_enrollments.courserungrade_grade
        , combined_enrollments.courserungrade_is_passing
        , combined_courseruns.course_title
        , combined_courseruns.course_readable_id
    from combined_enrollments
    left join combined_users
        on
            combined_enrollments.user_username = combined_users.user_username
            and combined_enrollments.platform = combined_users.platform
    left join bootcamps_completed_orders
        on
            combined_enrollments.user_id = bootcamps_completed_orders.order_purchaser_user_id
            and combined_enrollments.courserun_id = bootcamps_completed_orders.courserun_id
    left join combined_courseruns
        on combined_enrollments.courserun_readable_id = combined_courseruns.courserun_readable_id
    where combined_enrollments.platform = '{{ var("mitxpro") }}'
)

select * from combined_enrollment_detail
