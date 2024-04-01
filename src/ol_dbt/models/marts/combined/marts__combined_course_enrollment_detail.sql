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

, mitx_grades as (
    select * from {{ ref('int__mitx__courserun_grades') }}
)

, mitxpro_grades as (
    select * from {{ ref('int__mitxpro__courserun_grades') }}
)

, bootcamps_courses as (
    select * from {{ ref('int__bootcamps__courses') }}
)

, bootcamps_courseruns as (
    select * from {{ ref('int__bootcamps__course_runs') }}
)

, mitx_courses as (
    select * from {{ ref('int__mitx__courses') }}
)

, mitxpro_courseruns as (
    select * from {{ ref('int__mitxpro__course_runs') }}
)

, mitxpro_courses as (
    select * from {{ ref('int__mitxpro__courses') }}
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
        , mitx_enrollments.courserunenrollment_is_edx_enrolled
        , mitx_enrollments.user_id
        , mitx_enrollments.courserun_id
        , mitx_enrollments.courserun_title
        , mitx_enrollments.courserun_readable_id
        , mitx_enrollments.courserun_start_on
        , mitx_enrollments.user_username
        , mitx_enrollments.user_email
        , mitx_enrollments.user_full_name
        , mitx_enrollments.user_address_country as user_country_code
        , if(mitx_certificates.courseruncertificate_url is not null, true, false) as courseruncertificate_is_earned
        , mitx_certificates.courseruncertificate_created_on
        , mitx_certificates.courseruncertificate_url
        , mitx_certificates.courseruncertificate_uuid
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
        , mitx_grades.courserungrade_grade
        , mitx_grades.courserungrade_is_passing
        , mitx_courses.course_title
        , mitx_courses.course_readable_id
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
    left join mitx_grades
        on
            mitx_enrollments.courserun_readable_id = mitx_grades.courserun_readable_id
            and mitx_enrollments.user_mitxonline_username = mitx_grades.user_mitxonline_username
    left join mitx_courses
        on mitx_enrollments.course_number = mitx_courses.course_number
    where mitx_enrollments.platform = '{{ var("mitxonline") }}'

    union all

    select
        mitx_enrollments.platform
        , mitx_enrollments.courserunenrollment_id
        , mitx_enrollments.courserunenrollment_is_active
        , mitx_enrollments.courserunenrollment_created_on
        , mitx_enrollments.courserunenrollment_enrollment_mode
        , mitx_enrollments.courserunenrollment_enrollment_status
        , mitx_enrollments.courserunenrollment_is_edx_enrolled
        , mitx_enrollments.user_id
        , mitx_enrollments.courserun_id
        , mitx_enrollments.courserun_title
        , mitx_enrollments.courserun_readable_id
        , mitx_enrollments.courserun_start_on
        , mitx_enrollments.user_username
        , mitx_enrollments.user_email
        , mitx_enrollments.user_full_name
        , mitx_enrollments.user_address_country as user_country_code
        , if(mitx_certificates.courseruncertificate_url is not null, true, false) as courseruncertificate_is_earned
        , mitx_certificates.courseruncertificate_created_on
        , mitx_certificates.courseruncertificate_url
        , mitx_certificates.courseruncertificate_uuid
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
        , mitx_grades.courserungrade_grade
        , mitx_grades.courserungrade_is_passing
        , mitx_courses.course_title
        , mitx_courses.course_readable_id
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
    left join mitx_grades
        on
            mitx_enrollments.courserun_readable_id = mitx_grades.courserun_readable_id
            and mitx_enrollments.user_edxorg_username = mitx_grades.user_edxorg_username
    left join mitx_courses
        on mitx_enrollments.course_number = mitx_courses.course_number
    where mitx_enrollments.platform = '{{ var("edxorg") }}'


    union all


    select
        '{{ var("mitxpro") }}' as platform
        , mitxpro_enrollments.courserunenrollment_id
        , mitxpro_enrollments.courserunenrollment_is_active
        , mitxpro_enrollments.courserunenrollment_created_on
        , null as courserunenrollment_enrollment_mode
        , mitxpro_enrollments.courserunenrollment_enrollment_status
        , mitxpro_enrollments.courserunenrollment_is_edx_enrolled
        , mitxpro_enrollments.user_id
        , mitxpro_enrollments.courserun_id
        , mitxpro_enrollments.courserun_title
        , mitxpro_enrollments.courserun_readable_id
        , mitxpro_enrollments.courserun_start_on
        , mitxpro_enrollments.user_username
        , mitxpro_enrollments.user_email
        , mitxpro_enrollments.user_full_name
        , mitxpro_enrollments.user_address_country as user_country_code
        , if(mitxpro_certificates.courseruncertificate_url is not null, true, false) as courseruncertificate_is_earned
        , mitxpro_certificates.courseruncertificate_created_on
        , mitxpro_certificates.courseruncertificate_url
        , mitxpro_certificates.courseruncertificate_uuid
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
        , mitxpro_grades.courserungrade_grade
        , mitxpro_grades.courserungrade_is_passing
        , mitxpro_courses.course_title
        , mitxpro_courses.course_readable_id
    from mitxpro_enrollments
    left join mitxpro_certificates
        on
            mitxpro_enrollments.user_id = mitxpro_certificates.user_id
            and mitxpro_enrollments.courserun_id = mitxpro_certificates.courserun_id
    left join mitxpro_completed_orders
        on mitxpro_enrollments.ecommerce_order_id = mitxpro_completed_orders.order_id
    left join mitxpro_lines
        on mitxpro_completed_orders.order_id = mitxpro_lines.order_id
    left join mitxpro_grades
        on
            mitxpro_enrollments.courserun_readable_id = mitxpro_grades.courserun_readable_id
            and mitxpro_enrollments.user_username = mitxpro_grades.user_username
    left join mitxpro_courseruns
        on mitxpro_enrollments.courserun_readable_id = mitxpro_courseruns.courserun_readable_id
    left join mitxpro_courses
        on mitxpro_courseruns.course_id = mitxpro_courses.course_id


    union all

    select
        '{{ var("bootcamps") }}' as platform
        , bootcamps_enrollments.courserunenrollment_id
        , bootcamps_enrollments.courserunenrollment_is_active
        , bootcamps_enrollments.courserunenrollment_created_on
        , null as courserunenrollment_enrollment_mode
        , bootcamps_enrollments.courserunenrollment_enrollment_status
        , null as courserunenrollment_is_edx_enrolled
        , bootcamps_enrollments.user_id
        , bootcamps_enrollments.courserun_id
        , bootcamps_enrollments.courserun_title
        , bootcamps_enrollments.courserun_readable_id
        , bootcamps_enrollments.courserun_start_on
        , bootcamps_enrollments.user_username
        , bootcamps_enrollments.user_email
        , bootcamps_enrollments.user_full_name
        , bootcamps_enrollments.user_address_country as user_country_code
        , if(bootcamps_certificates.courseruncertificate_url is not null, true, false) as courseruncertificate_is_earned
        , bootcamps_certificates.courseruncertificate_created_on
        , bootcamps_certificates.courseruncertificate_url
        , bootcamps_certificates.courseruncertificate_uuid
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
        , null as courserungrade_grade
        , null as courserungrade_is_passing
        , bootcamps_courses.course_title
        , null as course_readable_id  --- to be populated after we add to bootcamp app
    from bootcamps_enrollments
    left join bootcamps_certificates
        on
            bootcamps_enrollments.user_id = bootcamps_certificates.user_id
            and bootcamps_enrollments.courserun_id = bootcamps_certificates.courserun_id
    left join bootcamps_completed_orders
        on
            bootcamps_enrollments.user_id = bootcamps_completed_orders.order_purchaser_user_id
            and bootcamps_enrollments.courserun_id = bootcamps_completed_orders.courserun_id
    left join bootcamps_courseruns
        on bootcamps_enrollments.courserun_id = bootcamps_courseruns.courserun_id
    left join bootcamps_courses
        on bootcamps_courseruns.course_id = bootcamps_courses.course_id
)

select * from combined_enrollment_detail
