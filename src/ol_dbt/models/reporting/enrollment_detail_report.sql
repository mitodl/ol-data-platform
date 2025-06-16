with enrollments as (
    select * from {{ ref('marts__combined_course_enrollment_detail') }}
)

, orders as (
    select * from {{ ref('marts__combined__orders') }}
)

, program_enrollments as (
    select * from {{ ref('marts__combined_program_enrollment_detail') }}
)

, coursesinprogram as (
    select * from {{ ref('marts__combined_coursesinprogram') }}
)

, programs as (
    select distinct
        program_enrollments.platform_name
        , program_enrollments.program_name
        , program_enrollments.user_email
        , coursesinprogram.course_readable_id
    from program_enrollments
    inner join coursesinprogram
        on
            program_enrollments.platform_name = coursesinprogram.platform
            and program_enrollments.program_name = coursesinprogram.program_name
            and program_enrollments.program_id = coursesinprogram.program_id
    where coursesinprogram.program_name <> 'Computer Science'
)

, course_passed_counts as (
    select
        user_email
        , count(distinct course_title) as num_of_course_passed
    from course_enrollments
    where courserungrade_is_passing = true
    group by user_email
)

select
    enrollments.platform
    , enrollments.courserunenrollment_id
    , enrollments.course_readable_id
    , enrollments.course_title
    , enrollments.courserun_id
    , enrollments.courserun_is_current
    , enrollments.courserun_readable_id
    , enrollments.courserun_start_on
    , enrollments.courserun_end_on
    , enrollments.courserun_title
    , enrollments.courseruncertificate_created_on
    , enrollments.courseruncertificate_is_earned
    , enrollments.courseruncertificate_url
    , enrollments.courseruncertificate_uuid
    , enrollments.courserunenrollment_created_on
    , enrollments.courserunenrollment_enrollment_mode
    , enrollments.courserunenrollment_enrollment_status
    , enrollments.courserunenrollment_is_active
    , enrollments.courserunenrollment_is_edx_enrolled
    , enrollments.courserunenrollment_upgraded_on
    , enrollments.courserungrade_grade
    , enrollments.courserungrade_is_passing
    , enrollments.order_id
    , enrollments.order_reference_number
    , enrollments.user_company
    , enrollments.user_country_code
    , enrollments.user_full_name
    , enrollments.user_highest_education
    , enrollments.user_id
    , enrollments.user_hashed_id
    , enrollments.user_username
    , course_passed_counts.num_of_course_passed
    , orders.coupon_code
    , orders.coupon_id
    , orders.coupon_name
    , orders.coupon_type
    , orders.discount
    , orders.order_total_price_paid_plus_tax
    , orders.order_created_on
    , orders.order_state
    , orders.receipt_authorization_code
    , orders.receipt_bill_to_address_state
    , orders.receipt_bill_to_address_country
    , orders.receipt_payment_method
    , orders.receipt_payment_amount
    , orders.receipt_payment_currency
    , orders.receipt_payment_card_number
    , orders.receipt_payment_card_type
    , orders.receipt_payment_transaction_type
    , orders.receipt_payment_transaction_uuid
    , orders.receipt_payer_name
    , orders.receipt_payer_email
    , orders.receipt_payer_ip_address
    , orders.receipt_payment_timestamp
    , orders.receipt_transaction_id
    , orders.unit_price
    , programs.program_name
    , lower(enrollments.user_email) as user_email
    , if(
        enrollments.platform = 'MITx Online'
        , concat('https://mitxonline.mit.edu/orders/receipt/', cast(orders.order_id as varchar))
        , null
    ) as receipt_url
from enrollments
left join orders
    on enrollments.order_id = orders.order_id and enrollments.platform = orders.platform
left join course_passed_counts
    on enrollments.user_email = course_passed_counts.user_email
left join programs
    on
        enrollments.platform = programs.platform_name
        and enrollments.course_readable_id = programs.course_readable_id
        and enrollments.user_email = programs.user_email
