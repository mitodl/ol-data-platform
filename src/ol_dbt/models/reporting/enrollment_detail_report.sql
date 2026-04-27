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

, user_course_roles as (
    select * from {{ ref('int__combined__user_course_roles') }}
)

, b2b_contract_to_courseruns as (
    select * from {{ ref('int__mitxonline__b2b_contract_to_courseruns') }}
)

, org_field as (
    select
        distinct courserun_readable_id
        , organization
    from user_course_roles
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
    from enrollments
    where courserungrade_is_passing = true
    group by user_email
)

select
    enrollments.platform
    , enrollments.courserunenrollment_id
    , enrollments.course_readable_id
    , enrollments.course_title
    , enrollments.courserun_is_current
    , enrollments.courserun_readable_id
    , enrollments.courserun_start_on
    , enrollments.courserun_end_on
    , enrollments.courserun_title
    , enrollments.courserunenrollment_created_on
    , enrollments.courserunenrollment_enrollment_mode
    , enrollments.courserunenrollment_enrollment_status
    , enrollments.courserunenrollment_is_active
    , enrollments.courserunenrollment_upgraded_on
    , enrollments.courseruncertificate_created_on
    , enrollments.courseruncertificate_is_earned
    , enrollments.courseruncertificate_url
    , enrollments.courserungrade_grade
    , enrollments.courserungrade_is_passing
    , coalesce(b2b_contract_to_courseruns.organization_key, org_field.organization) as organization_key
    , b2b_contract_to_courseruns.organization_name
    , enrollments.user_country_code
    , enrollments.user_highest_education
    , enrollments.user_full_name
    , enrollments.user_username
    , lower(enrollments.user_email) as user_email
    , course_passed_counts.num_of_course_passed
    , orders.coupon_code
    , orders.coupon_name
    , orders.discount
    , enrollments.order_id
    , enrollments.order_reference_number
    , orders.order_created_on
    , orders.order_state
    , orders.receipt_payment_method
    , orders.receipt_payment_amount
    , orders.receipt_payer_email
    , orders.receipt_payment_timestamp
    , orders.unit_price
    , programs.program_name
    , orders.order_type
    , orders.redeemed_email
    , if(
        enrollments.platform = '{{ var("mitxonline") }}'
        , concat('https://mitxonline.mit.edu/orders/receipt/', cast(orders.order_id as varchar))
        , null
    ) as receipt_url
from enrollments
left join orders
    on enrollments.order_id = orders.order_id
    and enrollments.line_id = orders.line_id
    and enrollments.platform = orders.platform
left join course_passed_counts
    on enrollments.user_email = course_passed_counts.user_email
left join programs
    on
        enrollments.platform = programs.platform_name
        and enrollments.course_readable_id = programs.course_readable_id
        and enrollments.user_email = programs.user_email
left join org_field
    on enrollments.courserun_readable_id = org_field.courserun_readable_id
left join b2b_contract_to_courseruns
    on enrollments.courserun_readable_id = b2b_contract_to_courseruns.courserun_readable_id
