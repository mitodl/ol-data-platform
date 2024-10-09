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

, bootcamps_completed_orders as (
    select * from {{ ref('int__bootcamps__ecommerce_order') }}
    where order_state in ('fulfilled', 'refunded')
)

, mitxpro__ecommerce_line as (
    select * from {{ ref('int__mitxpro__ecommerce_line') }}
)

, combined_enrollment_detail as (
    select
        '{{ var("mitxonline") }}' as platform
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
        , combined_users.user_gender
        , combined_enrollments.courseruncertificate_is_earned
        , combined_enrollments.courseruncertificate_created_on
        , combined_enrollments.courseruncertificate_url
        , combined_enrollments.courseruncertificate_uuid
        , mitxonline_completed_orders.order_id
        , mitxonline_completed_orders.line_id
        , mitxonline_completed_orders.order_reference_number
        , combined_enrollments.courserungrade_grade
        , combined_enrollments.courserungrade_is_passing
        , combined_enrollments.course_title
        , combined_enrollments.course_readable_id
        , combined_enrollments.courserun_upgrade_deadline
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
        on
            combined_enrollments.courserun_readable_id = combined_courseruns.courserun_readable_id
            and combined_enrollments.platform = combined_courseruns.platform
    where combined_enrollments.platform = '{{ var("mitxonline") }}'

    union all

    select
        '{{ var("edxorg") }}' as platform
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
        , combined_users.user_gender
        , combined_enrollments.courseruncertificate_is_earned
        , combined_enrollments.courseruncertificate_created_on
        , combined_enrollments.courseruncertificate_url
        , combined_enrollments.courseruncertificate_uuid
        , micromasters_completed_orders.order_id
        , micromasters_completed_orders.line_id
        , micromasters_completed_orders.order_reference_number
        , combined_enrollments.courserungrade_grade
        , combined_enrollments.courserungrade_is_passing
        , combined_enrollments.course_title
        , combined_enrollments.course_readable_id
        , combined_enrollments.courserun_upgrade_deadline
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
        on
            combined_enrollments.courserun_readable_id = combined_courseruns.courserun_readable_id
            and combined_enrollments.platform = combined_courseruns.platform
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
        , combined_users.user_gender
        , combined_enrollments.courseruncertificate_is_earned
        , combined_enrollments.courseruncertificate_created_on
        , combined_enrollments.courseruncertificate_url
        , combined_enrollments.courseruncertificate_uuid
        , mitxpro_completed_orders.order_id
        , mitxpro__ecommerce_line.line_id
        , mitxpro_completed_orders.receipt_reference_number as order_reference_number
        , combined_enrollments.courserungrade_grade
        , combined_enrollments.courserungrade_is_passing
        , combined_enrollments.course_title
        , combined_enrollments.course_readable_id
        , combined_enrollments.courserun_upgrade_deadline
    from mitxpro_enrollments
    inner join combined_enrollments
        on mitxpro_enrollments.courserunenrollment_id = combined_enrollments.courserunenrollment_id
    left join combined_users
        on
            mitxpro_enrollments.user_username = combined_users.user_username
            and combined_enrollments.platform = combined_users.platform
    left join mitxpro_completed_orders
        on mitxpro_enrollments.ecommerce_order_id = mitxpro_completed_orders.order_id
    left join combined_courseruns
        on mitxpro_enrollments.courserun_readable_id = combined_courseruns.courserun_readable_id
    left join mitxpro__ecommerce_line
        on mitxpro_completed_orders.order_id = mitxpro__ecommerce_line.order_id
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
        , combined_users.user_gender
        , combined_enrollments.courseruncertificate_is_earned
        , combined_enrollments.courseruncertificate_created_on
        , combined_enrollments.courseruncertificate_url
        , combined_enrollments.courseruncertificate_uuid
        , bootcamps_completed_orders.order_id
        , bootcamps_completed_orders.line_id
        , bootcamps_completed_orders.order_reference_number
        , combined_enrollments.courserungrade_grade
        , combined_enrollments.courserungrade_is_passing
        , combined_enrollments.course_title
        , combined_enrollments.course_readable_id
        , combined_enrollments.courserun_upgrade_deadline
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
        on
            combined_enrollments.courserun_readable_id = combined_courseruns.courserun_readable_id
            and combined_enrollments.platform = combined_courseruns.platform
    where combined_enrollments.platform = '{{ var("bootcamps") }}'

    union all

    select
        '{{ var("residential") }}' as platform
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
        , combined_users.user_gender
        , combined_enrollments.courseruncertificate_is_earned
        , combined_enrollments.courseruncertificate_created_on
        , combined_enrollments.courseruncertificate_url
        , combined_enrollments.courseruncertificate_uuid
        , null as order_id
        , null as line_id
        , null as order_reference_number
        , combined_enrollments.courserungrade_grade
        , combined_enrollments.courserungrade_is_passing
        , combined_enrollments.course_title
        , combined_enrollments.course_readable_id
        , combined_enrollments.courserun_upgrade_deadline
    from combined_enrollments
    left join combined_users
        on
            combined_enrollments.user_username = combined_users.user_username
            and combined_enrollments.platform = combined_users.platform
    left join combined_courseruns
        on
            combined_enrollments.courserun_readable_id = combined_courseruns.courserun_readable_id
            and combined_enrollments.platform = combined_courseruns.platform
    where combined_enrollments.platform = '{{ var("residential") }}'
)

select
    platform
    , courserunenrollment_id
    , {{ generate_hash_id('cast(order_id as varchar)
        || cast(coalesce(line_id, 9) as varchar)
        || platform') }} as combined_orders_hash_id
    , course_readable_id
    , course_title
    , courserun_id
    , courserun_is_current
    , courserun_readable_id
    , courserun_start_on
    , courserun_end_on
    , courserun_title
    , courserun_upgrade_deadline
    , courseruncertificate_created_on
    , courseruncertificate_is_earned
    , courseruncertificate_url
    , courseruncertificate_uuid
    , courserunenrollment_created_on
    , courserunenrollment_enrollment_mode
    , courserunenrollment_enrollment_status
    , courserunenrollment_is_active
    , courserunenrollment_is_edx_enrolled
    , courserungrade_grade
    , courserungrade_is_passing
    , line_id
    , order_id
    , order_reference_number
    , user_company
    , user_country_code
    , user_email
    , user_full_name
    , user_highest_education
    , user_gender
    , {{ generate_hash_id('cast(user_id as varchar) || platform') }} as user_hashed_id
    , user_id
    , user_username
from combined_enrollment_detail
