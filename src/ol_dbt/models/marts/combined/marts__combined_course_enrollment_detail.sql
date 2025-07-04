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

, mitxonline_transactions as (
    select
        order_id
        , max(transaction_timestamp) as payment_timestamp
    from {{ ref('int__mitxonline__ecommerce_transaction') }}
    where transaction_type= 'payment'
    group by order_id
)

, mitxonline_completed_orders as (
    select
        *
        , row_number() over (
            partition by user_id, courserun_id
            order by order_created_on desc
        ) as row_num
    from {{ ref('int__mitxonline__ecommerce_order') }}
    where order_state in ('fulfilled', 'refunded')
)

, micromasters_completed_orders as (
    select
        *
        , row_number() over (
            partition by user_id, courserun_readable_id
            order by order_created_on desc, order_id desc
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

, mitxpro_receipts as (
    select
        order_id
        , max(receipt_payment_timestamp) as payment_timestamp
    from {{ ref('int__mitxpro__ecommerce_receipt') }}
    where receipt_transaction_status != 'ERROR'
    group by order_id
)

, bootcamps_completed_orders as (
    select * from {{ ref('int__bootcamps__ecommerce_order') }}
    where order_state in ('fulfilled', 'refunded')
)

, bootcamps_receipts as (
    select
        order_id
        , max(receipt_payment_timestamp) as receipt_payment_timestamp
    from {{ ref('int__bootcamps__ecommerce_receipt') }}
    group by order_id
)

, mitxpro__ecommerce_line as (
    select * from {{ ref('int__mitxpro__ecommerce_line') }}
)

, mitxonline_certificates as (
    select * from {{ ref('int__mitxonline__courserun_certificates') }}
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
        , mitxonline_completed_orders.discount_code as coupon_code
        , combined_enrollments.courserungrade_grade
        , combined_enrollments.courserungrade_is_passing
        , combined_enrollments.course_title
        , combined_enrollments.course_readable_id
        , combined_enrollments.courserun_upgrade_deadline
        , if(mitxonline_completed_orders.order_id is not null
            , coalesce(mitxonline_transactions.payment_timestamp, mitxonline_completed_orders.order_created_on), null)
        as courserunenrollment_upgraded_on
    from combined_enrollments
    left join combined_users
        on
            combined_enrollments.user_username = combined_users.user_username
            and combined_enrollments.platform = combined_users.platform
    left join mitxonline_completed_orders
        on
            combined_enrollments.user_username = mitxonline_completed_orders.user_username
            and combined_enrollments.courserun_id = mitxonline_completed_orders.courserun_id
            and mitxonline_completed_orders.row_num = 1
    left join combined_courseruns
        on
            combined_enrollments.courserun_readable_id = combined_courseruns.courserun_readable_id
            and combined_enrollments.platform = combined_courseruns.platform
    left join mitxonline_transactions
        on mitxonline_completed_orders.order_id = mitxonline_transactions.order_id
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
        , case
            when mitxonline_certificates.courseruncertificate_is_revoked = false then true
            when combined_enrollments.courseruncertificate_created_on is not null then true
            else false
        end as courseruncertificate_is_earned
        , coalesce(
            mitxonline_certificates.courseruncertificate_created_on
            , combined_enrollments.courseruncertificate_created_on
        ) as courseruncertificate_created_on
        , coalesce(
            mitxonline_certificates.courseruncertificate_url
            , combined_enrollments.courseruncertificate_url
        ) as courseruncertificate_url
        , coalesce(
            mitxonline_certificates.courseruncertificate_uuid
            , combined_enrollments.courseruncertificate_uuid
        ) as courseruncertificate_uuid
        , micromasters_completed_orders.order_id
        , micromasters_completed_orders.line_id
        , micromasters_completed_orders.order_reference_number
        , micromasters_completed_orders.coupon_code
        , combined_enrollments.courserungrade_grade
        , combined_enrollments.courserungrade_is_passing
        , combined_enrollments.course_title
        , combined_enrollments.course_readable_id
        , combined_enrollments.courserun_upgrade_deadline
        , if(micromasters_completed_orders.order_id is not null
            , coalesce(micromasters_completed_orders.receipt_payment_timestamp, micromasters_completed_orders.order_created_on)
            , null
        ) as courserunenrollment_upgraded_on
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
    left join mitxonline_certificates
        on (
            micromasters_users.user_mitxonline_username = mitxonline_certificates.user_username
            or micromasters_users.user_email = mitxonline_certificates.user_email
        )
        and combined_enrollments.courserun_readable_id
        = replace(replace(mitxonline_certificates.courserun_readable_id, 'course-v1:', ''), '+', '/')
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
        , mitxpro_completed_orders.coupon_code
        , combined_enrollments.courserungrade_grade
        , combined_enrollments.courserungrade_is_passing
        , combined_enrollments.course_title
        , combined_enrollments.course_readable_id
        , combined_enrollments.courserun_upgrade_deadline
        , if(mitxpro_completed_orders.order_id is not null
            , coalesce(mitxpro_receipts.payment_timestamp, mitxpro_completed_orders.order_created_on)
            , null
        ) as courserunenrollment_upgraded_on
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
    left join mitxpro_receipts
        on mitxpro_completed_orders.order_id = mitxpro_receipts.order_id
    where combined_enrollments.platform = '{{ var("mitxpro") }}'

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
        , null as order_id
        , null as line_id
        , null as order_reference_number
        , null as coupon_code
        , combined_enrollments.courserungrade_grade
        , combined_enrollments.courserungrade_is_passing
        , combined_enrollments.course_title
        , combined_enrollments.course_readable_id
        , combined_enrollments.courserun_upgrade_deadline
        , null as courserunenrollment_upgraded_on
    from combined_enrollments
    left join combined_users
        on
            combined_enrollments.user_id = combined_users.user_id
            and combined_enrollments.user_email = combined_users.user_email
            and combined_enrollments.platform = combined_users.platform
    left join combined_courseruns
        on combined_enrollments.courserun_readable_id = combined_courseruns.courserun_readable_id
    where combined_enrollments.platform in ('{{ var("emeritus") }}', '{{ var("global_alumni") }}')

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
        , null as coupon_code
        , combined_enrollments.courserungrade_grade
        , combined_enrollments.courserungrade_is_passing
        , combined_enrollments.course_title
        , combined_enrollments.course_readable_id
        , combined_enrollments.courserun_upgrade_deadline
        , if(bootcamps_completed_orders.order_id is not null
            , coalesce(bootcamps_receipts.receipt_payment_timestamp, bootcamps_completed_orders.order_created_on)
            , null
        ) as courserunenrollment_upgraded_on
    from combined_enrollments
    left join combined_users
        on
            combined_enrollments.user_username = combined_users.user_username
            and combined_enrollments.platform = combined_users.platform
    left join bootcamps_completed_orders
        on
            combined_enrollments.user_username = bootcamps_completed_orders.user_username
            and combined_enrollments.courserun_id = bootcamps_completed_orders.courserun_id
    left join combined_courseruns
        on
            combined_enrollments.courserun_readable_id = combined_courseruns.courserun_readable_id
            and combined_enrollments.platform = combined_courseruns.platform
    left join bootcamps_receipts
        on bootcamps_completed_orders.order_id = bootcamps_receipts.order_id
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
        , null as coupon_code
        , combined_enrollments.courserungrade_grade
        , combined_enrollments.courserungrade_is_passing
        , combined_enrollments.course_title
        , combined_enrollments.course_readable_id
        , combined_enrollments.courserun_upgrade_deadline
        , null as courserunenrollment_upgraded_on
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
    , courserunenrollment_upgraded_on
    , courserungrade_grade
    , courserungrade_is_passing
    , line_id
    , order_id
    , order_reference_number
    , coupon_code
    , user_company
    , user_country_code
    , user_email
    , user_full_name
    , user_highest_education
    , user_gender
    , case
        when user_id is not null
        then {{ generate_hash_id('user_id || platform') }}
        when user_email is not null
        then {{ generate_hash_id('user_email || platform') }}
        else
            {{ generate_hash_id('user_full_name || platform') }}
    end as user_hashed_id
    , user_id
    , user_username
from combined_enrollment_detail
