with combined_enrollments as (
    select * from {{ ref('int__combined__courserun_enrollments') }}
)

, combined_courseruns as (
    select * from {{ ref('int__combined__course_runs') }}
)

, combined_users as (
    select * from {{ ref('int__combined__users') }}
)

, mitx__users as (
    select * from {{ ref('int__mitx__users') }}
)

, mitxonline__course_runs as (
    select * from {{ ref('int__mitxonline__course_runs') }}
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

, mitxonline_certificates as (
    select * from {{ ref('int__mitxonline__courserun_certificates') }}
)

, mitxonline_enrollment as (
    select
        combined_enrollments.courserun_readable_id
        , combined_enrollments.user_email
        , combined_enrollments.course_readable_id
    from combined_enrollments
    where combined_enrollments.platform = '{{ var("mitxonline") }}'
    group by 
        combined_enrollments.courserun_readable_id
        , combined_enrollments.user_email
        , combined_enrollments.course_readable_id
)

, edxorg_enrollment as (
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
)

select
    edxorg_enrollment.user_id as user_edxorg_id
    , mitx__users.user_mitxonline_id
    , edxorg_enrollment.user_email
    , mitxonline__course_runs.courserun_id
    , {{ format_course_id('edxorg_enrollment.courserun_readable_id', false) }} as courserun_readable_id
    , edxorg_enrollment.courserunenrollment_enrollment_mode
    , edxorg_enrollment.courserungrade_grade
    , edxorg_enrollment.courserungrade_is_passing
    , edxorg_enrollment.courserunenrollment_created_on
    , edxorg_enrollment.courseruncertificate_created_on
from edxorg_enrollment
left join mitxonline_enrollment
    on 
        edxorg_enrollment.user_email = mitxonline_enrollment.user_email
        and edxorg_enrollment.course_readable_id = mitxonline_enrollment.course_readable_id
        and substring(edxorg_enrollment.courserun_readable_id, length(edxorg_enrollment.courserun_readable_id) - 5) 
            = substring(mitxonline_enrollment.courserun_readable_id, length(mitxonline_enrollment.courserun_readable_id) - 5)
left join mitx__users
    on edxorg_enrollment.user_id = mitx__users.user_edxorg_id
left join mitxonline__course_runs
    on edxorg_enrollment.courserun_readable_id = mitxonline__course_runs.courserun_edx_readable_id
where 
    edxorg_enrollment.courseruncertificate_created_on is not null
    and mitxonline_enrollment.user_email is null