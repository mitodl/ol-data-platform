-- Enrollment information for MITx Online
-- For DEDP courses that run on MITx Online, enrollments are verified via their purchased orders either in MITx Online
-- or MicroMasters. We migrated some DEDP orders from MicroMasters for those who have accounts on MITx Online, but due
-- to migration timing issue (orders could be modified on MM after migration), we should use MM orders to verify
-- enrollments for DEDP runs in '3T2021', '1T2022', '2T2022'

with enrollments as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_courserunenrollment') }}
)

, mitxonline_users as (
    select * from {{ ref('int__mitxonline__users') }}
)

, mitxonline_runs as (
    select * from {{ ref('int__mitxonline__course_runs') }}
)

, mitxonline_programs as (
    select * from {{ ref('int__mitxonline__program_requirements') }}
)

, mitxonline_orders as (
    select * from {{ ref('int__mitxonline__ecommerce_order') }}
)

, micromasters_orders as (
    select * from {{ ref('stg__micromasters__app__postgres__ecommerce_order') }}
)

, micromasters_lines as (
    select * from {{ ref('stg__micromasters__app__postgres__ecommerce_line') }}
)

, micromasters_users as (
    select * from {{ ref('__micromasters__users') }}
)

, dedp_enrollments_verified_in_micromasters as (
    select distinct
        enrollments.user_id
        , enrollments.courserun_id
    from enrollments
    inner join mitxonline_runs on enrollments.courserun_id = mitxonline_runs.courserun_id
    inner join mitxonline_programs on mitxonline_runs.course_id = mitxonline_programs.course_id
    inner join mitxonline_users on enrollments.user_id = mitxonline_users.user_id
    inner join micromasters_users
        on mitxonline_users.user_micromasters_profile_id = micromasters_users.user_profile_id
    inner join micromasters_orders on micromasters_users.user_id = micromasters_orders.user_id
    inner join micromasters_lines
        on
            mitxonline_runs.courserun_readable_id = micromasters_lines.courserun_readable_id
            and micromasters_orders.order_id = micromasters_lines.order_id
    where
        mitxonline_runs.courserun_tag in ('2T2022', '1T2022', '3T2021')
        and mitxonline_programs.program_id in (
            {{ var("dedp_mitxonline_international_development_program_id") }}
            , {{ var("dedp_mitxonline_public_policy_program_id") }}
        )
        and micromasters_orders.order_state = 'fulfilled'
)

, dedp_enrollments_verified_in_mitxonline as (
    select distinct
        enrollments.user_id
        , enrollments.courserun_id
    from enrollments
    inner join mitxonline_orders
        on
            enrollments.user_id = mitxonline_orders.user_id
            and enrollments.courserun_id = mitxonline_orders.courserun_id
    inner join mitxonline_runs on enrollments.courserun_id = mitxonline_runs.courserun_id
    inner join mitxonline_programs on mitxonline_runs.course_id = mitxonline_programs.course_id
    where
        mitxonline_runs.courserun_tag not in ('2T2022', '1T2022', '3T2021')
        and mitxonline_programs.program_id in (
            {{ var("dedp_mitxonline_international_development_program_id") }}
            , {{ var("dedp_mitxonline_public_policy_program_id") }}
        )
        and mitxonline_orders.order_state = 'fulfilled'

)

, dedp_enrollments_verified as (
    select
        user_id
        , courserun_id
    from dedp_enrollments_verified_in_micromasters

    union distinct

    select
        user_id
        , courserun_id
    from dedp_enrollments_verified_in_mitxonline
)

, mitxonline_enrollments as (
    select
        enrollments.courserunenrollment_id
        , enrollments.courserunenrollment_is_active
        , enrollments.user_id
        , enrollments.courserun_id
        , enrollments.courserunenrollment_created_on
        , enrollments.courserunenrollment_enrollment_status
        , enrollments.courserunenrollment_is_edx_enrolled
        , mitxonline_runs.courserun_platform as courserunenrollment_platform
        , mitxonline_runs.courserun_title
        , mitxonline_runs.courserun_readable_id
        , mitxonline_runs.course_number
        , mitxonline_runs.course_id
        , mitxonline_runs.courserun_start_on
        , mitxonline_runs.courserun_upgrade_deadline
        , mitxonline_users.user_username
        , mitxonline_users.user_email
        , mitxonline_users.user_edxorg_username
        , mitxonline_users.user_full_name
        , mitxonline_users.user_address_country
        , case
            when
                dedp_enrollments_verified.user_id is not null
                and dedp_enrollments_verified.courserun_id is not null
                and enrollments.courserunenrollment_enrollment_status is null
                then 'verified'
            else enrollments.courserunenrollment_enrollment_mode
        end as courserunenrollment_enrollment_mode
    from enrollments
    inner join mitxonline_runs on enrollments.courserun_id = mitxonline_runs.courserun_id
    inner join mitxonline_users on enrollments.user_id = mitxonline_users.user_id
    left join dedp_enrollments_verified
        on
            enrollments.user_id = dedp_enrollments_verified.user_id
            and enrollments.courserun_id = dedp_enrollments_verified.courserun_id

)

select *
from mitxonline_enrollments
