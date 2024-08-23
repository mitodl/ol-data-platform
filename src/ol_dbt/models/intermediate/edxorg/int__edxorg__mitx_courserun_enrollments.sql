-- Course Run Enrollment information from edx.org
-- For DEDP courses that run on edX.org, enrollments are verified via their purchased orders in MicroMasters

with person_courses as (
    select
        user_id
        , user_username
        , courserun_readable_id
        , courserunenrollment_created_on
        , courserunenrollment_enrollment_mode
        , courserunenrollment_is_active
    from {{ ref('stg__edxorg__bigquery__mitx_person_course') }}
    where courserun_platform = '{{ var("edxorg") }}'
)

, user_info_combo as (
    select
        user_id
        , courserunenrollment_courserun_readable_id
    from {{ ref('stg__edxorg__bigquery__mitx_user_info_combo') }}
    where
        courserun_platform = '{{ var("edxorg") }}'
        and courserunenrollment_courserun_readable_id is not null
)

, micromasters_runs as (
    select * from {{ ref('stg__micromasters__app__postgres__courses_courserun') }}
)

, edxorg_enrollments as (
    select
        person_courses.user_id
        , person_courses.user_username
        , person_courses.courserun_readable_id
        , person_courses.courserunenrollment_created_on
        , person_courses.courserunenrollment_enrollment_mode
        , person_courses.courserunenrollment_is_active
        , micromasters_runs.courserun_upgrade_deadline
    from
        person_courses
    inner join user_info_combo
        on
            person_courses.user_id = user_info_combo.user_id
            and person_courses.courserun_readable_id = user_info_combo.courserunenrollment_courserun_readable_id
    left join micromasters_runs
        on person_courses.courserun_readable_id = micromasters_runs.courserun_readable_id
)

, edxorg_runs as (
    select *
    from {{ ref('stg__edxorg__bigquery__mitx_courserun') }}
)

, edxorg_users as (
    select * from {{ ref('int__edxorg__mitx_users') }}
)

, micromasters_orders as (
    select * from {{ ref('stg__micromasters__app__postgres__ecommerce_order') }}
)

, micromasters_lines as (
    select * from {{ ref('stg__micromasters__app__postgres__ecommerce_line') }}
)

, micromasters_courses as (
    select * from {{ ref('stg__micromasters__app__postgres__courses_course') }}
)

, micromasters_users as (
    select * from {{ ref('__micromasters__users') }}
)

, dedp_edxorg_enrollments_verified as (
    select distinct
        edxorg_enrollments.user_id
        , edxorg_enrollments.courserun_readable_id
    from edxorg_enrollments
    inner join edxorg_users on edxorg_enrollments.user_id = edxorg_users.user_id
    inner join micromasters_users on edxorg_users.user_username = micromasters_users.user_edxorg_username
    inner join micromasters_orders on micromasters_users.user_id = micromasters_orders.user_id
    inner join micromasters_lines
        on
            edxorg_enrollments.courserun_readable_id = micromasters_lines.courserun_edxorg_readable_id
            and micromasters_orders.order_id = micromasters_lines.order_id
    inner join micromasters_runs on micromasters_lines.courserun_readable_id = micromasters_runs.courserun_readable_id
    inner join micromasters_courses on micromasters_runs.course_id = micromasters_courses.course_id
    where
        micromasters_courses.program_id = {{ var("dedp_micromasters_program_id") }}
        and micromasters_orders.order_state = 'fulfilled'
)

, enrollments as (
    select
        edxorg_enrollments.courserun_readable_id
        , edxorg_runs.course_number
        , edxorg_enrollments.courserunenrollment_created_on
        , edxorg_enrollments.courserunenrollment_is_active
        , edxorg_users.user_id
        , edxorg_users.user_email
        , edxorg_users.user_username
        , micromasters_users.user_mitxonline_username
        , edxorg_runs.courserun_title
        , edxorg_runs.courserun_start_date as courserun_start_on
        , edxorg_enrollments.courserun_upgrade_deadline
        , edxorg_users.user_country as user_address_country
        , coalesce(edxorg_users.user_full_name, micromasters_users.user_full_name) as user_full_name
        , case
            when
                dedp_edxorg_enrollments_verified.user_id is not null
                and dedp_edxorg_enrollments_verified.courserun_readable_id is not null then 'verified'
            else edxorg_enrollments.courserunenrollment_enrollment_mode
        end as courserunenrollment_enrollment_mode
    from edxorg_enrollments
    inner join edxorg_users on edxorg_enrollments.user_id = edxorg_users.user_id
    left join micromasters_users on edxorg_users.user_username = micromasters_users.user_edxorg_username
    left join edxorg_runs on edxorg_enrollments.courserun_readable_id = edxorg_runs.courserun_readable_id
    left join dedp_edxorg_enrollments_verified
        on
            edxorg_enrollments.user_id = dedp_edxorg_enrollments_verified.user_id
            and edxorg_enrollments.courserun_readable_id = dedp_edxorg_enrollments_verified.courserun_readable_id
)

select * from enrollments
