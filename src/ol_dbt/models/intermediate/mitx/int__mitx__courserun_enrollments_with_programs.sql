with edx_enrollments as (
    select *
    from {{ ref('int__mitx__courserun_enrollments') }}
    where platform = '{{ var("edxorg") }}'
)

, mitx_users as (
    select * from {{ ref('int__mitx__users') }}
)

, mitxonline_enrollments_with_program as (
    select *
    from {{ ref('int__mitxonline__courserunenrollments_with_programs') }}
)


, program_requirements as (
    select * from {{ ref('int__mitx__program_requirements') }}
)

, edx_query as (
    select
        edx_enrollments.platform
        , edx_enrollments.courserunenrollment_is_active
        , edx_enrollments.courserunenrollment_created_on
        , edx_enrollments.courserunenrollment_enrollment_mode
        , edx_enrollments.courserunenrollment_enrollment_status
        , edx_enrollments.courserun_id
        , edx_enrollments.courserun_title
        , edx_enrollments.courserun_readable_id
        , edx_enrollments.course_number
        , mitx_users.user_edxorg_id as user_id
        , mitx_users.user_edxorg_email as user_email
        , mitx_users.user_full_name
        , mitx_users.user_edxorg_username as user_username
        , mitx_users.user_edxorg_username
        , mitx_users.user_mitxonline_username
        , mitx_users.user_address_country
        , program_requirements.micromasters_program_id
        , program_requirements.mitxonline_program_id
        , program_requirements.program_title
    from edx_enrollments
    left join mitx_users
        on edx_enrollments.user_id = mitx_users.user_edxorg_id
    inner join program_requirements on edx_enrollments.course_number = program_requirements.course_number
)

, mitxonline_query as (
    select
        mitxonline_enrollments_with_program.courserunenrollment_platform as platform
        , mitxonline_enrollments_with_program.courserunenrollment_is_active
        , mitxonline_enrollments_with_program.courserunenrollment_created_on
        , mitxonline_enrollments_with_program.courserunenrollment_enrollment_mode
        , mitxonline_enrollments_with_program.courserunenrollment_enrollment_status
        , mitxonline_enrollments_with_program.courserun_id
        , mitxonline_enrollments_with_program.courserun_title
        , mitxonline_enrollments_with_program.courserun_readable_id
        , mitxonline_enrollments_with_program.course_number
        , mitx_users.user_mitxonline_id as user_id
        , mitx_users.user_mitxonline_email as user_email
        , mitx_users.user_full_name
        , mitx_users.user_mitxonline_username as user_username
        , mitx_users.user_edxorg_username
        , mitx_users.user_mitxonline_username
        , mitx_users.user_address_country
        , mitxonline_enrollments_with_program.micromasters_program_id
        , mitxonline_enrollments_with_program.mitxonline_program_id
        , mitxonline_enrollments_with_program.program_title
    from mitxonline_enrollments_with_program
    left join mitx_users
        on mitxonline_enrollments_with_program.user_id = mitx_users.user_mitxonline_id
    where mitxonline_enrollments_with_program.courserunenrollment_platform = '{{ var("mitxonline") }}'
)

select *
from edx_query
union all
select *
from mitxonline_query
