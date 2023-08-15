with edx_enrollments as (
    select *
    from {{ ref('int__mitx__courserun_enrollments') }}
    where platform = '{{ var("edxorg") }}'
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
        , edx_enrollments.user_id
        , edx_enrollments.user_email
        , edx_enrollments.user_full_name
        , edx_enrollments.user_username
        , edx_enrollments.user_edxorg_username
        , edx_enrollments.user_mitxonline_username
        , edx_enrollments.user_address_country
        , program_requirements.micromasters_program_id
        , program_requirements.mitxonline_program_id
        , program_requirements.program_title
    from edx_enrollments
    inner join program_requirements on edx_enrollments.course_number = program_requirements.course_number
)

, mitxonline_query as (
    select
        courserunenrollment_platform as platform
        , courserunenrollment_is_active
        , courserunenrollment_created_on
        , courserunenrollment_enrollment_mode
        , courserunenrollment_enrollment_status
        , courserun_id
        , courserun_title
        , courserun_readable_id
        , course_number
        , user_id
        , user_email
        , user_full_name
        , user_username
        , user_edxorg_username
        , user_username as user_mitxonline_username
        , user_address_country
        , micromasters_program_id
        , mitxonline_program_id
        , program_title
    from mitxonline_enrollments_with_program
    where courserunenrollment_platform = '{{ var("mitxonline") }}'
)

select *
from edx_query
union all
select *
from mitxonline_query
