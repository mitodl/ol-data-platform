with mitx_enrollments as (
    select * from {{ ref('int__mitx__courserun_enrollments') }}
)

, program_requirements as (
    select * from {{ ref('int__mitx__program_requirements') }}
)

select
    program_requirements.program_title
    , program_requirements.micromasters_program_id
    , mitx_enrollments.user_id
    , mitx_enrollments.user_mitxonline_username
    , mitx_enrollments.user_edxorg_username
    , mitx_enrollments.user_address_country
    , mitx_enrollments.user_email
    , mitx_enrollments.user_full_name
    , mitx_enrollments.courserun_readable_id
    , mitx_enrollments.course_number
    , mitx_enrollments.platform
    , mitx_enrollments.courserunenrollment_created_on
    , mitx_enrollments.courserunenrollment_is_active
    , mitx_enrollments.courserun_title
    , mitx_enrollments.courserunenrollment_enrollment_mode
from mitx_enrollments
inner join program_requirements
    on program_requirements.course_number = mitx_enrollments.course_number
where program_requirements.micromasters_program_id is not null
