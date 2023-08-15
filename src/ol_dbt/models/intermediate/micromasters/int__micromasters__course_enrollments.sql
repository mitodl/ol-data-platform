with mitx_enrollments as (
    select * from {{ ref('int__mitx__courserun_enrollments_with_programs') }}
)

, programs as (
    select * from {{ ref('int__mitx__programs') }}

)

select
    mitx_enrollments.program_title
    , mitx_enrollments.micromasters_program_id
    , mitx_enrollments.mitxonline_program_id
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
inner join programs
    on
        mitx_enrollments.micromasters_program_id = programs.micromasters_program_id
        or mitx_enrollments.mitxonline_program_id = programs.mitxonline_program_id
where programs.is_micromasters_program = true
