with courserun_certificates as (
    select * from {{ ref('int__mitxonline__courserun_certificates') }}
    --- to be consistent with data from MicroMasters and edX.org, we filter out revoked certificates
    where courseruncertificate_is_revoked = false
)

, courseruns as (
    select * from {{ ref('int__mitxonline__course_runs') }}
)

, courses as (
    select * from {{ ref('int__mitxonline__courses') }}
)

, enrollments_with_program as (
    select * from {{ ref('int__mitxonline__courserunenrollments_with_programs') }}
)

select
    enrollments_with_program.program_title
    , enrollments_with_program.micromasters_program_id
    , enrollments_with_program.mitxonline_program_id
    , courseruns.courserun_title
    , courseruns.courserun_readable_id
    , courseruns.courserun_platform
    , courses.course_number
    , courserun_certificates.user_username as user_mitxonline_username
    , courserun_certificates.user_id as user_mitxonline_id
    , courserun_certificates.courseruncertificate_uuid
    , courserun_certificates.courseruncertificate_url
    , courserun_certificates.courseruncertificate_created_on
    , courserun_certificates.courseruncertificate_updated_on
from courserun_certificates
inner join courseruns on courserun_certificates.courserun_id = courseruns.courserun_id
inner join courses on courserun_certificates.course_id = courses.course_id
inner join enrollments_with_program
    on
        courseruns.courserun_id = enrollments_with_program.courserun_id
        and courserun_certificates.user_id = enrollments_with_program.user_id
where enrollments_with_program.is_dedp_program = true
