with course_certificates as (
    select * from {{ ref('int__micromasters__course_certificates') }}
)

, grades as (
    select *
    from {{ ref('int__micromasters__course_grades') }}
)

select
    course_certificates.program_title
    , course_certificates.mitxonline_program_id
    , course_certificates.micromasters_program_id
    , course_certificates.courserun_title
    , course_certificates.courserun_readable_id
    , course_certificates.courserun_platform
    , course_certificates.course_number
    , course_certificates.user_edxorg_username
    , course_certificates.user_mitxonline_username
    , course_certificates.user_full_name
    , course_certificates.user_country
    , course_certificates.user_email
    , course_certificates.courseruncertificate_url
    , course_certificates.courseruncertificate_created_on
    , grades.grade
    , grades.is_passing
from course_certificates
left join grades
    on
        course_certificates.user_email = grades.user_email
        and course_certificates.courserun_readable_id = grades.courserun_readable_id
        and
        (
            course_certificates.mitxonline_program_id = grades.mitxonline_program_id
            or course_certificates.micromasters_program_id = grades.micromasters_program_id
        )
