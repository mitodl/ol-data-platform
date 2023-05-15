with edxorg_enrollments as (
    select * from {{ ref('__micromasters_course_enrollments_from_edxorg') }}
)

, mitxonline_enrollments as (
    select * from {{ ref('__micromasters_course_enrollments_from_mitxonline') }}
)


select
    program_title
    , user_id
    , user_username
    , user_country
    , user_email
    , user_full_name
    , courserun_readable_id
    , course_number
    , platform
    , courserunenrollment_created_on
    , courserunenrollment_is_active
    , courserun_title
from edxorg_enrollments
union all
select
    program_title
    , user_id
    , user_username
    , user_country
    , user_email
    , user_full_name
    , courserun_readable_id
    , course_number
    , platform
    , courserunenrollment_created_on
    , courserunenrollment_is_active
    , courserun_title
from mitxonline_enrollments
