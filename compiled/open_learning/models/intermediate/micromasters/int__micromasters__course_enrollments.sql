with edxorg_enrollments as (
    select * from dev.main_intermediate.__micromasters_course_enrollments_from_edxorg
)

, mitxonline_enrollments as (
    select * from dev.main_intermediate.__micromasters_course_enrollments_from_mitxonline
)


select
    program_title
    , user_id
    , user_username
    , user_country
    , user_email
    , courserun_readable_id
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
    , courserun_readable_id
    , platform
    , courserunenrollment_created_on
    , courserunenrollment_is_active
    , courserun_title
from mitxonline_enrollments
