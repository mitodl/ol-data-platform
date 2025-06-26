-- Course Runs information for MITxPro

with course_runs as (
    select * from {{ ref('stg__mitxpro__app__postgres__courses_courserun') }}
)

, courses as (
    select * from {{ ref('stg__mitxpro__app__postgres__courses_course') }}
)

, platforms as (
    select * from {{ ref('stg__mitxpro__app__postgres__courses_platform') }}
)

select
    course_runs.courserun_id
    , course_runs.course_id
    , course_runs.courserun_title
    , course_runs.courserun_readable_id
    , course_runs.courserun_edx_readable_id
    , course_runs.courserun_external_readable_id
    , course_runs.courserun_tag
    , course_runs.courserun_url
    , course_runs.courserun_start_on
    , course_runs.courserun_end_on
    , course_runs.courserun_enrollment_start_on
    , course_runs.courserun_enrollment_end_on
    , course_runs.courserun_is_live
    , platforms.platform_name
    , platforms.platform_id
from course_runs
inner join courses
    on course_runs.course_id = courses.course_id
left join platforms
    on courses.platform_id = platforms.platform_id
