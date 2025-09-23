-- Course Runs information for MITx Online
with
    runs as (select * from {{ ref("stg__mitxonline__app__postgres__courses_courserun") }}),
    courses as (select * from {{ ref("stg__mitxonline__app__postgres__courses_course") }})

select
    runs.courserun_id,
    runs.course_id,
    courses.course_number,
    runs.courserun_title,
    runs.courserun_readable_id,
    runs.courserun_edx_readable_id,
    runs.courserun_tag,
    runs.courserun_url,
    runs.courserun_start_on,
    runs.courserun_end_on,
    runs.courserun_enrollment_start_on,
    runs.courserun_enrollment_end_on,
    runs.courserun_upgrade_deadline,
    runs.courserun_is_self_paced,
    runs.courserun_is_live,
    runs.courserun_platform
from runs
inner join courses on runs.course_id = courses.course_id
