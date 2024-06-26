-- MITx Course Runs from edx.org
---It also adds a field micromaster_program_id so that we could use it to get program requirements from MicroMaster


with runs as (
    select *
    from {{ ref('stg__edxorg__bigquery__mitx_courserun') }}
    where courserun_platform = '{{ var("edxorg") }}'
)

--- MicroMasters's course_edx_key can either be {org}+{course_number} or course-v1:{org}+{course_number}, so it
-- can't be directly used to link courses between edx and MM, it needs to be formatted as {org}/{course_number}
, micromasters_courses as (
    select
        course_id
        , program_id
        , course_edx_key
        , course_number
        , course_edx_key as course_readable_id
    from {{ ref('stg__micromasters__app__postgres__courses_course') }}
)

select
    runs.courserun_readable_id
    , runs.course_number
    , runs.course_readable_id
    , runs.courserun_title
    , runs.courserun_semester
    , runs.courserun_url
    , runs.courserun_institution
    , runs.courserun_instructors
    , runs.courserun_enrollment_start_date
    , runs.courserun_start_date
    , runs.courserun_end_date
    , runs.courserun_is_self_paced
    , micromasters_courses.program_id as micromasters_program_id
    , micromasters_courses.course_id as micromasters_course_id

from
    runs
left join
    micromasters_courses
    on runs.course_number = micromasters_courses.course_number
