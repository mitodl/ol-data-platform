-- MITx Course Runs from edx.org
---It also adds a field micromaster_program_id so that we could use it to get program requirements from MicroMaster


with runs_from_bigquery as (
    select *
    from {{ ref('stg__edxorg__bigquery__mitx_courserun') }}
    where courserun_platform = '{{ var("edxorg") }}'
)

, courseruns as (
    select * from {{ ref('stg__edxorg__api__courserun') }}
)

, courses as (
    select * from {{ ref('stg__edxorg__api__course') }}
)

, instructors as (
    select
        courseruns.courserun_readable_id
        , array_join(
            array_agg(
                concat(
                    json_extract_scalar(t.instructor, '$.first_name')
                    , ' '
                    , json_extract_scalar(t.instructor, '$.last_name')
                )
            )
            , ', '
        ) as instructor_names
    from courseruns
    cross join unnest(cast(json_parse(courseruns.courserun_instructors) as array (json))) as t (instructor) -- noqa
    group by courseruns.courserun_readable_id
)

, runs_from_api as (
    select
        courseruns.*
        , courses.course_organizations
        , instructors.instructor_names
        , array_join(courses.course_topics, ', ') as course_topics
    from courseruns
    inner join courses
        on courseruns.course_readable_id = courses.course_readable_id
    inner join instructors
        on courseruns.courserun_readable_id = instructors.courserun_readable_id
)

, runs as (
    select
        runs_from_bigquery.course_number
        , runs_from_bigquery.courserun_semester
        , runs_from_api.courserun_enrollment_end_on as courserun_enrollment_end_date
        , runs_from_api.courserun_short_description as courserun_description
        , runs_from_api.course_topics
        , runs_from_api.courserun_pace
        , runs_from_api.courserun_time_commitment
        , runs_from_api.courserun_estimated_hours
        , runs_from_api.courserun_duration
        , runs_from_api.courserun_enrollment_mode
        , runs_from_api.courserun_availability
        , runs_from_api.courserun_is_published
        , coalesce(
            replace(replace(runs_from_api.courserun_readable_id, 'course-v1:', ''), '+', '/')
            , runs_from_bigquery.courserun_readable_id
        ) as courserun_readable_id
        , coalesce(
            runs_from_api.courserun_is_self_paced, runs_from_bigquery.courserun_is_self_paced
        ) as courserun_is_self_paced
        , coalesce(runs_from_api.course_readable_id, runs_from_bigquery.course_readable_id) as course_readable_id
        , coalesce(runs_from_api.courserun_title, runs_from_bigquery.courserun_title) as courserun_title
        , coalesce(runs_from_api.courserun_marketing_url, runs_from_bigquery.courserun_url) as courserun_url
        , coalesce(
            runs_from_api.course_organizations, runs_from_bigquery.courserun_institution
        ) as courserun_institution
        , coalesce(runs_from_api.instructor_names, runs_from_bigquery.courserun_instructors) as courserun_instructors
        , coalesce(runs_from_api.courserun_enrollment_start_on, runs_from_bigquery.courserun_enrollment_start_date)
        as courserun_enrollment_start_date
        , coalesce(runs_from_api.courserun_start_on, runs_from_bigquery.courserun_start_date) as courserun_start_date
        , coalesce(runs_from_api.courserun_end_on, runs_from_bigquery.courserun_end_date) as courserun_end_date
    from runs_from_api
    full outer join runs_from_bigquery
        on
            replace(replace(runs_from_api.courserun_readable_id, 'course-v1:', ''), '+', '/')
            = runs_from_bigquery.courserun_readable_id
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
    , runs.courserun_enrollment_end_date
    , runs.courserun_start_date
    , runs.courserun_end_date
    , runs.courserun_is_self_paced
    , runs.courserun_description
    , runs.course_topics
    , runs.courserun_is_published
    , runs.courserun_pace
    , runs.courserun_time_commitment
    , runs.courserun_estimated_hours
    , runs.courserun_duration
    , runs.courserun_enrollment_mode
    , runs.courserun_availability
    , micromasters_courses.program_id as micromasters_program_id
    , micromasters_courses.course_id as micromasters_course_id

from
    runs
left join
    micromasters_courses
    on runs.course_number = micromasters_courses.course_number
