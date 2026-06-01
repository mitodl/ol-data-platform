{#
  integrations__learn__mit_edx_courses
  Exposes MIT edX (MITx on edX.org) courses for MIT Learn's Trino-pull ETL.
  One row per live course in the MITx catalog.
  Contract: docs/learn_marts_contract.md

  Source decision: edxorg dg_project (edx.org S3 exports via IRx), not
  mitxresidential — the latter covers MIT's internal OpenEdX deployment, while
  this model needs the public-facing edX.org catalog. See:
  implementation_guide_01_db_catalog.md, open question resolved 2026-05-29.
#}

with courses as (
    select * from {{ ref('stg__edxorg__api__course') }}
)

, raw_runs as (
    select
        course_readable_id
        , concat(
            courserun_readable_id,
            '|', coalesce(courserun_start_on, ''),
            '|', coalesce(courserun_end_on, ''),
            '|', coalesce(cast(courserun_is_published as varchar), 'false')
        ) as run_string
        , courserun_is_published
    from {{ ref('stg__edxorg__api__courserun') }}
)

, course_runs as (
    select
        course_readable_id
        , {{ array_join('array_agg(run_string order by run_string)', ';') }} as course_runs
        , bool_or(courserun_is_published) as any_run_published
    from raw_runs
    group by course_readable_id
)

, raw_instructors as (
    select
        runs.course_readable_id
        , concat(
            coalesce({{ json_extract_scalar('t.instructor', "'$.first_name'") }}, ''),
            ' ',
            coalesce({{ json_extract_scalar('t.instructor', "'$.last_name'") }}, '')
        ) as instructor_name
    from {{ ref('stg__edxorg__api__courserun') }} as runs
    cross join {{ unnest_json_array('runs.courserun_instructors', 't', 'instructor') }}  -- noqa
    where runs.courserun_instructors is not null and runs.courserun_instructors != '[]'
)

, course_instructors as (
    select
        course_readable_id
        , {{ array_join('array_agg(distinct trim(instructor_name))', ', ') }} as instructors
    from raw_instructors
    where trim(instructor_name) != ''
    group by course_readable_id
)

, course_run_attrs as (
    select
        course_readable_id
        , max(courserun_duration)        as course_length
        , max(courserun_time_commitment) as course_effort
    from {{ ref('stg__edxorg__api__courserun') }}
    where courserun_is_published = true
    group by course_readable_id
)

select
    courses.course_readable_id                              as readable_id
    , courses.course_title                                  as title
    , courses.course_updated_at                             as last_modified
    , 'mit_edx'                                             as etl_source
    , courses.course_description                            as description
    , courses.course_marketing_url                          as url
    , courses.course_image_url                              as image_url
    , coalesce(course_runs.any_run_published, false)        as published
    , 'edxorg'                                              as platform
    , cast(null as varchar)                                 as page_slug
    , {{ array_join('courses.course_topics', ', ') }}       as topics
    , course_instructors.instructors                        as instructors
    , courses.course_type                                   as certification_type
    , cast(null as varchar)                                 as price
    , course_run_attrs.course_length                        as length
    , course_run_attrs.course_effort                        as effort
    , course_runs.course_runs                               as runs
from courses
left join course_runs on courses.course_readable_id = course_runs.course_readable_id
left join course_instructors on courses.course_readable_id = course_instructors.course_readable_id
left join course_run_attrs on courses.course_readable_id = course_run_attrs.course_readable_id
where coalesce(course_runs.any_run_published, false) = true
